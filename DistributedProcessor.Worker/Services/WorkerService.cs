using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Json;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;
using Confluent.Kafka;
using DistributedProcessor.Shared.Models;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace DistributedProcessor.Worker.Services
{
    public class WorkerService : BackgroundService
    {
        private readonly ILogger<WorkerService> _logger;
        private readonly IConsumer<string, string> _taskConsumer;
        private readonly IProducer<string, string> _resultProducer;
        private readonly HttpClient _httpClient;
        private readonly string _workerId;
        private int _activeTasks = 0;
        private int _totalProcessed = 0;
        private const string TaskTopic = "processing-tasks";
        private const string ResultTopic = "processing-results";
        private const int MAX_RETRIES = 3;
        private const int BASE_DELAY_MS = 1000; // Start with 1 second
        private const int MAX_DELAY_MS = 30000; // Cap at 30 seconds

        // CPU and Memory monitoring
        private PerformanceCounter _cpuCounter;
        private Process _currentProcess;

        public WorkerService(
            ILogger<WorkerService> logger,
            IConfiguration configuration)
        {
            _logger = logger;
            _workerId = $"worker-{Guid.NewGuid().ToString()[..8]}";
            _currentProcess = Process.GetCurrentProcess();

            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = configuration["Kafka:BootstrapServers"] ?? "localhost:5081",
                GroupId = "worker-group",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false, //  Manual commit for reliability

                //  NEW: Faster crash detection
                SessionTimeoutMs = 10000,        // 10 seconds - faster rebalance
                MaxPollIntervalMs = 300000,      // 5 minutes max between polls
                HeartbeatIntervalMs = 3000       // Send heartbeat every 3 seconds
            };

            var producerConfig = new ProducerConfig
            {
                BootstrapServers = configuration["Kafka:BootstrapServers"] ?? "localhost:5081",

                //  NEW: Ensure messages are not lost
                Acks = Acks.All,                 // Wait for all replicas
                MessageSendMaxRetries = 3,       // Retry failed sends
                EnableIdempotence = true         // Prevent duplicate messages
            };

            _taskConsumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
            _resultProducer = new ProducerBuilder<string, string>(producerConfig).Build();

            _httpClient = new HttpClient
            {
                BaseAddress = new Uri(configuration["ApiUrl"] ?? "http://localhost:5000"),
                Timeout = TimeSpan.FromSeconds(5)
            };

            InitializePerformanceCounters();
        }

        private void InitializePerformanceCounters()
        {
            try
            {
                if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                {
                    _cpuCounter = new PerformanceCounter(
                        "Processor",
                        "% Processor Time",
                        "_Total",
                        true);
                    _cpuCounter.NextValue();
                    _logger.LogInformation("Performance counters initialized (Windows)");
                }
                else
                {
                    _logger.LogInformation("Running on Linux/Mac - using alternative monitoring");
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to initialize performance counters, using fallback");
            }
        }
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _taskConsumer.Subscribe(TaskTopic);
            _logger.LogInformation("Worker {WorkerId} subscribed to {Topic}", _workerId, TaskTopic);

            var heartbeatTask = Task.Run(() => SendHeartbeatsAsync(stoppingToken), stoppingToken);

            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    ConsumeResult<string, string>? consumeResult = null;
                    ProcessingTask? task = null;

                    try
                    {
                        consumeResult = _taskConsumer.Consume(TimeSpan.FromMilliseconds(100));

                        if (consumeResult == null || consumeResult.IsPartitionEOF)
                            continue;

                        task = JsonSerializer.Deserialize<ProcessingTask>(consumeResult.Message.Value);

                        if (task == null)
                        {
                            _logger.LogWarning("Received null task, skipping");
                            _taskConsumer.Commit(consumeResult);
                            continue;
                        }

                        Interlocked.Increment(ref _activeTasks);
                        await SendImmediateHeartbeatAsync(stoppingToken);

                        // Process with retry logic
                        var success = await ProcessTaskWithRetryAsync(task, consumeResult, stoppingToken);

                        if (success)
                        {
                            Interlocked.Increment(ref _totalProcessed);
                        }
                    }
                    catch (ConsumeException cex)
                    {
                        _logger.LogError(cex, "Kafka consume error: {Reason}", cex.Error.Reason);
                    }
                    catch (OperationCanceledException)
                    {
                        _logger.LogInformation("Worker shutting down gracefully");
                        break;
                    }
                    finally
                    {
                        if (_activeTasks > 0)
                            Interlocked.Decrement(ref _activeTasks);
                        await SendImmediateHeartbeatAsync(stoppingToken);
                    }
                }
            }
            finally
            {
                _logger.LogInformation("Worker {WorkerId} shutting down, flushing producer...", _workerId);
                _resultProducer.Flush(TimeSpan.FromSeconds(10));
                _taskConsumer.Close();
                _resultProducer.Dispose();
                _logger.LogInformation("Worker {WorkerId} stopped", _workerId);
            }
        }

        private async Task<bool> ProcessTaskWithRetryAsync(
            ProcessingTask task,
            ConsumeResult<string, string> consumeResult,
            CancellationToken stoppingToken)
        {
            Exception? lastException = null;
            int retryCount = 0;
            var taskStopwatch = Stopwatch.StartNew();

            while (retryCount <= MAX_RETRIES)
            {
                try
                {
                    _logger.LogInformation(
                        "Worker {WorkerId} processing task {TaskId} (Attempt {Attempt}/{MaxAttempts})",
                        _workerId, task.TaskId, retryCount + 1, MAX_RETRIES + 1);

                    // Attempt to process the task
                    await ProcessTaskAsync(task, stoppingToken);

                    // Success - commit and return
                    _taskConsumer.Commit(consumeResult);
                    taskStopwatch.Stop();

                    _logger.LogInformation(
                        "Worker {WorkerId} completed task {TaskId} in {Duration}ms after {Attempts} attempt(s)",
                        _workerId, task.TaskId, taskStopwatch.ElapsedMilliseconds, retryCount + 1);

                    return true;
                }
                catch (Exception ex) when (IsTransientError(ex) && retryCount < MAX_RETRIES)
                {
                    lastException = ex;
                    retryCount++;

                    // Calculate exponential backoff with jitter
                    var delay = CalculateBackoffDelay(retryCount);

                    _logger.LogWarning(
                        "Transient error processing task {TaskId}, retry {Retry}/{MaxRetry} after {Delay}ms. Error: {Error}",
                        task.TaskId, retryCount, MAX_RETRIES, delay.TotalMilliseconds, ex.Message);

                    await Task.Delay(delay, stoppingToken);
                }
                catch (Exception ex)
                {
                    // Non-transient error or max retries exceeded
                    lastException = ex;
                    break;
                }
            }

            // All retries failed - send to DLQ
            taskStopwatch.Stop();

            _logger.LogError(lastException,
                "Task {TaskId} failed after {Retries} retries. Total time: {Duration}ms. Sending to DLQ.",
                task.TaskId, retryCount, taskStopwatch.ElapsedMilliseconds);

            await SendToDlqAsync(task, lastException!, retryCount);
            await UpdateTaskStatusAsync(
                task.TaskId,
                "Failed",
                _workerId,
                stoppingToken,
                0,
                taskStopwatch.ElapsedMilliseconds,
                $"Failed after {retryCount} retries: {lastException?.Message}");

            // Commit to prevent infinite reprocessing
            _taskConsumer.Commit(consumeResult);

            return false;
        }

        private async Task ProcessTaskAsync(ProcessingTask task, CancellationToken stoppingToken)
        {
            // Update to Processing status
            await UpdateTaskStatusAsync(task.TaskId, "Processing", _workerId, stoppingToken);

            // Process the task
            var calculatedRows = new List<CalculatedRow>();
            decimal previousValue = 0;
            decimal cumulativeReturns = 0;

            foreach (var row in task.Rows.OrderBy(r => r.Date))
            {
                decimal returns = 0;
                if (previousValue != 0)
                {
                    returns = (row.Value - previousValue) / previousValue;
                }
                cumulativeReturns = (1 + cumulativeReturns) * (1 + returns) - 1;

                var calculated = new CalculatedRow
                {
                    Date = row.Date,
                    Price = row.Value,
                    Returns = returns,
                    CumulativeReturns = cumulativeReturns
                };
                calculatedRows.Add(calculated);

                previousValue = row.Value;
            }

            _logger.LogInformation(
                "Worker {WorkerId} simulating 5-second processing delay for task {TaskId}",
                _workerId, task.TaskId);
            await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);

            // Update to Processed status
            await UpdateTaskStatusAsync(
                task.TaskId,
                "Processed",
                _workerId,
                stoppingToken,
                calculatedRows.Count);

            // Create and publish result
            var result = new ProcessingResult
            {
                TaskId = task.TaskId,
                JobId = task.JobId,
                WorkerId = _workerId,
                Fund = task.Fund,
                Symbol = task.Symbol,
                CalculatedRows = calculatedRows,
                ProcessedAt = DateTime.UtcNow,
                Success = true
            };

            var message = new Message<string, string>
            {
                Key = task.JobId,
                Value = JsonSerializer.Serialize(result)
            };

            await _resultProducer.ProduceAsync(ResultTopic, message, stoppingToken);

            _logger.LogInformation(
                "Result published to {Topic} for task {TaskId}",
                ResultTopic, task.TaskId);
        }

        private TimeSpan CalculateBackoffDelay(int retryCount)
        {
            // Exponential backoff: delay = baseDelay * 2^(retryCount-1)
            var exponentialDelay = BASE_DELAY_MS * Math.Pow(2, retryCount - 1);

            // Cap the delay at maximum
            var cappedDelay = Math.Min(exponentialDelay, MAX_DELAY_MS);

            // Add jitter (±20% randomness) to prevent thundering herd
            var jitterFactor = 0.8 + (Random.Shared.NextDouble() * 0.4); // 0.8 to 1.2
            var finalDelay = cappedDelay * jitterFactor;

            return TimeSpan.FromMilliseconds(finalDelay);
        }

        private bool IsTransientError(Exception ex)
        {
            return ex switch
            {
                HttpRequestException => true,
                TimeoutException => true,
                TaskCanceledException tce when !tce.CancellationToken.IsCancellationRequested => true,
                IOException => true,
                SocketException => true,
                SqlException sqlEx => IsTransientSqlError(sqlEx),
                _ => false
            };
        }

        private bool IsTransientSqlError(SqlException sqlEx)
        {
            // Common transient SQL error codes
            int[] transientErrorNumbers = {
            -2,     // Timeout
            -1,     // Connection broken
            2,      // Network error
            53,     // Connection broken
            64,     // Connection lost
            233,    // Connection init error
            10053,  // Transport-level error
            10054,  // Connection forcibly closed
            10060,  // Network timeout
            40197,  // Service error
            40501,  // Service busy
            40613,  // Database unavailable
            49918,  // Cannot process request
            49919,  // Cannot process create/update
            49920   // Cannot process request
        };

            return transientErrorNumbers.Contains(sqlEx.Number);
        }

        private async Task SendToDlqAsync(ProcessingTask task, Exception exception, int retryCount)
        {
            try
            {
                var dlqRequest = new
                {
                    message = task,
                    topic = TaskTopic,
                    errorMessage = exception.Message,
                    exceptionType = exception.GetType().Name,
                    retryCount = retryCount,
                    workerId = _workerId,
                    jobId = task.JobId,
                    taskId = task.TaskId
                };

                var response = await _httpClient.PostAsJsonAsync(
                    "api/deadletter/send",
                    dlqRequest,
                    new CancellationTokenSource(TimeSpan.FromSeconds(5)).Token);

                if (!response.IsSuccessStatusCode)
                {
                    _logger.LogError(
                        "Failed to send to DLQ: {StatusCode} - {TaskId}",
                        response.StatusCode, task.TaskId);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Exception sending to DLQ for task {TaskId}", task.TaskId);
            }
        }

        private async Task UpdateTaskStatusAsync(
            string taskId,
            string status,
            string workerId = null,
            CancellationToken stoppingToken = default,
            int rowCount = 0,
            long durationMs = 0,
            string errorMessage = null)
        {
            try
            {
                var updateData = new
                {
                    taskId = taskId,
                    status = status,
                    workerId = workerId ?? _workerId,
                    rowCount = rowCount,
                    durationMs = durationMs,
                    errorMessage = errorMessage
                };

                var response = await _httpClient.PutAsJsonAsync($"/api/task/update-status", updateData, stoppingToken);

                if (response.IsSuccessStatusCode)
                {
                    _logger.LogDebug($"Task {taskId} status updated to {status}");
                }
                else
                {
                    _logger.LogWarning($"Failed to update task {taskId} status: {response.StatusCode}");
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, $"Failed to update task {taskId} status to {status}");
            }
        }

        // NEW: Send immediate heartbeat on task state change
        private async Task SendImmediateHeartbeatAsync(CancellationToken stoppingToken)
        {
            try
            {
                string currentState = _activeTasks > 0 ? "Busy" : "Idle";

                var status = new WorkerStatus
                {
                    WorkerId = _workerId,
                    State = currentState,
                    ActiveTasks = _activeTasks,
                    CpuUsage = GetCpuUsage(),
                    MemoryUsageMB = GetMemoryUsage(),
                    LastHeartbeat = DateTime.UtcNow,
                    TotalProcessed = _totalProcessed
                };

                await _httpClient.PostAsJsonAsync("/api/health/update", status, stoppingToken);
                _logger.LogDebug($"Immediate heartbeat: State={currentState}, Tasks={_activeTasks}");
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Failed to send immediate heartbeat");
            }
        }

        private async Task SendHeartbeatsAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    string currentState = _activeTasks > 0 ? "Busy" : "Idle";

                    var status = new WorkerStatus
                    {
                        WorkerId = _workerId,
                        State = currentState,
                        ActiveTasks = _activeTasks,
                        CpuUsage = GetCpuUsage(),
                        MemoryUsageMB = GetMemoryUsage(),
                        LastHeartbeat = DateTime.UtcNow,
                        TotalProcessed = _totalProcessed
                    };

                    var response = await _httpClient.PostAsJsonAsync("/api/health/update", status, stoppingToken);

                    if (response.IsSuccessStatusCode)
                    {
                        _logger.LogDebug(
                            $"Heartbeat: State={currentState}, Tasks={_activeTasks}, " +
                            $"CPU={status.CpuUsage:F1}%, Mem={status.MemoryUsageMB:F0}MB");
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Failed to send heartbeat");
                }

                await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
            }
        }

        private double GetCpuUsage()
        {
            try
            {
                if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows) && _cpuCounter != null)
                {
                    return _cpuCounter.NextValue();
                }
                else
                {
                    return GetCpuUsageCrossPlatform();
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error getting CPU usage");
                return 0;
            }
        }

        private double GetCpuUsageCrossPlatform()
        {
            try
            {
                var startTime = DateTime.UtcNow;
                var startCpuUsage = _currentProcess.TotalProcessorTime;

                Thread.Sleep(100);

                var endTime = DateTime.UtcNow;
                var endCpuUsage = _currentProcess.TotalProcessorTime;

                var cpuUsedMs = (endCpuUsage - startCpuUsage).TotalMilliseconds;
                var totalMsPassed = (endTime - startTime).TotalMilliseconds;
                var cpuUsageTotal = cpuUsedMs / (Environment.ProcessorCount * totalMsPassed);

                return cpuUsageTotal * 100;
            }
            catch
            {
                return 0;
            }
        }

        private double GetMemoryUsage()
        {
            try
            {
                _currentProcess.Refresh();
                return _currentProcess.WorkingSet64 / 1024.0 / 1024.0;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error getting memory usage");
                return 0;
            }
        }

        public override void Dispose()
        {
            _cpuCounter?.Dispose();
            _taskConsumer?.Dispose();
            _resultProducer?.Dispose();
            _httpClient?.Dispose();
            base.Dispose();
        }
    }
}
