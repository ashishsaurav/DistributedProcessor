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
using DistributedProcessor.Shared.Services;
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

        private readonly CancellationTokenSource _shutdownCts = new();
        private volatile bool _isShuttingDown = false;
        private readonly TimeSpan _shutdownTimeout = TimeSpan.FromSeconds(30);
        private readonly SemaphoreSlim _shutdownLock = new(1, 1);

        // Circuit Breakers
        private readonly ICircuitBreaker _apiCircuitBreaker;
        private readonly ICircuitBreaker _kafkaCircuitBreaker;

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
            IConfiguration configuration,
            ILoggerFactory loggerFactory)
        {
            _logger = logger;
            _workerId = $"worker-{Guid.NewGuid().ToString()[..8]}";
            _currentProcess = Process.GetCurrentProcess();

            // Initialize circuit breakers
            _apiCircuitBreaker = new CircuitBreaker(
                "API",
                loggerFactory.CreateLogger<CircuitBreaker>(),
                failureThreshold: 5,
                openDurationSeconds: 30);

            _kafkaCircuitBreaker = new CircuitBreaker(
                "Kafka",
                loggerFactory.CreateLogger<CircuitBreaker>(),
                failureThreshold: 3,
                openDurationSeconds: 60);

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
                Timeout = TimeSpan.FromSeconds(10)
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
            // Link cancellation tokens
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
                stoppingToken, _shutdownCts.Token);
            var cancellationToken = linkedCts.Token;

            _taskConsumer.Subscribe(TaskTopic);
            _logger.LogInformation("Worker {WorkerId} started and subscribed to {Topic}",
                _workerId, TaskTopic);

            // Start heartbeat task
            var heartbeatTask = Task.Run(() => SendHeartbeatsAsync(cancellationToken));

            try
            {
                while (!cancellationToken.IsCancellationRequested && !_isShuttingDown)
                {
                    ConsumeResult<string, string>? consumeResult = null;
                    ProcessingTask? task = null;

                    try
                    {
                        // Use short timeout to check shutdown flag frequently
                        consumeResult = _taskConsumer.Consume(TimeSpan.FromMilliseconds(100));

                        if (consumeResult == null || consumeResult.IsPartitionEOF)
                            continue;

                        // Check shutdown before accepting new task
                        if (_isShuttingDown)
                        {
                            _logger.LogInformation(
                                "Shutdown in progress, not accepting new task from offset {Offset}",
                                consumeResult.Offset);
                            break; // Stop accepting new tasks
                        }

                        task = JsonSerializer.Deserialize<ProcessingTask>(consumeResult.Message.Value);

                        if (task == null)
                        {
                            _logger.LogWarning("Received null task, skipping");
                            _taskConsumer.Commit(consumeResult);
                            continue;
                        }

                        Interlocked.Increment(ref _activeTasks);
                        await SendImmediateHeartbeatAsync(cancellationToken);

                        // Process with retry logic
                        var success = await ProcessTaskWithRetryAsync(task, consumeResult, cancellationToken);

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
                        _logger.LogInformation("Worker operation cancelled, initiating graceful shutdown");
                        break;
                    }
                    finally
                    {
                        if (_activeTasks > 0)
                            Interlocked.Decrement(ref _activeTasks);
                        await SendImmediateHeartbeatAsync(cancellationToken);
                    }
                }
            }
            finally
            {
                await ShutdownGracefullyAsync();
            }
        }

        private async Task ShutdownGracefullyAsync()
        {
            await _shutdownLock.WaitAsync();
            try
            {
                if (_isShuttingDown) return; // Already shutting down
                _isShuttingDown = true;

                _logger.LogInformation(
                    "Worker {WorkerId} initiating graceful shutdown. Active tasks: {Count}",
                    _workerId, _activeTasks);

                // STEP 1: Unsubscribe from Kafka (stop receiving new messages)
                try
                {
                    _taskConsumer.Unsubscribe();
                    _logger.LogInformation("Unsubscribed from Kafka topic");
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Error unsubscribing from Kafka");
                }

                // STEP 2: Wait for active tasks to complete (with timeout)
                var sw = Stopwatch.StartNew();
                while (_activeTasks > 0 && sw.Elapsed < _shutdownTimeout)
                {
                    _logger.LogInformation(
                        "Waiting for {Count} active task(s) to complete... ({Elapsed:F0}s elapsed)",
                        _activeTasks, sw.Elapsed.TotalSeconds);

                    await Task.Delay(TimeSpan.FromSeconds(1));
                }

                if (_activeTasks > 0)
                {
                    _logger.LogWarning(
                        "Shutdown timeout reached with {Count} task(s) still pending. " +
                        "These tasks will be reprocessed by another worker.",
                        _activeTasks);
                }
                else
                {
                    _logger.LogInformation("All active tasks completed successfully");
                }

                // STEP 3: Flush Kafka producer (ensure all results are sent)
                _logger.LogInformation("Flushing Kafka producer...");
                try
                {
                    var remainingMessages = _resultProducer.Flush(TimeSpan.FromSeconds(10));
                    if (remainingMessages > 0)
                    {
                        _logger.LogWarning(
                            "{Count} message(s) could not be flushed from producer",
                            remainingMessages);
                    }
                    else
                    {
                        _logger.LogInformation("Kafka producer flushed successfully");
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error flushing Kafka producer");
                }

                // STEP 4: Commit final offsets
                try
                {
                    var committed = _taskConsumer.Commit();
                    _logger.LogInformation(
                        "Final offset commit: {Offsets}",
                        string.Join(", ", committed.Select(c =>
                            $"{c.Topic}[{c.Partition}]@{c.Offset}")));
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Error committing final offsets");
                }

                // STEP 5: Send offline heartbeat
                await SendOfflineHeartbeatAsync();

                // STEP 6: Close Kafka consumer
                try
                {
                    _taskConsumer.Close();
                    _logger.LogInformation("Kafka consumer closed");
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Error closing Kafka consumer");
                }

                // STEP 7: Dispose resources
                _resultProducer?.Dispose();
                _httpClient?.Dispose();

                _logger.LogInformation(
                    "Worker {WorkerId} graceful shutdown completed. " +
                    "Total processed: {TotalProcessed}",
                    _workerId, _totalProcessed);
            }
            finally
            {
                _shutdownLock.Release();
            }
        }

        private async Task SendOfflineHeartbeatAsync()
        {
            try
            {
                var status = new
                {
                    workerId = _workerId,
                    state = "Offline",
                    activeTasks = 0,
                    cpuUsage = 0.0,
                    memoryUsageMB = 0.0,
                    lastHeartbeat = DateTime.UtcNow,
                    totalProcessed = _totalProcessed
                };

                await _httpClient.PostAsJsonAsync(
                    "api/health/update",
                    status,
                    new CancellationTokenSource(TimeSpan.FromSeconds(5)).Token);

                _logger.LogInformation("Sent offline heartbeat to API");
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to send offline heartbeat");
            }
        }

        public override async Task StopAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("StopAsync called for worker {WorkerId}", _workerId);

            // Trigger graceful shutdown
            _isShuttingDown = true;
            _shutdownCts.Cancel();

            // Call base implementation
            await base.StopAsync(cancellationToken);
        }

        private async Task<bool> ProcessTaskWithRetryAsync(
        ProcessingTask task,
        ConsumeResult<string, string> consumeResult,
        CancellationToken stoppingToken)
        {
            // CHECK IDEMPOTENCY FIRST
            var canProcess = await CheckIdempotencyAsync(task.TaskId);

            if (!canProcess)
            {
                _logger.LogInformation(
                    "Task {TaskId} already processed or currently processing. Skipping.",
                    task.TaskId);

                // Already processed - commit to move forward
                _taskConsumer.Commit(consumeResult);
                return true;
            }

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

                    await ProcessTaskAsync(task, stoppingToken);

                    // MARK AS COMPLETED
                    await MarkIdempotencyCompletedAsync(task.TaskId);

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

                    var delay = CalculateBackoffDelay(retryCount);

                    _logger.LogWarning(
                        "Transient error processing task {TaskId}, retry {Retry}/{MaxRetry} after {Delay}ms. Error: {Error}",
                        task.TaskId, retryCount, MAX_RETRIES, delay.TotalMilliseconds, ex.Message);

                    await Task.Delay(delay, stoppingToken);
                }
                catch (Exception ex)
                {
                    lastException = ex;
                    break;
                }
            }

            // All retries failed
            taskStopwatch.Stop();

            _logger.LogError(lastException,
                "Task {TaskId} failed after {Retries} retries. Total time: {Duration}ms. Sending to DLQ.",
                task.TaskId, retryCount, taskStopwatch.ElapsedMilliseconds);

            // MARK AS FAILED
            await MarkIdempotencyFailedAsync(task.TaskId);

            await SendToDlqAsync(task, lastException!, retryCount);
            await UpdateTaskStatusAsync(
                task.TaskId,
                "Failed",
                _workerId,
                stoppingToken,
                0,
                taskStopwatch.ElapsedMilliseconds,
                $"Failed after {retryCount} retries: {lastException?.Message}");

            _taskConsumer.Commit(consumeResult);

            return false;
        }

        private async Task<bool> CheckIdempotencyAsync(string taskId)
        {
            try
            {
                return await _apiCircuitBreaker.ExecuteAsync(async () =>
                {
                    var response = await _httpClient.GetAsync(
                        $"api/idempotency/can-process/{taskId}?workerId={_workerId}",
                        new CancellationTokenSource(TimeSpan.FromSeconds(5)).Token);

                    if (response.IsSuccessStatusCode)
                    {
                        var result = await response.Content.ReadFromJsonAsync<IdempotencyCheckResult>();
                        return result?.CanProcess ?? true;
                    }

                    return true;
                });
            }
            catch (CircuitBreakerOpenException ex)
            {
                _logger.LogWarning(
                    "Circuit breaker open for API: {Message}. Allowing processing.",
                    ex.Message);
                return true; // Fail open when circuit is open
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error checking idempotency. Allowing processing.");
                return true;
            }
        }

        private async Task MarkIdempotencyCompletedAsync(string taskId)
        {
            try
            {
                await _apiCircuitBreaker.ExecuteAsync(async () =>
                    await _httpClient.PostAsync(
                        $"api/idempotency/complete/{taskId}",
                        null,
                        new CancellationTokenSource(TimeSpan.FromSeconds(5)).Token));
            }
            catch (CircuitBreakerOpenException)
            {
                _logger.LogWarning("Circuit breaker open - skipping idempotency completion for {TaskId}", taskId);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to mark idempotency completed for {TaskId}", taskId);
            }
        }

        private async Task MarkIdempotencyFailedAsync(string taskId)
        {
            try
            {
                await _apiCircuitBreaker.ExecuteAsync(async () =>
                    await _httpClient.PostAsync(
                        $"api/idempotency/failed/{taskId}",
                        null,
                        new CancellationTokenSource(TimeSpan.FromSeconds(5)).Token));
            }
            catch (CircuitBreakerOpenException)
            {
                _logger.LogWarning("Circuit breaker open - skipping idempotency failed mark for {TaskId}", taskId);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to mark idempotency failed for {TaskId}", taskId);
            }
        }

        private async Task ProcessTaskAsync(ProcessingTask task, CancellationToken stoppingToken)
        {
            // Update status with circuit breaker
            await _apiCircuitBreaker.ExecuteAsync(async () =>
                await UpdateTaskStatusAsync(task.TaskId, "Processing", _workerId, stoppingToken));

            // Process the task (your existing logic)
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

            // Update to Processed with circuit breaker
            await _apiCircuitBreaker.ExecuteAsync(async () =>
                await UpdateTaskStatusAsync(
                    task.TaskId,
                    "Processed",
                    _workerId,
                    stoppingToken,
                    calculatedRows.Count));

            // Create result
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

            // Publish with circuit breaker
            await _kafkaCircuitBreaker.ExecuteAsync(async () =>
                await _resultProducer.ProduceAsync(ResultTopic, message, stoppingToken));

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
                CircuitBreakerOpenException => true,
                _ => false
            };
        }

        private async Task SendToDlqAsync(ProcessingTask task, Exception exception, int retryCount)
        {
            try
            {
                await _apiCircuitBreaker.ExecuteAsync(async () =>
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
                });
            }
            catch (CircuitBreakerOpenException)
            {
                _logger.LogError("Circuit breaker open - DLQ send failed for {TaskId}", task.TaskId);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Exception sending to DLQ for task {TaskId}", task.TaskId);
            }
        }

        private async Task UpdateTaskStatusAsync(
            string taskId,
            string status,
            string? workerId = null,
            CancellationToken stoppingToken = default,
            int rowCount = 0,
            long durationMs = 0,
            string? errorMessage = null)
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

                var response = await _httpClient.PutAsJsonAsync(
                    "api/task/update-status",
                    updateData,
                    stoppingToken);

                if (response.IsSuccessStatusCode)
                {
                    _logger.LogDebug("Task {TaskId} status updated to {Status}", taskId, status);
                }
                else
                {
                    _logger.LogWarning(
                        "Failed to update task {TaskId} status: {StatusCode}",
                        taskId, response.StatusCode);
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to update task {TaskId} status to {Status}", taskId, status);
                throw; // Re-throw for circuit breaker to handle
            }
        }

        private async Task SendImmediateHeartbeatAsync(CancellationToken stoppingToken)
        {
            if (_isShuttingDown) return; // Don't send heartbeats during shutdown

            try
            {
                string currentState = _activeTasks > 0 ? "Busy" : "Idle";

                var status = new
                {
                    workerId = _workerId,
                    state = currentState,
                    activeTasks = _activeTasks,
                    cpuUsage = 0.0, // Can be calculated if needed
                    memoryUsageMB = 0.0,
                    lastHeartbeat = DateTime.UtcNow,
                    totalProcessed = _totalProcessed
                };

                await _httpClient.PostAsJsonAsync("api/health/update", status, stoppingToken);
                _logger.LogDebug("Immediate heartbeat: State={State}, Tasks={Tasks}",
                    currentState, _activeTasks);
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Failed to send immediate heartbeat");
            }
        }

        private async Task SendHeartbeatsAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested && !_isShuttingDown)
            {
                try
                {
                    await SendImmediateHeartbeatAsync(stoppingToken);
                    await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Failed to send heartbeat");
                    await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
                }
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
            _shutdownCts?.Dispose();
            _shutdownLock?.Dispose();
            _taskConsumer?.Dispose();
            _resultProducer?.Dispose();
            _httpClient?.Dispose();
            base.Dispose();
        }
    }
    internal class IdempotencyCheckResult
    {
        public bool CanProcess { get; set; }
    }
}
