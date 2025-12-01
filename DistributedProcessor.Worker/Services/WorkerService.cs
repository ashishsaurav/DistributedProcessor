using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Json;
using System.Runtime.InteropServices;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using DistributedProcessor.Shared.Models;
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
                EnableAutoCommit = false
            };

            var producerConfig = new ProducerConfig
            {
                BootstrapServers = configuration["Kafka:BootstrapServers"] ?? "localhost:5081"
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
            _logger.LogInformation($"Worker {_workerId} subscribed to {TaskTopic}");

            // Start background heartbeat task with 1-second interval
            var heartbeatTask = Task.Run(() => SendHeartbeatsAsync(stoppingToken), stoppingToken);

            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    ProcessingTask task = null;
                    Stopwatch taskStopwatch = null;

                    try
                    {
                        var consumeResult = _taskConsumer.Consume(stoppingToken);
                        if (consumeResult == null || consumeResult.IsPartitionEOF)
                        {
                            continue;
                        }

                        task = JsonSerializer.Deserialize<ProcessingTask>(consumeResult.Message.Value);
                        if (task == null)
                        {
                            _logger.LogWarning("Received null task, skipping");
                            _taskConsumer.Commit(consumeResult);
                            continue;
                        }

                        // IMMEDIATE STATUS UPDATE: Task starting
                        Interlocked.Increment(ref _activeTasks);
                        await SendImmediateHeartbeatAsync(stoppingToken); // NEW: Instant update

                        taskStopwatch = Stopwatch.StartNew();

                        try
                        {
                            _logger.LogInformation($"Worker {_workerId} processing task {task.TaskId} from partition {consumeResult.Partition.Value} ({task.Fund}/{task.Symbol}, {task.Rows.Count} rows)");

                            // Update to Processing status
                            await UpdateTaskStatusAsync(task.TaskId, "Processing", _workerId, stoppingToken);

                            // Process the task
                            var calculatedRows = new List<CalculatedRow>();
                            decimal previousValue = 0;
                            decimal cumulativeReturns = 0;

                            foreach (var row in task.Rows.OrderBy(r => r.Date))
                            {
                                decimal returns = 0;
                                if (previousValue > 0)
                                {
                                    returns = (row.Value - previousValue) / previousValue;
                                    cumulativeReturns = (1 + cumulativeReturns) * (1 + returns) - 1;
                                }

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

                            _logger.LogInformation($"Worker {_workerId} simulating 5-second processing delay for task {task.TaskId}");
                            await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);

                            taskStopwatch.Stop();

                            // Update to Processed status (worker finished, before sending to Kafka)
                            await UpdateTaskStatusAsync(
                                task.TaskId,
                                "Processed",
                                _workerId,
                                stoppingToken,
                                calculatedRows.Count,
                                taskStopwatch.ElapsedMilliseconds);

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
                                Success = true,
                                ProcessingDurationMs = taskStopwatch.ElapsedMilliseconds
                            };

                            var message = new Message<string, string>
                            {
                                Key = task.JobId,
                                Value = JsonSerializer.Serialize(result)
                            };

                            await _resultProducer.ProduceAsync(ResultTopic, message, stoppingToken);
                            _taskConsumer.Commit(consumeResult);

                            Interlocked.Increment(ref _totalProcessed);
                            _logger.LogInformation($"Worker {_workerId} completed task {task.TaskId} in {taskStopwatch.ElapsedMilliseconds}ms ({calculatedRows.Count} rows)");
                        }
                        catch (Exception taskEx)
                        {
                            taskStopwatch?.Stop();
                            _logger.LogError(taskEx, $"Error processing task {task?.TaskId}");

                            if (task != null)
                            {
                                await UpdateTaskStatusAsync(
                                    task.TaskId,
                                    "Failed",
                                    _workerId,
                                    stoppingToken,
                                    0,
                                    taskStopwatch?.ElapsedMilliseconds ?? 0,
                                    taskEx.Message);

                                var failureResult = new ProcessingResult
                                {
                                    TaskId = task.TaskId,
                                    JobId = task.JobId,
                                    WorkerId = _workerId,
                                    Fund = task.Fund,
                                    Symbol = task.Symbol,
                                    Success = false,
                                    ErrorMessage = taskEx.Message,
                                    ProcessedAt = DateTime.UtcNow,
                                    ProcessingDurationMs = taskStopwatch?.ElapsedMilliseconds ?? 0
                                };

                                var failureMessage = new Message<string, string>
                                {
                                    Key = task.JobId,
                                    Value = JsonSerializer.Serialize(failureResult)
                                };

                                await _resultProducer.ProduceAsync(ResultTopic, failureMessage, stoppingToken);
                                _taskConsumer.Commit(consumeResult);
                            }
                        }
                        finally
                        {
                            // IMMEDIATE STATUS UPDATE: Task ended
                            Interlocked.Decrement(ref _activeTasks);
                            await SendImmediateHeartbeatAsync(stoppingToken); // NEW: Instant update
                        }
                    }
                    catch (ConsumeException cex)
                    {
                        _logger.LogError(cex, $"Kafka consume error: {cex.Error.Reason}");
                    }
                    catch (OperationCanceledException)
                    {
                        _logger.LogInformation("Worker shutting down");
                        break;
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Unexpected error in consumer loop");
                    }
                }
            }
            finally
            {
                _taskConsumer.Close();
                _resultProducer.Dispose();
                _logger.LogInformation($"Worker {_workerId} stopped");
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
                        _logger.LogDebug($"Heartbeat: State={currentState}, Tasks={_activeTasks}, CPU={status.CpuUsage:F1}%, Mem={status.MemoryUsageMB:F0}MB");
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Failed to send heartbeat");
                }

                // Reduced from 5 seconds to 1 second
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
