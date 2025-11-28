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

        private const string TaskTopic = "processing-tasks";
        private const string ResultTopic = "processing-results";

        // CPU and Memory monitoring
        private PerformanceCounter _cpuCounter;
        private PerformanceCounter _memoryCounter;
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
                BaseAddress = new Uri(configuration["ApiUrl"] ?? "http://localhost:5000")
            };

            // Initialize performance counters (Windows only)
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

                    // Prime the counter (first call returns 0)
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

            var heartbeatTask = Task.Run(() => SendHeartbeatsAsync(stoppingToken), stoppingToken);

            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    ProcessingTask task = null;

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

                        Interlocked.Increment(ref _activeTasks);

                        try
                        {
                            _logger.LogInformation($"Worker {_workerId} processing task {task.TaskId} from partition {consumeResult.Partition.Value} ({task.Fund}/{task.Symbol}, {task.Rows.Count} rows)");

                            await UpdateTaskStatusAsync(task.TaskId, "Processing", stoppingToken);

                            var stopwatch = Stopwatch.StartNew();
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

                            stopwatch.Stop();

                            await UpdateTaskStatusAsync(task.TaskId, "Completed", stoppingToken);

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
                            _taskConsumer.Commit(consumeResult);

                            _logger.LogInformation($"Worker {_workerId} completed task {task.TaskId} in {stopwatch.ElapsedMilliseconds}ms ({calculatedRows.Count} rows)");
                        }
                        catch (Exception taskEx)
                        {
                            _logger.LogError(taskEx, $"Error processing task {task?.TaskId}");

                            if (task != null)
                            {
                                await UpdateTaskStatusAsync(task.TaskId, "Failed", stoppingToken);

                                var failureResult = new ProcessingResult
                                {
                                    TaskId = task.TaskId,
                                    JobId = task.JobId,
                                    WorkerId = _workerId,
                                    Fund = task.Fund,
                                    Symbol = task.Symbol,
                                    Success = false,
                                    ErrorMessage = taskEx.Message,
                                    ProcessedAt = DateTime.UtcNow
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
                            Interlocked.Decrement(ref _activeTasks);
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

        private async Task UpdateTaskStatusAsync(string taskId, string status, CancellationToken stoppingToken)
        {
            try
            {
                await _httpClient.PostAsJsonAsync("/api/task/update-status", new
                {
                    taskId = taskId,
                    status = status,
                    workerId = _workerId
                }, stoppingToken);

                _logger.LogDebug($"Updated task {taskId} status to {status}");
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, $"Failed to update task status for {taskId}");
            }
        }

        private async Task SendHeartbeatsAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var cpuUsage = GetCpuUsage();
                    var memoryUsage = GetMemoryUsage();

                    var status = new WorkerStatus
                    {
                        WorkerId = _workerId,
                        State = _activeTasks > 0 ? "Busy" : "Idle",
                        ActiveTasks = _activeTasks,
                        CpuUsage = cpuUsage,
                        MemoryUsageMB = memoryUsage,
                        LastHeartbeat = DateTime.UtcNow
                    };

                    var response = await _httpClient.PostAsJsonAsync("/api/health/update", status, stoppingToken);

                    if (response.IsSuccessStatusCode)
                    {
                        _logger.LogDebug($"Heartbeat sent: CPU={cpuUsage:F2}%, Memory={memoryUsage:F2}MB, Tasks={_activeTasks}");
                    }
                    else
                    {
                        _logger.LogWarning($"Heartbeat failed: {response.StatusCode}");
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error sending heartbeat");
                }

                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
            }
        }

        private double GetCpuUsage()
        {
            try
            {
                if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows) && _cpuCounter != null)
                {
                    // Windows: Use PerformanceCounter
                    return _cpuCounter.NextValue();
                }
                else
                {
                    // Linux/Mac: Use process CPU time calculation
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

                Thread.Sleep(100); // Short delay

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
                return _currentProcess.WorkingSet64 / 1024.0 / 1024.0; // Convert to MB
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
            _memoryCounter?.Dispose();
            _taskConsumer?.Dispose();
            _resultProducer?.Dispose();
            _httpClient?.Dispose();
            base.Dispose();
        }
    }
}
