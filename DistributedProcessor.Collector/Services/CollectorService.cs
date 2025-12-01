using System;
using System.Diagnostics;
using System.Net.Http;
using System.Net.Http.Json;
using System.Runtime.InteropServices;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using DistributedProcessor.Shared.Models;
using DistributedProcessor.Data.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace DistributedProcessor.Collector.Services
{
    public class CollectorService : BackgroundService
    {
        private readonly ILogger<CollectorService> _logger;
        private readonly IServiceScopeFactory _scopeFactory;
        private readonly IConsumer<string, string> _consumer;
        private readonly HttpClient _httpClient;
        private readonly string _collectorId;
        private int _activeCollections = 0;
        private int _totalCollected = 0;
        private long _totalCollectionTimeMs = 0;
        private Process _currentProcess;
        private PerformanceCounter _cpuCounter;

        public CollectorService(
            ILogger<CollectorService> logger,
            IServiceScopeFactory scopeFactory,
            IConfiguration configuration)
        {
            _logger = logger;
            _scopeFactory = scopeFactory;
            _collectorId = $"collector-{Guid.NewGuid().ToString()[..8]}";
            _currentProcess = Process.GetCurrentProcess();

            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = configuration["Kafka:BootstrapServers"] ?? "localhost:5081",
                GroupId = "collector-group",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false, //  Manual commit for reliability

                //  NEW: Faster crash detection
                SessionTimeoutMs = 10000,
                MaxPollIntervalMs = 300000,
                HeartbeatIntervalMs = 3000
            };

            _consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();

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
                    _cpuCounter = new PerformanceCounter("Processor", "% Processor Time", "_Total", true);
                    _cpuCounter.NextValue();
                    _logger.LogInformation("Performance counters initialized (Windows)");
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to initialize performance counters");
            }
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _consumer.Subscribe("processing-results");
            _logger.LogInformation($"Collector {_collectorId} subscribed to processing-results topic");

            // Start background heartbeat task
            var heartbeatTask = Task.Run(() => SendHeartbeatsAsync(stoppingToken), stoppingToken);

            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    ConsumeResult<string, string> consumeResult = null;
                    ProcessingResult result = null;
                    Stopwatch collectionStopwatch = null;

                    try
                    {
                        consumeResult = _consumer.Consume(stoppingToken);

                        if (consumeResult?.Message?.Value == null)
                        {
                            _logger.LogWarning("Received null message");
                            continue;
                        }

                        _logger.LogInformation($"Consumed message at offset {consumeResult.Offset}");

                        result = JsonSerializer.Deserialize<ProcessingResult>(consumeResult.Message.Value);

                        if (result == null)
                        {
                            _logger.LogWarning("Deserialized ProcessingResult is null");
                            _consumer.Commit(consumeResult); //  Commit even if null
                            continue;
                        }

                        // Increment active collections
                        Interlocked.Increment(ref _activeCollections);
                        await SendImmediateHeartbeatAsync(stoppingToken);

                        // Simulate collection delay for testing
                        _logger.LogInformation(
                            $"Collector {_collectorId} simulating 5-second delay before saving TaskId: {result.TaskId}");
                        await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);

                        collectionStopwatch = Stopwatch.StartNew();

                        using var scope = _scopeFactory.CreateScope();
                        var dbService = scope.ServiceProvider.GetRequiredService<IDbService>();

                        _logger.LogInformation($"Saving results for TaskId: {result.TaskId}");

                        //  CRITICAL: Save to database
                        await dbService.SaveCalculatedResultsAsync(result);

                        collectionStopwatch.Stop();

                        // Update task status to "Collected"
                        await UpdateTaskStatusAsync(
                            result.TaskId,
                            "Collected",
                            result.WorkerId,
                            collectionStopwatch.ElapsedMilliseconds,
                            stoppingToken);

                        _logger.LogInformation(
                            $"Results saved and collected for TaskId: {result.TaskId} in {collectionStopwatch.ElapsedMilliseconds}ms");

                        //  CRITICAL: Commit ONLY after successful database save
                        _consumer.Commit(consumeResult);

                        _logger.LogInformation($" Collection committed to Kafka (offset {consumeResult.Offset})");

                        // Update metrics
                        Interlocked.Increment(ref _totalCollected);
                        Interlocked.Add(ref _totalCollectionTimeMs, collectionStopwatch.ElapsedMilliseconds);
                    }
                    catch (ConsumeException cex)
                    {
                        _logger.LogError(cex, $"Kafka consume error: {cex.Error.Reason}");
                        //  Don't commit on consume error
                    }
                    catch (OperationCanceledException)
                    {
                        _logger.LogInformation("CollectorService stopping gracefully");
                        break;
                    }
                    catch (Exception ex)
                    {
                        collectionStopwatch?.Stop();
                        _logger.LogError(ex, $"Error collecting results for task {result?.TaskId}");

                        if (result != null && consumeResult != null)
                        {
                            try
                            {
                                // Mark task as failed collection
                                await UpdateTaskStatusAsync(
                                    result.TaskId,
                                    "Failed",
                                    result.WorkerId,
                                    collectionStopwatch?.ElapsedMilliseconds ?? 0,
                                    stoppingToken);

                                //  Commit to prevent infinite retry
                                _consumer.Commit(consumeResult);

                                _logger.LogWarning($"Failed collection for {result.TaskId} committed to prevent reprocessing");
                            }
                            catch (Exception commitEx)
                            {
                                _logger.LogError(commitEx, $"Failed to handle collection error for {result?.TaskId}");
                                //  Don't commit - collection will be retried
                            }
                        }
                    }
                    finally
                    {
                        // Decrement active collections
                        Interlocked.Decrement(ref _activeCollections);
                        await SendImmediateHeartbeatAsync(stoppingToken);
                    }
                }
            }
            finally
            {
                _logger.LogInformation("Closing Kafka consumer");
                _consumer.Close();
            }
        }

        private async Task UpdateTaskStatusAsync(
            string taskId,
            string status,
            string workerId,
            long durationMs,
            CancellationToken stoppingToken)
        {
            try
            {
                var updateData = new
                {
                    taskId = taskId,
                    status = status,
                    workerId = workerId,
                    rowCount = 0,
                    durationMs = durationMs,
                    errorMessage = (string)null
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

        // NEW: Send immediate heartbeat on collection state change
        private async Task SendImmediateHeartbeatAsync(CancellationToken stoppingToken)
        {
            try
            {
                string currentState = _activeCollections > 0 ? "Collecting" : "Idle";
                long avgCollectionTime = _totalCollected > 0 ? _totalCollectionTimeMs / _totalCollected : 0;

                var status = new CollectorStatus
                {
                    CollectorId = _collectorId,
                    State = currentState,
                    ActiveCollections = _activeCollections,
                    CpuUsage = GetCpuUsage(),
                    MemoryUsageMB = GetMemoryUsage(),
                    LastHeartbeat = DateTime.UtcNow,
                    TotalCollected = _totalCollected,
                    AvgCollectionTimeMs = avgCollectionTime
                };

                await _httpClient.PostAsJsonAsync("/api/collector-health/update", status, stoppingToken);
                _logger.LogDebug($"Immediate heartbeat: State={currentState}, Collections={_activeCollections}");
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
                    string currentState = _activeCollections > 0 ? "Collecting" : "Idle";
                    long avgCollectionTime = _totalCollected > 0 ? _totalCollectionTimeMs / _totalCollected : 0;

                    var status = new CollectorStatus
                    {
                        CollectorId = _collectorId,
                        State = currentState,
                        ActiveCollections = _activeCollections,
                        CpuUsage = GetCpuUsage(),
                        MemoryUsageMB = GetMemoryUsage(),
                        LastHeartbeat = DateTime.UtcNow,
                        TotalCollected = _totalCollected,
                        AvgCollectionTimeMs = avgCollectionTime
                    };

                    var response = await _httpClient.PostAsJsonAsync("/api/collector-health/update", status, stoppingToken);

                    if (response.IsSuccessStatusCode)
                    {
                        _logger.LogDebug($"Heartbeat: State={currentState}, Collections={_activeCollections}, Total={_totalCollected}");
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
            catch
            {
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
            catch
            {
                return 0;
            }
        }

        public override void Dispose()
        {
            _cpuCounter?.Dispose();
            _consumer?.Dispose();
            _httpClient?.Dispose();
            base.Dispose();
        }
    }
}
