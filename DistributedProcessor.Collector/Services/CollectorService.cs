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
using DistributedProcessor.Data.Models;
using DistributedProcessor.Data;

namespace DistributedProcessor.Collector.Services
{
    public class CollectorService : BackgroundService
    {
        private readonly ILogger<CollectorService> _logger;
        private readonly IServiceScopeFactory _scopeFactory;
        private readonly IConsumer<string, string> _consumer;
        private readonly HttpClient _httpClient;
        private readonly string _collectorId;

        // Graceful shutdown fields
        private readonly CancellationTokenSource _shutdownCts = new();
        private volatile bool _isShuttingDown = false;
        private readonly TimeSpan _shutdownTimeout = TimeSpan.FromSeconds(30);
        private readonly SemaphoreSlim _shutdownLock = new(1, 1);

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
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
                stoppingToken, _shutdownCts.Token);
            var cancellationToken = linkedCts.Token;

            _consumer.Subscribe("processing-results");
            _logger.LogInformation("Collector {CollectorId} started", _collectorId);

            var heartbeatTask = Task.Run(() => SendHeartbeatsAsync(cancellationToken));

            try
            {
                while (!cancellationToken.IsCancellationRequested && !_isShuttingDown)
                {
                    ConsumeResult<string, string>? consumeResult = null;

                    try
                    {
                        consumeResult = _consumer.Consume(TimeSpan.FromMilliseconds(100));

                        if (consumeResult?.Message?.Value == null)
                            continue;

                        // Check shutdown before accepting new collection
                        if (_isShuttingDown)
                        {
                            _logger.LogInformation(
                                "Shutdown in progress, not accepting new result");
                            break;
                        }

                        var result = JsonSerializer.Deserialize<ProcessingResult>(
                            consumeResult.Message.Value);

                        if (result == null)
                        {
                            _consumer.Commit(consumeResult);
                            continue;
                        }

                        Interlocked.Increment(ref _activeCollections);

                        // Process collection
                        await ProcessCollectionAsync(result, consumeResult, cancellationToken);

                        Interlocked.Increment(ref _totalCollected);
                    }
                    catch (ConsumeException cex)
                    {
                        _logger.LogError(cex, "Kafka consume error: {Reason}", cex.Error.Reason);
                    }
                    catch (OperationCanceledException)
                    {
                        _logger.LogInformation("Collector operation cancelled");
                        break;
                    }
                    finally
                    {
                        if (_activeCollections > 0)
                            Interlocked.Decrement(ref _activeCollections);
                    }
                }
            }
            finally
            {
                await ShutdownGracefullyAsync();
            }
        }

        private async Task ProcessCollectionAsync(
            ProcessingResult result,
            ConsumeResult<string, string> consumeResult,
            CancellationToken cancellationToken)
        {
            try
            {
                using var scope = _scopeFactory.CreateScope();
                var dbService = scope.ServiceProvider.GetRequiredService<IDbService>();
                var context = scope.ServiceProvider.GetRequiredService<ApplicationDbContext>();

                _logger.LogInformation("Saving results for TaskId {TaskId}", result.TaskId);

                // START TRANSACTION
                using var transaction = await context.Database.BeginTransactionAsync(cancellationToken);

                try
                {
                    // 1. Save calculated results to database
                    await dbService.SaveCalculatedResultsAsync(result);

                    // 2. Add status update to outbox (instead of direct API call)
                    var statusUpdate = new
                    {
                        taskId = result.TaskId,
                        status = "Collected",
                        workerId = _collectorId,
                        rowCount = result.CalculatedRows?.Count ?? 0
                    };

                    var outboxMessage = new OutboxMessage
                    {
                        MessageId = Guid.NewGuid().ToString(),
                        Topic = "task-status-updates", // Virtual topic for status updates
                        MessageKey = result.TaskId,
                        Payload = JsonSerializer.Serialize(statusUpdate),
                        MessageType = "TaskStatusUpdate",
                        CreatedAt = DateTime.UtcNow,
                        Status = "Pending"
                    };

                    context.OutboxMessages.Add(outboxMessage);

                    // 3. Save everything atomically
                    await context.SaveChangesAsync(cancellationToken);

                    // 4. Commit transaction
                    await transaction.CommitAsync(cancellationToken);

                    // 5. Now commit Kafka offset (after DB transaction succeeds)
                    _consumer.Commit(consumeResult);

                    _logger.LogInformation(
                        "Collection completed for TaskId {TaskId} with transactional guarantee",
                        result.TaskId);
                }
                catch (Exception ex)
                {
                    await transaction.RollbackAsync(cancellationToken);
                    throw;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error collecting results for task {TaskId}", result.TaskId);

                // Only commit offset if we want to skip this message
                _consumer.Commit(consumeResult);
            }
        }

        private async Task ShutdownGracefullyAsync()
        {
            await _shutdownLock.WaitAsync();
            try
            {
                if (_isShuttingDown) return;
                _isShuttingDown = true;

                _logger.LogInformation(
                    "Collector {CollectorId} initiating graceful shutdown. Active: {Count}",
                    _collectorId, _activeCollections);

                // Unsubscribe
                _consumer.Unsubscribe();

                // Wait for active collections
                var sw = Stopwatch.StartNew();
                while (_activeCollections > 0 && sw.Elapsed < _shutdownTimeout)
                {
                    _logger.LogInformation(
                        "Waiting for {Count} collection(s)... ({Elapsed:F0}s)",
                        _activeCollections, sw.Elapsed.TotalSeconds);
                    await Task.Delay(1000);
                }

                // Commit final offsets
                try
                {
                    _consumer.Commit();
                    _logger.LogInformation("Final offsets committed");
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Error committing final offsets");
                }

                // Send offline heartbeat
                await SendOfflineHeartbeatAsync();

                // Close consumer
                _consumer.Close();

                _logger.LogInformation(
                    "Collector {CollectorId} shutdown complete. Total: {Total}",
                    _collectorId, _totalCollected);
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
                    collectorId = _collectorId,
                    state = "Offline",
                    activeCollections = 0,
                    totalCollected = _totalCollected,
                    lastHeartbeat = DateTime.UtcNow
                };

                await _httpClient.PostAsJsonAsync("api/collector-health/update", status);
                _logger.LogInformation("Sent offline heartbeat");
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to send offline heartbeat");
            }
        }

        private async Task UpdateTaskStatusAsync(
        string taskId,
        string status,
        CancellationToken cancellationToken)
        {
            try
            {
                var updateData = new
                {
                    taskId = taskId,
                    status = status,
                    workerId = _collectorId
                };

                await _httpClient.PutAsJsonAsync("api/task/update-status", updateData, cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to update task status");
            }
        }

        private async Task SendHeartbeatsAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested && !_isShuttingDown)
            {
                try
                {
                    var status = new
                    {
                        collectorId = _collectorId,
                        state = _activeCollections > 0 ? "Collecting" : "Idle",
                        activeCollections = _activeCollections,
                        totalCollected = _totalCollected,
                        lastHeartbeat = DateTime.UtcNow
                    };

                    await _httpClient.PostAsJsonAsync("api/collector-health/update", status);
                    await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Failed to send heartbeat");
                }
            }
        }

        public override async Task StopAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("StopAsync called for collector {CollectorId}", _collectorId);
            _isShuttingDown = true;
            _shutdownCts.Cancel();
            await base.StopAsync(cancellationToken);
        }

        public override void Dispose()
        {
            _shutdownCts?.Dispose();
            _shutdownLock?.Dispose();
            _consumer?.Dispose();
            _httpClient?.Dispose();
            base.Dispose();
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
