using Confluent.Kafka;
using DistributedProcessor.Shared.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Text.Json;

namespace DistributedProcessor.API.Services
{
    public interface ISmartTaskDispatcher
    {
        Task<bool> DispatchTaskAsync(ProcessingTask task);
        Task<string> SelectBestWorkerAsync();
    }

    public class SmartTaskDispatcher : ISmartTaskDispatcher
    {
        private readonly IProducer<string, string> _producer;
        private readonly IWorkerHealthService _workerHealthService;
        private readonly ILogger<SmartTaskDispatcher> _logger;
        private const string TaskTopic = "processing-tasks";

        public SmartTaskDispatcher(
            IWorkerHealthService workerHealthService,
            ILogger<SmartTaskDispatcher> logger,
            IConfiguration config)
        {
            _workerHealthService = workerHealthService;
            _logger = logger;

            var producerConfig = new ProducerConfig
            {
                BootstrapServers = config["Kafka:BootstrapServers"] ?? "localhost:5081"
            };
            _producer = new ProducerBuilder<string, string>(producerConfig).Build();
        }

        public async Task<bool> DispatchTaskAsync(ProcessingTask task)
        {
            try
            {
                // Select best worker based on CPU and Memory
                var bestWorkerId = await SelectBestWorkerAsync();

                if (string.IsNullOrEmpty(bestWorkerId))
                {
                    _logger.LogWarning("No available workers found, task will be picked by any worker");
                    // Don't assign to specific worker, let any available worker take it
                    task.AssignedWorkerId = null;
                }
                else
                {
                    task.AssignedWorkerId = bestWorkerId;
                }

                // Send to single shared topic
                var message = new Message<string, string>
                {
                    Key = task.TaskId,
                    Value = JsonSerializer.Serialize(task)
                };

                var result = await _producer.ProduceAsync(TaskTopic, message);

                _logger.LogInformation(
                    $"Dispatched task {task.TaskId} to worker {bestWorkerId ?? "ANY"} (partition: {result.Partition})");

                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error dispatching task {task.TaskId}");
                return false;
            }
        }

        public async Task<string> SelectBestWorkerAsync()
        {
            try
            {
                var workers = await _workerHealthService.GetAllWorkerStatusesAsync();

                if (workers == null || !workers.Any())
                {
                    _logger.LogWarning("No workers available");
                    return null;
                }

                // Filter only active workers (heartbeat within last 30 seconds)
                var activeWorkers = workers
                    .Where(w => (DateTime.UtcNow - w.LastHeartbeat).TotalSeconds < 30)
                    .ToList();

                if (!activeWorkers.Any())
                {
                    _logger.LogWarning("No active workers found");
                    return null;
                }

                // Calculate worker scores (lower is better)
                var workerScores = activeWorkers.Select(w => new
                {
                    Worker = w,
                    Score = CalculateWorkerLoad(w)
                }).OrderBy(x => x.Score).ToList();

                var bestWorker = workerScores.First().Worker;

                _logger.LogInformation(
                    $"Selected worker {bestWorker.WorkerId}: CPU={bestWorker.CpuUsage:F2}%, " +
                    $"Memory={bestWorker.MemoryUsageMB:F2}MB, ActiveTasks={bestWorker.ActiveTasks}, " +
                    $"Score={workerScores.First().Score:F2}");

                return bestWorker.WorkerId;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error selecting best worker");
                return null;
            }
        }

        private double CalculateWorkerLoad(WorkerStatus worker)
        {
            // Weighted score: 40% CPU + 30% Memory + 30% Active Tasks
            var cpuWeight = 0.4;
            var memoryWeight = 0.3;
            var taskWeight = 0.3;

            // Normalize CPU (0-100%)
            var cpuScore = worker.CpuUsage;

            // Normalize Memory (assuming max 4GB = 4096 MB)
            var memoryScore = (worker.MemoryUsageMB / 4096.0) * 100;

            // Normalize tasks (assuming max 10 concurrent tasks)
            var taskScore = (worker.ActiveTasks / 10.0) * 100;

            var totalScore = (cpuScore * cpuWeight) + (memoryScore * memoryWeight) + (taskScore * taskWeight);

            return totalScore;
        }

        public void Dispose()
        {
            _producer?.Dispose();
        }
    }
}
