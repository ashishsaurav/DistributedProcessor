using Microsoft.Extensions.Caching.Memory;
using DistributedProcessor.Shared.Models;
using Microsoft.Extensions.Logging;

namespace DistributedProcessor.API.Services
{
    public interface IWorkerHealthService
    {
        Task UpdateWorkerStatusAsync(WorkerStatus status);
        Task<List<WorkerStatus>> GetAllWorkerStatusesAsync();
        Task<WorkerStatus> GetWorkerStatusAsync(string workerId);
    }

    public class WorkerHealthService : IWorkerHealthService
    {
        private readonly IMemoryCache _cache;
        private readonly ILogger<WorkerHealthService> _logger;
        private const int HeartbeatTimeoutSeconds = 90;
        private const string WorkerListKey = "worker_list";

        public WorkerHealthService(IMemoryCache cache, ILogger<WorkerHealthService> logger)
        {
            _cache = cache;
            _logger = logger;
        }

        public Task UpdateWorkerStatusAsync(WorkerStatus status)
        {
            var key = $"worker:{status.WorkerId}";
            _cache.Set(key, status, TimeSpan.FromSeconds(HeartbeatTimeoutSeconds));

            var workerIds = _cache.GetOrCreate(WorkerListKey, e =>
            {
                e.SlidingExpiration = TimeSpan.FromDays(1);
                return new HashSet<string>();
            });
            workerIds.Add(status.WorkerId);
            _cache.Set(WorkerListKey, workerIds);

            _logger.LogInformation($"Worker heartbeat: {status.WorkerId}, State={status.State}, Tasks={status.ActiveTasks}, CPU={status.CpuUsage:F2}%");

            return Task.CompletedTask;
        }

        public Task<List<WorkerStatus>> GetAllWorkerStatusesAsync()
        {
            var workerIds = _cache.Get<HashSet<string>>(WorkerListKey) ?? new HashSet<string>();
            var workers = new List<WorkerStatus>();
            var now = DateTime.UtcNow;

            foreach (var workerId in workerIds)
            {
                if (_cache.TryGetValue($"worker:{workerId}", out WorkerStatus worker))
                {
                    // Mark as offline if no heartbeat in last 90 seconds
                    if ((now - worker.LastHeartbeat).TotalSeconds > HeartbeatTimeoutSeconds)
                    {
                        worker.State = "Offline";
                    }
                    workers.Add(worker);
                }
            }

            _logger.LogDebug($"GetAllWorkers: returning {workers.Count} workers");
            return Task.FromResult(workers);
        }

        public Task<WorkerStatus> GetWorkerStatusAsync(string workerId)
        {
            _cache.TryGetValue($"worker:{workerId}", out WorkerStatus status);
            return Task.FromResult(status);
        }
    }
}
