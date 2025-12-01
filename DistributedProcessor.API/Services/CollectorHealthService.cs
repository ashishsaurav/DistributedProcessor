using DistributedProcessor.Shared.Models;
using Microsoft.Extensions.Caching.Memory;

namespace DistributedProcessor.API.Services
{
    public interface ICollectorHealthService
    {
        Task UpdateCollectorStatusAsync(CollectorStatus status);
        Task<List<CollectorStatus>> GetAllCollectorStatusesAsync();
        Task<CollectorStatus?> GetCollectorStatusAsync(string collectorId);
    }

    public class CollectorHealthService : ICollectorHealthService
    {
        private readonly IMemoryCache _cache;
        private readonly ILogger<CollectorHealthService> _logger;
        private const string COLLECTOR_CACHE_PREFIX = "collector_";
        private const string ALL_COLLECTORS_KEY = "all_collectors";

        public CollectorHealthService(IMemoryCache cache, ILogger<CollectorHealthService> logger)
        {
            _cache = cache;
            _logger = logger;
        }

        public Task UpdateCollectorStatusAsync(CollectorStatus status)
        {
            var cacheKey = $"{COLLECTOR_CACHE_PREFIX}{status.CollectorId}";

            _cache.Set(cacheKey, status, TimeSpan.FromMinutes(5));

            // Update collectors list
            var allCollectors = _cache.Get<HashSet<string>>(ALL_COLLECTORS_KEY) ?? new HashSet<string>();
            allCollectors.Add(status.CollectorId);
            _cache.Set(ALL_COLLECTORS_KEY, allCollectors, TimeSpan.FromHours(24));

            _logger.LogDebug($"Collector {status.CollectorId} status updated: State={status.State}, Collections={status.ActiveCollections}, Total={status.TotalCollected}");

            return Task.CompletedTask;
        }

        public Task<List<CollectorStatus>> GetAllCollectorStatusesAsync()
        {
            var allCollectors = _cache.Get<HashSet<string>>(ALL_COLLECTORS_KEY) ?? new HashSet<string>();
            var statuses = new List<CollectorStatus>();

            foreach (var collectorId in allCollectors)
            {
                var cacheKey = $"{COLLECTOR_CACHE_PREFIX}{collectorId}";
                if (_cache.TryGetValue(cacheKey, out CollectorStatus status))
                {
                    statuses.Add(status);
                }
            }

            return Task.FromResult(statuses);
        }

        public Task<CollectorStatus?> GetCollectorStatusAsync(string collectorId)
        {
            var cacheKey = $"{COLLECTOR_CACHE_PREFIX}{collectorId}";
            _cache.TryGetValue(cacheKey, out CollectorStatus status);
            return Task.FromResult(status);
        }
    }
}
