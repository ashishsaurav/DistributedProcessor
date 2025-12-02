using DistributedProcessor.Data;
using Microsoft.EntityFrameworkCore;

namespace DistributedProcessor.API.Services
{
    public class IdempotencyCleanupService : BackgroundService
    {
        private readonly IServiceScopeFactory _scopeFactory;
        private readonly ILogger<IdempotencyCleanupService> _logger;

        public IdempotencyCleanupService(
            IServiceScopeFactory scopeFactory,
            ILogger<IdempotencyCleanupService> logger)
        {
            _scopeFactory = scopeFactory;
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("Idempotency cleanup service started");

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(TimeSpan.FromHours(1), stoppingToken);

                    using var scope = _scopeFactory.CreateScope();
                    var context = scope.ServiceProvider.GetRequiredService<ApplicationDbContext>();

                    var deleted = await context.IdempotencyKeys
                        .Where(k => k.ExpiresAt < DateTime.UtcNow)
                        .ExecuteDeleteAsync(stoppingToken);

                    if (deleted > 0)
                    {
                        _logger.LogInformation("Cleaned up {Count} expired idempotency keys", deleted);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error in idempotency cleanup");
                }
            }
        }
    }
}
