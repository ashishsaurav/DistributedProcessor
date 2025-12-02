using DistributedProcessor.Data;
using Microsoft.EntityFrameworkCore;

namespace DistributedProcessor.API.Services
{
    public class OutboxCleanupService : BackgroundService
    {
        private readonly IServiceScopeFactory _scopeFactory;
        private readonly ILogger<OutboxCleanupService> _logger;

        public OutboxCleanupService(
            IServiceScopeFactory scopeFactory,
            ILogger<OutboxCleanupService> logger)
        {
            _scopeFactory = scopeFactory;
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("Outbox Cleanup Service started");

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(TimeSpan.FromHours(1), stoppingToken);

                    using var scope = _scopeFactory.CreateScope();
                    var context = scope.ServiceProvider.GetRequiredService<ApplicationDbContext>();

                    // Delete messages older than 7 days
                    var cutoffDate = DateTime.UtcNow.AddDays(-7);
                    var deleted = await context.OutboxMessages
                        .Where(m => m.Status == "Sent" && m.ProcessedAt < cutoffDate)
                        .ExecuteDeleteAsync(stoppingToken);

                    if (deleted > 0)
                    {
                        _logger.LogInformation("Cleaned up {Count} old outbox messages", deleted);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error in outbox cleanup");
                }
            }
        }
    }
}
