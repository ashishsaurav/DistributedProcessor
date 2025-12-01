using DistributedProcessor.API.Hubs;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace DistributedProcessor.API.Services
{
    public class DashboardUpdateService : BackgroundService
    {
        private readonly IHubContext<MonitoringHub> _hubContext;
        private readonly IServiceProvider _serviceProvider;
        private readonly ILogger<DashboardUpdateService> _logger;

        public DashboardUpdateService(
            IHubContext<MonitoringHub> hubContext,
            IServiceProvider serviceProvider,
            ILogger<DashboardUpdateService> logger)
        {
            _hubContext = hubContext;
            _serviceProvider = serviceProvider;
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("Dashboard Update Service started - Broadcasting every 500ms for real-time visibility");

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    using var scope = _serviceProvider.CreateScope();
                    var dashboardService = scope.ServiceProvider.GetRequiredService<IDashboardService>();
                    var workerHealthService = scope.ServiceProvider.GetRequiredService<IWorkerHealthService>();
                    var collectorHealthService = scope.ServiceProvider.GetRequiredService<ICollectorHealthService>(); // NEW

                    var jobs = await dashboardService.GetJobSummariesAsync();
                    var tasks = await dashboardService.GetTaskSummariesAsync();
                    var workers = await workerHealthService.GetAllWorkerStatusesAsync();
                    var collectors = await collectorHealthService.GetAllCollectorStatusesAsync(); // NEW

                    await _hubContext.Clients.All.SendAsync("JobsUpdate", jobs, stoppingToken);
                    await _hubContext.Clients.All.SendAsync("TasksUpdate", tasks, stoppingToken);
                    await _hubContext.Clients.All.SendAsync("WorkersUpdate", workers, stoppingToken);
                    await _hubContext.Clients.All.SendAsync("CollectorsUpdate", collectors, stoppingToken); // NEW

                    _logger.LogDebug($"Broadcast: {jobs.Count} jobs, {tasks.Count} tasks, {workers.Count} workers, {collectors.Count} collectors");
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error broadcasting dashboard updates");
                }

                await Task.Delay(TimeSpan.FromMilliseconds(500), stoppingToken);
            }
        }
    }
}
