using Microsoft.AspNetCore.SignalR;
using DistributedProcessor.API.Services;

namespace DistributedProcessor.API.Hubs
{
    public class MonitoringHub : Hub
    {
        private readonly IDashboardService _dashboardService;
        private readonly IWorkerHealthService _workerHealthService;
        private readonly ILogger<MonitoringHub> _logger;

        public MonitoringHub(
            IDashboardService dashboardService,
            IWorkerHealthService workerHealthService,
            ILogger<MonitoringHub> logger)
        {
            _dashboardService = dashboardService;
            _workerHealthService = workerHealthService;
            _logger = logger;
        }

        public override async Task OnConnectedAsync()
        {
            _logger.LogInformation($"Client connected: {Context.ConnectionId}");

            // Send initial data to newly connected client
            try
            {
                var jobs = await _dashboardService.GetJobSummariesAsync();
                var tasks = await _dashboardService.GetTaskSummariesAsync();
                var workers = await _workerHealthService.GetAllWorkerStatusesAsync();

                await Clients.Caller.SendAsync("JobsUpdate", jobs);
                await Clients.Caller.SendAsync("TasksUpdate", tasks);
                await Clients.Caller.SendAsync("WorkersUpdate", workers);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error sending initial data to client");
            }

            await base.OnConnectedAsync();
        }

        public override async Task OnDisconnectedAsync(Exception exception)
        {
            _logger.LogInformation($"Client disconnected: {Context.ConnectionId}");
            await base.OnDisconnectedAsync(exception);
        }

        // Client can request immediate update
        public async Task RequestUpdate()
        {
            _logger.LogInformation($"Client {Context.ConnectionId} requested update");

            try
            {
                var jobs = await _dashboardService.GetJobSummariesAsync();
                var tasks = await _dashboardService.GetTaskSummariesAsync();
                var workers = await _workerHealthService.GetAllWorkerStatusesAsync();

                await Clients.Caller.SendAsync("JobsUpdate", jobs);
                await Clients.Caller.SendAsync("TasksUpdate", tasks);
                await Clients.Caller.SendAsync("WorkersUpdate", workers);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error sending update to client");
            }
        }
    }
}
