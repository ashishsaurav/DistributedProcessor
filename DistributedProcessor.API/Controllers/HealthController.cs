using DistributedProcessor.API.Services;
using DistributedProcessor.Shared.Models;
using Microsoft.AspNetCore.Mvc;

namespace DistributedProcessor.API.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class HealthController : ControllerBase
    {
        private readonly IWorkerHealthService _workerHealthService;
        private readonly ILogger<HealthController> _logger;

        public HealthController(IWorkerHealthService workerHealthService, ILogger<HealthController> logger)
        {
            _workerHealthService = workerHealthService;
            _logger = logger;
        }

        [HttpPost("update")]
        public async Task<IActionResult> UpdateWorkerStatus([FromBody] WorkerStatus status)
        {
            try
            {
                _logger.LogInformation($"Heartbeat from {status.WorkerId}: State={status.State}, Tasks={status.ActiveTasks}, CPU={status.CpuUsage:F2}%, Memory={status.MemoryUsageMB:F2}MB");

                // Just update the status in cache - DashboardUpdateService will broadcast
                await _workerHealthService.UpdateWorkerStatusAsync(status);

                return Ok(new { message = "Status updated" });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error updating worker status for {status.WorkerId}");
                return StatusCode(500, new { error = ex.Message });
            }
        }

        [HttpGet("workers")]
        public async Task<IActionResult> GetWorkers()
        {
            try
            {
                var workers = await _workerHealthService.GetAllWorkerStatusesAsync();
                _logger.LogInformation($"Returning {workers.Count} workers");
                return Ok(workers);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error getting workers");
                return StatusCode(500, new { error = ex.Message });
            }
        }

        [HttpGet("debug-workers")]
        public async Task<IActionResult> DebugWorkers()
        {
            var workers = await _workerHealthService.GetAllWorkerStatusesAsync();
            return Ok(new
            {
                count = workers.Count,
                timestamp = DateTime.UtcNow,
                workers = workers.Select(w => new
                {
                    w.WorkerId,
                    w.State,
                    w.ActiveTasks,
                    w.CpuUsage,
                    w.MemoryUsageMB,
                    w.LastHeartbeat,
                    secondsSinceLastHeartbeat = (DateTime.UtcNow - w.LastHeartbeat).TotalSeconds,
                    isOnline = (DateTime.UtcNow - w.LastHeartbeat).TotalSeconds < 30
                })
            });
        }
    }
}
