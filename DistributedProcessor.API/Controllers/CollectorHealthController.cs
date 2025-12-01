using DistributedProcessor.API.Services;
using DistributedProcessor.Shared.Models;
using Microsoft.AspNetCore.Mvc;

namespace DistributedProcessor.API.Controllers
{
    [ApiController]
    [Route("api/collector-health")]
    public class CollectorHealthController : ControllerBase
    {
        private readonly ICollectorHealthService _collectorHealthService;
        private readonly ILogger<CollectorHealthController> _logger;

        public CollectorHealthController(
            ICollectorHealthService collectorHealthService,
            ILogger<CollectorHealthController> logger)
        {
            _collectorHealthService = collectorHealthService;
            _logger = logger;
        }

        [HttpPost("update")]
        public async Task<IActionResult> UpdateCollectorStatus([FromBody] CollectorStatus status)
        {
            try
            {
                await _collectorHealthService.UpdateCollectorStatusAsync(status);
                return Ok(new { message = "Collector status updated" });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error updating collector status");
                return StatusCode(500, new { error = ex.Message });
            }
        }

        [HttpGet("all")]
        public async Task<IActionResult> GetAllCollectors()
        {
            try
            {
                var collectors = await _collectorHealthService.GetAllCollectorStatusesAsync();
                return Ok(collectors);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error getting all collectors");
                return StatusCode(500, new { error = ex.Message });
            }
        }

        [HttpGet("{collectorId}")]
        public async Task<IActionResult> GetCollectorStatus(string collectorId)
        {
            try
            {
                var status = await _collectorHealthService.GetCollectorStatusAsync(collectorId);
                if (status == null)
                {
                    return NotFound(new { message = "Collector not found" });
                }
                return Ok(status);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error getting collector {collectorId}");
                return StatusCode(500, new { error = ex.Message });
            }
        }
    }
}
