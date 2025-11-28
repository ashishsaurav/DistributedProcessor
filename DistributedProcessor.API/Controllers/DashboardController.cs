using Microsoft.AspNetCore.Mvc;
using DistributedProcessor.API.Services;
using Microsoft.EntityFrameworkCore;

namespace DistributedProcessor.API.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class DashboardController : ControllerBase
    {
        private readonly IDashboardService _dashboardService;

        public DashboardController(IDashboardService dashboardService)
        {
            _dashboardService = dashboardService;
        }

        [HttpGet("state")]
        public async Task<IActionResult> GetDashboardState()
        {
            var state = await _dashboardService.GetDashboardStateAsync();
            return Ok(state);
        }

        [HttpGet("jobs")]
        public async Task<IActionResult> GetJobs()
        {
            var jobs = await _dashboardService.GetJobSummariesAsync();
            return Ok(jobs);
        }

        [HttpGet("tasks")]
        public async Task<IActionResult> GetTasks([FromQuery] string jobId = null)
        {
            var tasks = await _dashboardService.GetTaskSummariesAsync(jobId);
            return Ok(tasks);
        }

        [HttpGet("funds")]
        public async Task<IActionResult> GetFunds()
        {
            try
            {
                var funds = await _dashboardService.GetAvailableFundsAsync();
                return Ok(funds);
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { error = ex.Message });
            }
        }

        [HttpGet("symbols")]
        public async Task<IActionResult> GetSymbols([FromQuery] string fund = null)
        {
            try
            {
                var symbols = await _dashboardService.GetAvailableSymbolsAsync(fund);
                return Ok(symbols);
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { error = ex.Message });
            }
        }
    }
}
