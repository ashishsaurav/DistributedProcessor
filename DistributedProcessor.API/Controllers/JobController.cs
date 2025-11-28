using Microsoft.AspNetCore.Mvc;
using DistributedProcessor.API.Services;
using DistributedProcessor.Shared.Models;

namespace DistributedProcessor.API.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class JobController : ControllerBase
    {
        private readonly ISimpleJobOrchestrator _jobOrchestrator;
        private readonly ILogger<JobController> _logger;

        public JobController(ISimpleJobOrchestrator jobOrchestrator, ILogger<JobController> logger)
        {
            _jobOrchestrator = jobOrchestrator;
            _logger = logger;
        }

        [HttpPost("submit-simple")]
        public async Task<IActionResult> SubmitSimpleJob([FromBody] SimpleJobRequest request)
        {
            try
            {
                if (string.IsNullOrEmpty(request.Fund))
                {
                    return BadRequest(new { error = "Fund is required" });
                }

                if (request.StartDate >= request.EndDate)
                {
                    return BadRequest(new { error = "StartDate must be before EndDate" });
                }

                var response = await _jobOrchestrator.SubmitJobAsync(request);

                if (response.Success)
                {
                    return Ok(response);
                }
                else
                {
                    return BadRequest(response);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error submitting job");
                return StatusCode(500, new { error = ex.Message });
            }
        }

        [HttpPost("submit-batch")]
        public async Task<IActionResult> SubmitBatchJob([FromBody] BatchJobRequest request)
        {
            try
            {
                if (request.Funds == null || !request.Funds.Any())
                {
                    return BadRequest(new { error = "At least one fund is required" });
                }

                if (request.StartDate >= request.EndDate)
                {
                    return BadRequest(new { error = "StartDate must be before EndDate" });
                }

                var response = await _jobOrchestrator.SubmitBatchJobAsync(request);

                if (response.Success)
                {
                    return Ok(response);
                }
                else
                {
                    return BadRequest(response);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error submitting batch job");
                return StatusCode(500, new { error = ex.Message });
            }
        }
    }
}
