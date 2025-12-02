// DistributedProcessor.API/Controllers/IdempotencyController.cs
using DistributedProcessor.API.Services;
using Microsoft.AspNetCore.Mvc;

namespace DistributedProcessor.API.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class IdempotencyController : ControllerBase
    {
        private readonly IIdempotencyService _idempotencyService;

        public IdempotencyController(IIdempotencyService idempotencyService)
        {
            _idempotencyService = idempotencyService;
        }

        [HttpGet("can-process/{taskId}")]
        public async Task<IActionResult> CanProcess(string taskId, [FromQuery] string workerId)
        {
            var canProcess = await _idempotencyService.CanProcessAsync(taskId, workerId);
            return Ok(new { canProcess });
        }

        [HttpPost("complete/{taskId}")]
        public async Task<IActionResult> MarkCompleted(string taskId)
        {
            await _idempotencyService.MarkCompletedAsync(taskId);
            return Ok();
        }

        [HttpPost("failed/{taskId}")]
        public async Task<IActionResult> MarkFailed(string taskId)
        {
            await _idempotencyService.MarkFailedAsync(taskId);
            return Ok();
        }
    }
}
