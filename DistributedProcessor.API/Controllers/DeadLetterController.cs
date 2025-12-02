using DistributedProcessor.API.Services;
using Microsoft.AspNetCore.Mvc;

namespace DistributedProcessor.API.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class DeadLetterController : ControllerBase
    {
        private readonly IDeadLetterService _dlqService;

        public DeadLetterController(IDeadLetterService dlqService)
        {
            _dlqService = dlqService;
        }

        [HttpPost("send")]
        public async Task<IActionResult> SendToDlq([FromBody] DlqRequest request)
        {
            try
            {
                await _dlqService.SendToDeadLetterAsync(
                    request.Message,
                    request.Topic,
                    new Exception(request.ErrorMessage) { Source = request.ExceptionType },
                    request.RetryCount,
                    request.WorkerId,
                    request.JobId,
                    request.TaskId
                );

                return Ok(new { message = "Sent to DLQ" });
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { error = ex.Message });
            }
        }

        [HttpGet("pending")]
        public async Task<IActionResult> GetPending()
        {
            var messages = await _dlqService.GetPendingAsync();
            return Ok(messages);
        }
    }

    public class DlqRequest
    {
        public object Message { get; set; } = null!;
        public string Topic { get; set; } = string.Empty;
        public string ErrorMessage { get; set; } = string.Empty;
        public string ExceptionType { get; set; } = string.Empty;
        public int RetryCount { get; set; }
        public string? WorkerId { get; set; }
        public string? JobId { get; set; }
        public string? TaskId { get; set; }
    }
}
