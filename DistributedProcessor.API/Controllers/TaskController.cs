using DistributedProcessor.Data;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

namespace DistributedProcessor.API.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class TaskController : ControllerBase
    {
        private readonly ApplicationDbContext _context;
        private readonly ILogger<TaskController> _logger;

        public TaskController(ApplicationDbContext context, ILogger<TaskController> logger)
        {
            _context = context;
            _logger = logger;
        }

        [HttpPut("update-status")]
        public async Task<IActionResult> UpdateTaskStatus([FromBody] TaskStatusUpdate update)
        {
            try
            {
                var taskLog = await _context.TaskLogs
                    .FirstOrDefaultAsync(t => t.TaskId == update.TaskId);

                if (taskLog == null)
                {
                    _logger.LogWarning($"Task {update.TaskId} not found in database");
                    return NotFound(new { message = "Task not found" });
                }

                taskLog.Status = update.Status;

                // Update WorkerId if provided
                if (!string.IsNullOrEmpty(update.WorkerId))
                {
                    taskLog.WorkerId = update.WorkerId;
                }

                // Update RowCount if provided
                if (update.RowCount > 0)
                {
                    taskLog.RowsProcessed = update.RowCount;
                }

                // Set timestamps based on status
                if (update.Status == "Processing" && taskLog.StartedAt == null)
                {
                    taskLog.StartedAt = DateTime.UtcNow;
                    _logger.LogInformation($"Task {update.TaskId} started by worker {update.WorkerId}");
                }
                else if (update.Status == "Completed" && taskLog.CompletedAt == null)
                {
                    taskLog.CompletedAt = DateTime.UtcNow;
                    _logger.LogInformation($"Task {update.TaskId} completed by worker {update.WorkerId} ({update.RowCount} rows)");
                }
                else if (update.Status == "Failed" && taskLog.CompletedAt == null)
                {
                    taskLog.CompletedAt = DateTime.UtcNow;
                    _logger.LogError($"Task {update.TaskId} failed on worker {update.WorkerId}");
                }

                await _context.SaveChangesAsync();

                return Ok(new { message = "Task status updated" });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error updating task status for {update.TaskId}");
                return StatusCode(500, new { error = ex.Message });
            }
        }
    }

    public class TaskStatusUpdate
    {
        public string TaskId { get; set; } = string.Empty;
        public string Status { get; set; } = string.Empty;
        public string? WorkerId { get; set; }
        public int RowCount { get; set; }
    }
}
