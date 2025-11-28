using Microsoft.AspNetCore.Mvc;
using DistributedProcessor.Data;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;

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

        [HttpPost("update-status")]
        public async Task<IActionResult> UpdateTaskStatus([FromBody] TaskStatusUpdate update)
        {
            try
            {
                var sql = @"
                    UPDATE TaskLogs 
                    SET Status = {0},
                        WorkerId = COALESCE(WorkerId, {1}),
                        StartedAt = CASE WHEN {0} = 'Processing' AND StartedAt IS NULL THEN GETUTCDATE() ELSE StartedAt END
                    WHERE TaskId = {2}";

                var rowsAffected = await _context.Database.ExecuteSqlRawAsync(
                    sql,
                    update.Status,
                    update.WorkerId ?? "Unknown",
                    update.TaskId);

                if (rowsAffected > 0)
                {
                    _logger.LogInformation($"Updated task {update.TaskId} to status {update.Status}");
                    return Ok(new { message = "Task status updated" });
                }
                else
                {
                    return NotFound(new { error = "Task not found" });
                }
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
        public string TaskId { get; set; }
        public string Status { get; set; }
        public string WorkerId { get; set; }
    }
}
