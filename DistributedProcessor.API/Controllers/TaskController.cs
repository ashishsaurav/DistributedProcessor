using DistributedProcessor.Data;
using DistributedProcessor.API.Hubs;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.SignalR;
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
        private readonly IHubContext<MonitoringHub> _hubContext; // NEW: Event-driven updates

        public TaskController(
            ApplicationDbContext context,
            ILogger<TaskController> logger,
            IHubContext<MonitoringHub> hubContext)
        {
            _context = context;
            _logger = logger;
            _hubContext = hubContext;
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

                var oldStatus = taskLog.Status;
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

                // Update duration if provided
                if (update.DurationMs > 0)
                {
                    if (update.Status == "Processed" || update.Status == "Completed")
                    {
                        taskLog.ProcessingDurationMs = update.DurationMs;
                    }
                    else if (update.Status == "Collected")
                    {
                        taskLog.CollectionDurationMs = update.DurationMs;
                    }
                }

                // Update error message if provided
                if (!string.IsNullOrEmpty(update.ErrorMessage))
                {
                    taskLog.ErrorMessage = update.ErrorMessage;
                }

                // Set timestamps based on status
                if (update.Status == "Processing" && taskLog.StartedAt == null)
                {
                    taskLog.StartedAt = DateTime.UtcNow;
                    _logger.LogInformation($"Task {update.TaskId} started by worker {update.WorkerId}");
                }
                else if (update.Status == "Processed" && taskLog.ProcessedAt == null)
                {
                    taskLog.ProcessedAt = DateTime.UtcNow;
                    _logger.LogInformation($"Task {update.TaskId} processed by worker {update.WorkerId} ({update.RowCount} rows in {update.DurationMs}ms)");
                }
                else if (update.Status == "Collected" && taskLog.CollectedAt == null)
                {
                    taskLog.CollectedAt = DateTime.UtcNow;
                    _logger.LogInformation($"Task {update.TaskId} collected by collector ({update.DurationMs}ms)");
                }
                else if (update.Status == "Completed" && taskLog.CompletedAt == null)
                {
                    taskLog.CompletedAt = DateTime.UtcNow;
                    _logger.LogInformation($"Task {update.TaskId} completed ({update.RowCount} rows)");
                }
                else if (update.Status == "Failed" && taskLog.CompletedAt == null)
                {
                    taskLog.CompletedAt = DateTime.UtcNow;
                    _logger.LogError($"Task {update.TaskId} failed on worker {update.WorkerId}: {update.ErrorMessage}");
                }

                await _context.SaveChangesAsync();

                // NEW: Trigger immediate broadcast to all connected clients
                await BroadcastTaskUpdateAsync(taskLog);

                // NEW: Trigger job status recalculation if task reached terminal state
                if (update.Status == "Completed" || update.Status == "Failed" || update.Status == "Collected")
                {
                    await TriggerJobStatusUpdateAsync(taskLog.JobId);
                }

                return Ok(new { message = "Task status updated" });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error updating task status for {update.TaskId}");
                return StatusCode(500, new { error = ex.Message });
            }
        }

        // NEW: Broadcast task update immediately via SignalR
        private async Task BroadcastTaskUpdateAsync(Data.Models.TaskLog taskLog)
        {
            try
            {
                var taskSummary = new
                {
                    taskLog.TaskId,
                    taskLog.JobId,
                    Fund = taskLog.Fund ?? "",
                    Symbol = taskLog.Symbol ?? "",
                    taskLog.Status,
                    taskLog.WorkerId,
                    RowsProcessed = taskLog.RowsProcessed ?? 0,
                    taskLog.StartedAt,
                    taskLog.ProcessedAt,
                    taskLog.CollectedAt,
                    taskLog.CompletedAt,
                    taskLog.ProcessingDurationMs,
                    taskLog.CollectionDurationMs
                };

                await _hubContext.Clients.All.SendAsync("TaskStatusChanged", taskSummary);
                _logger.LogDebug($"Broadcasted task update for {taskLog.TaskId} - Status: {taskLog.Status}");
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to broadcast task update");
            }
        }

        private async Task TriggerJobStatusUpdateAsync(string jobId)
        {
            try
            {
                var jobExecution = await _context.JobExecutions
                    .FirstOrDefaultAsync(j => j.JobId == jobId);

                if (jobExecution == null) return;

                var tasks = await _context.TaskLogs
                    .Where(t => t.JobId == jobId)
                    .ToListAsync();

                if (tasks.Count == 0) return;

                var totalTasks = tasks.Count;
                var completedTasks = tasks.Count(t => t.Status == "Completed");
                var collectedTasks = tasks.Count(t => t.Status == "Collected");
                var failedTasks = tasks.Count(t => t.Status == "Failed");
                var processingTasks = tasks.Count(t => t.Status == "Processing");
                var processedTasks = tasks.Count(t => t.Status == "Processed");
                var pendingTasks = tasks.Count(t => t.Status == "Pending");
                var processedRows = tasks.Sum(t => t.RowsProcessed ?? 0);

                // FIXED: Determine job status - Collected is a finished state
                string newStatus = jobExecution.Status;
                DateTime? startedAt = jobExecution.StartedAt;
                DateTime? completedAt = jobExecution.CompletedAt;

                int finishedTasks = completedTasks + collectedTasks + failedTasks;

                if (finishedTasks == totalTasks)
                {
                    newStatus = failedTasks > 0 ? "Completed with Errors" : "Completed";
                    completedAt = DateTime.UtcNow;
                }
                else if (processingTasks > 0 || processedTasks > 0 || collectedTasks > 0)
                {
                    newStatus = "Processing";
                    if (startedAt == null)
                    {
                        startedAt = DateTime.UtcNow;
                    }
                }

                // Update job
                jobExecution.Status = newStatus;
                jobExecution.CompletedTasks = completedTasks + collectedTasks; // FIXED
                jobExecution.FailedTasks = failedTasks;
                jobExecution.ProcessedRows = processedRows;
                jobExecution.StartedAt = startedAt;
                jobExecution.CompletedAt = completedAt;

                await _context.SaveChangesAsync();

                // Broadcast job update with correct counts
                await _hubContext.Clients.All.SendAsync("JobStatusChanged", new
                {
                    jobExecution.JobId,
                    jobExecution.Fund,
                    jobExecution.Status,
                    jobExecution.TotalTasks,
                    CompletedTasks = completedTasks + collectedTasks, // FIXED
                    PendingTasks = pendingTasks + processingTasks + processedTasks, // FIXED: Don't include collected
                    FailedTasks = failedTasks,
                    jobExecution.TotalRows,
                    ProcessedRows = processedRows,
                    ProgressPercentage = totalTasks > 0 ? (int)(finishedTasks * 100.0 / totalTasks) : 0
                });

                _logger.LogDebug($"Triggered job status update for {jobId} - Status: {newStatus}");
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, $"Failed to trigger job status update for {jobId}");
            }
        }


        public class TaskStatusUpdate
        {
            public string TaskId { get; set; } = string.Empty;
            public string Status { get; set; } = string.Empty;
            public string? WorkerId { get; set; }
            public int RowCount { get; set; }
            public long DurationMs { get; set; } // NEW
            public string? ErrorMessage { get; set; } // NEW
        }
    }
}
