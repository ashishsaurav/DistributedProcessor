using DistributedProcessor.Data;
using DistributedProcessor.Data.Models;
using Microsoft.EntityFrameworkCore;

namespace DistributedProcessor.API.Services
{
    public interface IIdempotencyService
    {
        Task<bool> CanProcessAsync(string taskId, string workerId);
        Task MarkCompletedAsync(string taskId);
        Task MarkFailedAsync(string taskId);
    }

    public class IdempotencyService : IIdempotencyService
    {
        private readonly ApplicationDbContext _context;
        private readonly ILogger<IdempotencyService> _logger;
        private static readonly TimeSpan Expiry = TimeSpan.FromHours(24);

        public IdempotencyService(
            ApplicationDbContext context,
            ILogger<IdempotencyService> logger)
        {
            _context = context;
            _logger = logger;
        }

        public async Task<bool> CanProcessAsync(string taskId, string workerId)
        {
            try
            {
                // Check if already exists
                var existing = await _context.IdempotencyKeys
                    .FirstOrDefaultAsync(k => k.TaskId == taskId);

                if (existing != null)
                {
                    // Check if expired
                    if (existing.ExpiresAt < DateTime.UtcNow)
                    {
                        _context.IdempotencyKeys.Remove(existing);
                        await _context.SaveChangesAsync();
                        _logger.LogInformation("Expired idempotency key removed: {TaskId}", taskId);
                    }
                    else if (existing.Status == "Completed")
                    {
                        _logger.LogInformation(
                            "Task {TaskId} already completed by {WorkerId}. Skipping.",
                            taskId, existing.WorkerId);
                        return false; // Already processed
                    }
                    else if (existing.Status == "Processing")
                    {
                        var age = DateTime.UtcNow - existing.CreatedAt;
                        if (age > TimeSpan.FromMinutes(10))
                        {
                            // Stuck task - allow retry
                            _logger.LogWarning(
                                "Task {TaskId} stuck for {Minutes}min. Allowing retry.",
                                taskId, age.TotalMinutes);

                            existing.WorkerId = workerId;
                            existing.CreatedAt = DateTime.UtcNow;
                            await _context.SaveChangesAsync();
                            return true;
                        }

                        _logger.LogInformation(
                            "Task {TaskId} currently processing by {WorkerId}. Skipping.",
                            taskId, existing.WorkerId);
                        return false; // Currently being processed
                    }
                }

                // Create new record
                var newKey = new IdempotencyKey
                {
                    TaskId = taskId,
                    Status = "Processing",
                    WorkerId = workerId,
                    CreatedAt = DateTime.UtcNow,
                    ExpiresAt = DateTime.UtcNow.Add(Expiry)
                };

                _context.IdempotencyKeys.Add(newKey);
                await _context.SaveChangesAsync();

                _logger.LogDebug("Idempotency key created: {TaskId} by {WorkerId}", taskId, workerId);
                return true; // Can process
            }
            catch (DbUpdateException ex) when (ex.InnerException?.Message.Contains("duplicate") == true)
            {
                // Race condition - another worker got it
                _logger.LogWarning(
                    "Race condition for task {TaskId}. Another worker started processing.",
                    taskId);
                return false;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error checking idempotency for {TaskId}", taskId);
                return true; // Fail open - allow processing
            }
        }

        public async Task MarkCompletedAsync(string taskId)
        {
            try
            {
                var key = await _context.IdempotencyKeys
                    .FirstOrDefaultAsync(k => k.TaskId == taskId);

                if (key != null)
                {
                    key.Status = "Completed";
                    key.CompletedAt = DateTime.UtcNow;
                    await _context.SaveChangesAsync();
                    _logger.LogDebug("Task {TaskId} marked as completed", taskId);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error marking task {TaskId} as completed", taskId);
            }
        }

        public async Task MarkFailedAsync(string taskId)
        {
            try
            {
                var key = await _context.IdempotencyKeys
                    .FirstOrDefaultAsync(k => k.TaskId == taskId);

                if (key != null)
                {
                    key.Status = "Failed";
                    key.CompletedAt = DateTime.UtcNow;
                    await _context.SaveChangesAsync();
                    _logger.LogDebug("Task {TaskId} marked as failed", taskId);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error marking task {TaskId} as failed", taskId);
            }
        }
    }
}
