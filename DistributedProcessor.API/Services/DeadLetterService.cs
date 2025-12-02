using System.Text.Json;
using DistributedProcessor.Data;
using DistributedProcessor.Data.Models;
using Microsoft.EntityFrameworkCore;

namespace DistributedProcessor.API.Services
{
    public interface IDeadLetterService
    {
        Task SendToDeadLetterAsync(object message, string topic, Exception ex,
            int retryCount, string? workerId, string? jobId, string? taskId);
        Task<List<DeadLetterMessage>> GetPendingAsync();
    }

    public class DeadLetterService : IDeadLetterService
    {
        private readonly ApplicationDbContext _context;
        private readonly ILogger<DeadLetterService> _logger;

        public DeadLetterService(ApplicationDbContext context, ILogger<DeadLetterService> logger)
        {
            _context = context;
            _logger = logger;
        }

        public async Task SendToDeadLetterAsync(
            object message,
            string topic,
            Exception ex,
            int retryCount,
            string? workerId,
            string? jobId,
            string? taskId)
        {
            var dlqMessage = new DeadLetterMessage
            {
                MessageId = Guid.NewGuid().ToString(),
                OriginalTopic = topic,
                Payload = JsonSerializer.Serialize(message),
                ErrorMessage = ex.Message.Length > 2000 ? ex.Message[..2000] : ex.Message,
                ExceptionType = ex.GetType().Name,
                RetryCount = retryCount,
                WorkerId = workerId,
                JobId = jobId,
                TaskId = taskId,
                FailedAt = DateTime.UtcNow,
                Status = 0
            };

            _context.DeadLetterMessages.Add(dlqMessage);
            await _context.SaveChangesAsync();

            _logger.LogWarning(
                "DLQ: TaskId={TaskId}, Error={Error}, Retries={Retries}",
                taskId, ex.Message, retryCount);
        }

        public async Task<List<DeadLetterMessage>> GetPendingAsync()
        {
            return await _context.DeadLetterMessages
                .Where(m => m.Status == 0)
                .OrderByDescending(m => m.FailedAt)
                .Take(100)
                .ToListAsync();
        }
    }
}
