using System.Text.Json;
using DistributedProcessor.Data;
using DistributedProcessor.Data.Models;
using Microsoft.EntityFrameworkCore;

namespace DistributedProcessor.API.Services
{
    public interface IOutboxService
    {
        Task AddMessageAsync<T>(string topic, string? messageKey, T message, string messageType);
        Task<List<OutboxMessage>> GetPendingMessagesAsync(int batchSize = 100);
        Task MarkAsSentAsync(long id);
        Task MarkAsFailedAsync(long id, string errorMessage);
    }

    public class OutboxService : IOutboxService
    {
        private readonly ApplicationDbContext _context;
        private readonly ILogger<OutboxService> _logger;

        public OutboxService(
            ApplicationDbContext context,
            ILogger<OutboxService> logger)
        {
            _context = context;
            _logger = logger;
        }

        public async Task AddMessageAsync<T>(
            string topic,
            string? messageKey,
            T message,
            string messageType)
        {
            var outboxMessage = new OutboxMessage
            {
                MessageId = Guid.NewGuid().ToString(),
                Topic = topic,
                MessageKey = messageKey,
                Payload = JsonSerializer.Serialize(message),
                MessageType = messageType,
                CreatedAt = DateTime.UtcNow,
                Status = "Pending"
            };

            _context.OutboxMessages.Add(outboxMessage);
            // Note: SaveChanges is called by the caller within their transaction

            _logger.LogDebug(
                "Added outbox message: Topic={Topic}, Type={Type}, MessageId={MessageId}",
                topic, messageType, outboxMessage.MessageId);
        }

        public async Task<List<OutboxMessage>> GetPendingMessagesAsync(int batchSize = 100)
        {
            return await _context.OutboxMessages
                .Where(m => m.Status == "Pending" ||
                           (m.Status == "Failed" && m.RetryCount < 3))
                .OrderBy(m => m.CreatedAt)
                .Take(batchSize)
                .ToListAsync();
        }

        public async Task MarkAsSentAsync(long id)
        {
            var message = await _context.OutboxMessages.FindAsync(id);
            if (message != null)
            {
                message.Status = "Sent";
                message.ProcessedAt = DateTime.UtcNow;
                await _context.SaveChangesAsync();
            }
        }

        public async Task MarkAsFailedAsync(long id, string errorMessage)
        {
            var message = await _context.OutboxMessages.FindAsync(id);
            if (message != null)
            {
                message.Status = "Failed";
                message.RetryCount++;
                message.ErrorMessage = errorMessage.Length > 2000
                    ? errorMessage[..2000]
                    : errorMessage;
                message.LastAttemptAt = DateTime.UtcNow;
                await _context.SaveChangesAsync();
            }
        }
    }
}
