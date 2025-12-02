using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DistributedProcessor.Data.Models
{
    public class OutboxMessage
    {
        public long Id { get; set; }
        public string MessageId { get; set; } = Guid.NewGuid().ToString();
        public string Topic { get; set; } = string.Empty;
        public string? MessageKey { get; set; }
        public string Payload { get; set; } = string.Empty;
        public string MessageType { get; set; } = string.Empty;
        public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
        public DateTime? ProcessedAt { get; set; }
        public string Status { get; set; } = "Pending"; // Pending, Sent, Failed
        public int RetryCount { get; set; } = 0;
        public string? ErrorMessage { get; set; }
        public DateTime? LastAttemptAt { get; set; }
    }
}
