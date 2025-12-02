namespace DistributedProcessor.Data.Models
{
    public class DeadLetterMessage
    {
        public int Id { get; set; }
        public string MessageId { get; set; } = Guid.NewGuid().ToString();
        public string OriginalTopic { get; set; } = string.Empty;
        public string Payload { get; set; } = string.Empty;
        public string ErrorMessage { get; set; } = string.Empty;
        public string ExceptionType { get; set; } = string.Empty;
        public int RetryCount { get; set; }
        public string? WorkerId { get; set; }
        public string? JobId { get; set; }
        public string? TaskId { get; set; }
        public DateTime FailedAt { get; set; } = DateTime.UtcNow;
        public int Status { get; set; } = 0; // 0=Pending, 2=Reprocessed
    }
}
