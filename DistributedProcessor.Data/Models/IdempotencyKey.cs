namespace DistributedProcessor.Data.Models
{
    public class IdempotencyKey
    {
        public int Id { get; set; }
        public string TaskId { get; set; } = string.Empty;
        public string Status { get; set; } = string.Empty; // Processing, Completed, Failed
        public string? WorkerId { get; set; }
        public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
        public DateTime? CompletedAt { get; set; }
        public DateTime ExpiresAt { get; set; }
    }
}
