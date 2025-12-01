namespace DistributedProcessor.Data.Models
{
    public class TaskLog
    {
        public int Id { get; set; }
        public string TaskId { get; set; } = string.Empty;
        public string JobId { get; set; } = string.Empty;
        public string? Fund { get; set; }
        public string? Symbol { get; set; }
        public string Status { get; set; } = "Pending"; // Pending, Processing, Processed, Collected, Completed, Failed
        public string? WorkerId { get; set; }
        public int? RowsProcessed { get; set; }
        public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
        public DateTime? StartedAt { get; set; }
        public DateTime? ProcessedAt { get; set; } // NEW: When worker finished processing
        public DateTime? CollectedAt { get; set; } // NEW: When collector saved to DB
        public DateTime? CompletedAt { get; set; }
        public string? ErrorMessage { get; set; }
        public long? ProcessingDurationMs { get; set; } // NEW: Time taken by worker
        public long? CollectionDurationMs { get; set; } // NEW: Time taken by collector

        // Navigation property to parent JobExecution
        public virtual JobExecution? JobExecution { get; set; }
    }
}
