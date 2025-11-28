namespace DistributedProcessor.Data.Models
{
    public class JobExecution
    {
        public int Id { get; set; }
        public string JobId { get; set; } = string.Empty;
        public string Fund { get; set; } = string.Empty;
        public string Status { get; set; } = "Pending";
        public int TotalTasks { get; set; }
        public int CompletedTasks { get; set; }
        public int FailedTasks { get; set; }
        public int TotalRows { get; set; }
        public int ProcessedRows { get; set; }
        public DateTime SubmittedAt { get; set; } = DateTime.UtcNow;
        public DateTime? StartedAt { get; set; }
        public DateTime? CompletedAt { get; set; }
        public string? CreatedBy { get; set; }
        public string? ErrorMessage { get; set; }

        // Navigation property to child TaskLogs
        public virtual ICollection<TaskLog> TaskLogs { get; set; } = new List<TaskLog>();
    }
}
