namespace DistributedProcessor.Shared.Models
{
    public class DashboardState
    {
        public List<WorkerStatus> Workers { get; set; } = new();
        public List<CollectorStatus> Collectors { get; set; } = new(); // NEW
        public List<JobSummary> Jobs { get; set; } = new();
        public List<TaskSummary> Tasks { get; set; } = new();
        public CollectorStats CollectorStats { get; set; } = new();
        public SystemMetrics SystemMetrics { get; set; } = new();
    }

    public class JobSummary
    {
        public string JobId { get; set; }
        public string Fund { get; set; }
        public DateTime SubmittedAt { get; set; }
        public string Status { get; set; }
        public int TotalTasks { get; set; }
        public int CompletedTasks { get; set; }
        public int PendingTasks { get; set; }
        public int FailedTasks { get; set; }
        public int TotalRows { get; set; }
        public int ProcessedRows { get; set; }
        public int ProgressPercentage { get; set; }
    }

    public class TaskSummary
    {
        public string TaskId { get; set; } = string.Empty;
        public string JobId { get; set; } = string.Empty;
        public string Fund { get; set; } = string.Empty;
        public string Symbol { get; set; } = string.Empty;
        public string Status { get; set; } = string.Empty;
        public string? WorkerId { get; set; }
        public int RowsProcessed { get; set; }
        public DateTime? StartedAt { get; set; }
        public DateTime? ProcessedAt { get; set; }
        public DateTime? CollectedAt { get; set; }
        public DateTime? CompletedAt { get; set; }
        public long? ProcessingDurationMs { get; set; }
        public long? CollectionDurationMs { get; set; }
    }

    public class CollectorStats
    {
        public bool IsRunning { get; set; }
        public int TotalResultsCollected { get; set; }
        public int ResultsCollectedToday { get; set; }
        public DateTime LastCollectionTime { get; set; }
    }

    public class SystemMetrics
    {
        public int TotalWorkers { get; set; }
        public int ActiveWorkers { get; set; }
        public int BusyWorkers { get; set; }
        public int TotalCollectors { get; set; } // NEW
        public int ActiveCollectors { get; set; } // NEW
        public int TotalJobs { get; set; }
        public int ActiveJobs { get; set; }
        public int TotalTasksCompleted { get; set; }
        public int TotalTasksProcessing { get; set; }
    }

    public class SymbolInfo
    {
        public string Fund { get; set; }
        public string Symbol { get; set; }
    }
}
