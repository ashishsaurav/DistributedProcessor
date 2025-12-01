using System;

namespace DistributedProcessor.Shared.Models
{
    public class WorkerStatus
    {
        public string WorkerId { get; set; }
        public string State { get; set; } // Idle, Busy, Offline
        public DateTime LastHeartbeat { get; set; }
        public int ActiveTasks { get; set; }
        public double CpuUsage { get; set; }
        public double MemoryUsageMB { get; set; }
        public int TotalProcessed { get; set; }
    }

    public class CollectorStatus
    {
        public string CollectorId { get; set; } = string.Empty;
        public string State { get; set; } = "Idle"; // Idle, Collecting, Offline
        public int ActiveCollections { get; set; }
        public double CpuUsage { get; set; }
        public double MemoryUsageMB { get; set; }
        public DateTime LastHeartbeat { get; set; }
        public int TotalCollected { get; set; }
        public long AvgCollectionTimeMs { get; set; }
    }
}
