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
}
