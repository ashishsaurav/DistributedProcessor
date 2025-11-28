using System;
using System.Collections.Generic;

namespace DistributedProcessor.Shared.Models
{

    public class JobStatusResponse
    {
        public string JobId { get; set; }
        public string Fund { get; set; }
        public string Symbol { get; set; }
        public string Status { get; set; }
        public int TotalTasks { get; set; }
        public int CompletedTasks { get; set; }
        public int ProgressPercentage { get; set; }
    }
}
