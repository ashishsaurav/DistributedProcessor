using System;

namespace DistributedProcessor.Shared.Models
{
    public class UserJobRequest
    {
        public string JobId { get; set; } = Guid.NewGuid().ToString();
        public string Fund { get; set; }
        public string Symbol { get; set; }
        public DateTime StartDate { get; set; }
        public DateTime EndDate { get; set; }
    }
}
