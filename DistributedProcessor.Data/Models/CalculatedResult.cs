namespace DistributedProcessor.Data.Models
{
    public class CalculatedResult
    {
        public int Id { get; set; }
        public string TaskId { get; set; } = string.Empty;
        public string JobId { get; set; } = string.Empty;
        public string Fund { get; set; } = string.Empty;
        public string Symbol { get; set; } = string.Empty;
        public DateTime Date { get; set; }
        public decimal Price { get; set; }
        public decimal Returns { get; set; }
        public decimal CumulativeReturns { get; set; }
        public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    }
}
