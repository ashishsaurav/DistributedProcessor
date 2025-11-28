namespace DistributedProcessor.Data
{
    public class SourceData
    {
        public int Id { get; set; }
        public string Fund { get; set; } = string.Empty;
        public string Symbol { get; set; } = string.Empty;
        public DateTime Date { get; set; }
        public decimal OpenPrice { get; set; }
        public decimal ClosePrice { get; set; }
        public decimal High { get; set; }
        public decimal Low { get; set; }
        public long Volume { get; set; }
        public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    }
}
