using System;
using System.Collections.Generic;

namespace DistributedProcessor.Shared.Models
{
    public class ProcessingTask
    {
        public string TaskId { get; set; } = string.Empty;
        public string JobId { get; set; } = string.Empty;
        public string Fund { get; set; } = string.Empty;
        public string Symbol { get; set; } = string.Empty;
        public DateTime StartDate { get; set; }
        public DateTime EndDate { get; set; }
        public List<SourceDataRow> Rows { get; set; } = new();
        public string? AssignedWorkerId { get; set; }
    }

    public class SourceDataRow
    {
        public int Id { get; set; }
        public string Fund { get; set; }
        public string Symbol { get; set; }
        public DateTime Date { get; set; }
        public decimal Value { get; set; }
    }

    public class ProcessingResult
    {
        public string TaskId { get; set; }
        public string JobId { get; set; }
        public string WorkerId { get; set; }
        public string Fund { get; set; }
        public string Symbol { get; set; }
        public List<CalculatedRow> CalculatedRows { get; set; } = new();
        public DateTime ProcessedAt { get; set; } = DateTime.UtcNow;
        public bool Success { get; set; }
        public string ErrorMessage { get; set; }
        public long ProcessingDurationMs { get; set; } // NEW: Track processing time
    }

    public class CalculatedRow
    {
        public DateTime Date { get; set; }
        public decimal Price { get; set; }
        public decimal Returns { get; set; }
        public decimal CumulativeReturns { get; set; }
    }

    public class SimpleJobRequest
    {
        public string Fund { get; set; }
        public DateTime StartDate { get; set; }
        public DateTime EndDate { get; set; }
    }

    public class SimpleJobResponse
    {
        public bool Success { get; set; }
        public string JobId { get; set; }
        public string Fund { get; set; }
        public int SymbolsToProcess { get; set; }
        public int TotalRows { get; set; }
        public DateTime? EstimatedCompletion { get; set; }
        public string Message { get; set; }
    }

    public class BatchJobRequest
    {
        public List<string> Funds { get; set; } = new();
        public DateTime StartDate { get; set; }
        public DateTime EndDate { get; set; }
    }
}
