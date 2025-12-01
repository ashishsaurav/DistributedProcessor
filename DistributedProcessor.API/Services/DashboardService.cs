using DistributedProcessor.Data;
using DistributedProcessor.Shared.Models;
using Microsoft.EntityFrameworkCore;

namespace DistributedProcessor.API.Services
{
    public interface IDashboardService
    {
        Task<DashboardState> GetDashboardStateAsync();
        Task<List<JobSummary>> GetJobSummariesAsync();
        Task<List<TaskSummary>> GetTaskSummariesAsync(string jobId = null);
        Task<CollectorStats> GetCollectorStatsAsync();
        Task<List<string>> GetAvailableFundsAsync();  // NEW
        Task<List<SymbolInfo>> GetAvailableSymbolsAsync(string fund = null);  // NEW
    }

    public class DashboardService : IDashboardService
    {
        private readonly ApplicationDbContext _context;
        private readonly IWorkerHealthService _workerHealthService;
        private readonly ILogger<DashboardService> _logger;

        public DashboardService(
            ApplicationDbContext context,
            IWorkerHealthService workerHealthService,
            ILogger<DashboardService> logger)
        {
            _context = context;
            _workerHealthService = workerHealthService;
            _logger = logger;
        }

        public async Task<DashboardState> GetDashboardStateAsync()
        {
            var workers = await _workerHealthService.GetAllWorkerStatusesAsync();
            var jobs = await GetJobSummariesAsync();
            var tasks = await GetTaskSummariesAsync();
            var collectorStats = await GetCollectorStatsAsync();

            var systemMetrics = new SystemMetrics
            {
                TotalWorkers = workers.Count,
                ActiveWorkers = workers.Count(w => w.State != "Offline"),
                BusyWorkers = workers.Count(w => w.State == "Busy"),
                TotalJobs = jobs.Count,
                ActiveJobs = jobs.Count(j => j.Status == "Processing" || j.Status == "Pending"),
                TotalTasksCompleted = await _context.TaskLogs.CountAsync(t => t.Status == "Completed")
            };

            return new DashboardState
            {
                Workers = workers,
                Jobs = jobs,
                Tasks = tasks.Take(50).ToList(), // Latest 50 tasks
                CollectorStats = collectorStats,
                SystemMetrics = systemMetrics
            };
        }

        public async Task<List<JobSummary>> GetJobSummariesAsync()
        {
            try
            {
                // Get jobs with real-time task aggregation from TaskLogs
                var jobsWithStats = await (from job in _context.JobExecutions
                                           let completedCount = _context.TaskLogs.Count(t => t.JobId == job.JobId && t.Status == "Completed")
                                           let failedCount = _context.TaskLogs.Count(t => t.JobId == job.JobId && t.Status == "Failed")
                                           let processingCount = _context.TaskLogs.Count(t => t.JobId == job.JobId && t.Status == "Processing")
                                           let pendingCount = _context.TaskLogs.Count(t => t.JobId == job.JobId && t.Status == "Pending")
                                           let processedRows = _context.TaskLogs
                                               .Where(t => t.JobId == job.JobId && t.Status == "Completed")
                                               .Sum(t => (int?)t.RowsProcessed) ?? 0
                                           select new
                                           {
                                               job.JobId,
                                               job.Fund,
                                               job.SubmittedAt,
                                               job.TotalTasks,
                                               job.TotalRows,
                                               CompletedTasks = completedCount,
                                               FailedTasks = failedCount,
                                               ProcessingTasks = processingCount,
                                               PendingTasks = pendingCount,
                                               ProcessedRows = processedRows
                                           })
                                            .OrderByDescending(j => j.SubmittedAt)
                                            .ToListAsync();

                return jobsWithStats.Select(j => new JobSummary
                {
                    JobId = j.JobId,
                    Fund = j.Fund,
                    SubmittedAt = j.SubmittedAt,
                    Status = DetermineJobStatus(j.CompletedTasks, j.FailedTasks, j.ProcessingTasks, j.PendingTasks, j.TotalTasks),
                    TotalTasks = j.TotalTasks,
                    CompletedTasks = j.CompletedTasks,
                    PendingTasks = j.PendingTasks + j.ProcessingTasks,
                    FailedTasks = j.FailedTasks,
                    TotalRows = j.TotalRows,
                    ProcessedRows = j.ProcessedRows
                }).ToList();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error getting job summaries");
                return new List<JobSummary>();
            }
        }

        private string DetermineJobStatus(int completed, int failed, int processing, int pending, int total)
        {
            if (total == 0) return "Pending";

            if (processing > 0) return "Processing";

            if (completed + failed >= total)
            {
                return failed > 0 ? "Completed with Errors" : "Completed";
            }

            if (pending > 0) return "Pending";

            return "Unknown";
        }

        public async Task<List<TaskSummary>> GetTaskSummariesAsync(string jobId = null)
        {
            var query = _context.TaskLogs.AsQueryable();

            if (!string.IsNullOrEmpty(jobId))
            {
                query = query.Where(t => t.JobId == jobId);
            }

            var tasks = await query
                .OrderByDescending(t => t.CreatedAt)
                .Take(100)
                .Select(t => new TaskSummary
                {
                    TaskId = t.TaskId,
                    JobId = t.JobId,
                    Fund = t.Fund ?? "",
                    Symbol = t.Symbol ?? "",
                    Status = t.Status,
                    WorkerId = t.WorkerId,
                    RowsProcessed = t.RowsProcessed ?? 0,
                    StartedAt = t.StartedAt,
                    CompletedAt = t.CompletedAt
                })
                .ToListAsync();

            return tasks;
        }


        public async Task<CollectorStats> GetCollectorStatsAsync()
        {
            var today = DateTime.UtcNow.Date;

            // Total results collected
            var totalCollected = await _context.CalculatedResults.CountAsync();

            // Results collected today
            var todayCollected = await _context.CalculatedResults
                .CountAsync(r => r.CreatedAt >= today);

            // Last collection time
            var lastCollection = await _context.CalculatedResults
                .OrderByDescending(r => r.CreatedAt)
                .Select(r => r.CreatedAt)
                .FirstOrDefaultAsync();

            // Consider collector running if last collection was within 5 minutes
            var isRunning = lastCollection != default &&
                            (DateTime.UtcNow - lastCollection).TotalMinutes < 5;

            return new CollectorStats
            {
                IsRunning = isRunning,
                TotalResultsCollected = totalCollected,
                ResultsCollectedToday = todayCollected,
                LastCollectionTime = lastCollection
            };
        }

        public async Task<List<string>> GetAvailableFundsAsync()
        {
            try
            {
                var funds = await _context.SourceData
                    .Select(sd => sd.Fund)
                    .Distinct()
                    .OrderBy(f => f)
                    .ToListAsync();

                _logger.LogInformation($"Retrieved {funds.Count} available funds");
                return funds;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error getting available funds");
                return new List<string>();
            }
        }

        public async Task<List<SymbolInfo>> GetAvailableSymbolsAsync(string fund = null)
        {
            try
            {
                var query = _context.SourceData.AsQueryable();

                if (!string.IsNullOrEmpty(fund))
                    query = query.Where(sd => sd.Fund == fund);

                var symbols = await query
                    .Select(sd => new SymbolInfo
                    {
                        Fund = sd.Fund,
                        Symbol = sd.Symbol
                    })
                    .Distinct()
                    .OrderBy(s => s.Fund)
                    .ThenBy(s => s.Symbol)
                    .ToListAsync();

                _logger.LogInformation($"Retrieved {symbols.Count} symbols for fund: {fund ?? "all"}");
                return symbols;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error getting available symbols");
                return new List<SymbolInfo>();
            }
        }
    }
}
