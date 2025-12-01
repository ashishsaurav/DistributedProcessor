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
        Task<List<string>> GetAvailableFundsAsync();
        Task<List<SymbolInfo>> GetAvailableSymbolsAsync(string fund = null);
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
                TotalTasksCompleted = await _context.TaskLogs.CountAsync(t => t.Status == "Completed" || t.Status == "Collected"),
                TotalTasksProcessing = await _context.TaskLogs.CountAsync(t => t.Status == "Processing" || t.Status == "Processed")
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
                var jobsWithStats = await (from job in _context.JobExecutions
                                           let completedCount = _context.TaskLogs.Count(t => t.JobId == job.JobId && t.Status == "Completed")
                                           let collectedCount = _context.TaskLogs.Count(t => t.JobId == job.JobId && t.Status == "Collected")
                                           let failedCount = _context.TaskLogs.Count(t => t.JobId == job.JobId && t.Status == "Failed")
                                           let processingCount = _context.TaskLogs.Count(t => t.JobId == job.JobId && t.Status == "Processing")
                                           let processedCount = _context.TaskLogs.Count(t => t.JobId == job.JobId && t.Status == "Processed")
                                           let pendingCount = _context.TaskLogs.Count(t => t.JobId == job.JobId && t.Status == "Pending")
                                           let processedRows = _context.TaskLogs
                                               .Where(t => t.JobId == job.JobId && (t.Status == "Completed" || t.Status == "Collected" || t.Status == "Processed"))
                                               .Sum(t => (int?)t.RowsProcessed) ?? 0
                                           select new
                                           {
                                               job.JobId,
                                               job.Fund,
                                               job.SubmittedAt,
                                               job.TotalTasks,
                                               job.TotalRows,
                                               CompletedTasks = completedCount,
                                               CollectedTasks = collectedCount,
                                               FailedTasks = failedCount,
                                               ProcessingTasks = processingCount,
                                               ProcessedTasks = processedCount,
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
                    Status = DetermineJobStatus(
                        j.CompletedTasks,
                        j.FailedTasks,
                        j.ProcessingTasks,
                        j.ProcessedTasks,
                        j.CollectedTasks,
                        j.PendingTasks,
                        j.TotalTasks),
                    TotalTasks = j.TotalTasks,
                    CompletedTasks = j.CompletedTasks + j.CollectedTasks, // FIXED: Include Collected as completed
                    PendingTasks = j.PendingTasks + j.ProcessingTasks + j.ProcessedTasks, // FIXED: Don't include Collected
                    FailedTasks = j.FailedTasks,
                    TotalRows = j.TotalRows,
                    ProcessedRows = j.ProcessedRows,
                    ProgressPercentage = j.TotalTasks > 0
                        ? (int)((j.CompletedTasks + j.CollectedTasks + j.FailedTasks) * 100.0 / j.TotalTasks)
                        : 0
                }).ToList();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error getting job summaries");
                return new List<JobSummary>();
            }
        }

        private string DetermineJobStatus(int completed, int failed, int processing, int processed, int collected, int pending, int total)
        {
            if (total == 0) return "Pending";

            // Count tasks that are truly finished (Collected is the final working state)
            int finishedTasks = completed + collected + failed;

            // All tasks are done
            if (finishedTasks >= total)
            {
                return failed > 0 ? "Completed with Errors" : "Completed";
            }

            // Some tasks are actively being worked on
            if (processing > 0 || processed > 0)
            {
                return "Processing";
            }

            // Tasks waiting in collector after being processed
            if (collected > 0)
            {
                return "Processing";
            }

            // All tasks still pending
            if (pending > 0)
            {
                return "Pending";
            }

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
                    ProcessedAt = t.ProcessedAt,
                    CollectedAt = t.CollectedAt,
                    CompletedAt = t.CompletedAt,
                    ProcessingDurationMs = t.ProcessingDurationMs,
                    CollectionDurationMs = t.CollectionDurationMs
                })
                .ToListAsync();

            return tasks;
        }

        public async Task<CollectorStats> GetCollectorStatsAsync()
        {
            var today = DateTime.UtcNow.Date;

            var totalCollected = await _context.CalculatedResults.CountAsync();
            var todayCollected = await _context.CalculatedResults
                .CountAsync(r => r.CreatedAt >= today);

            var lastCollection = await _context.CalculatedResults
                .OrderByDescending(r => r.CreatedAt)
                .Select(r => r.CreatedAt)
                .FirstOrDefaultAsync();

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
