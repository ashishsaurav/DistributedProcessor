using DistributedProcessor.Data;
using DistributedProcessor.Data.Models;
using DistributedProcessor.Shared.Models;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DistributedProcessor.Data.Services
{
    public interface IDbService
    {
        Task<List<SourceData>> GetSourceDataAsync(string fund, DateTime startDate, DateTime endDate);
        Task<List<SourceData>> FetchRowsAsync(string fund, string symbol, DateTime startDate, DateTime endDate);
        Task SaveCalculatedResultsAsync(ProcessingResult result);
    }

    public class DbService : IDbService
    {
        private readonly ApplicationDbContext _context;
        private readonly ILogger<DbService> _logger;

        public DbService(ApplicationDbContext context, ILogger<DbService> logger)
        {
            _context = context;
            _logger = logger;
        }

        public async Task<List<SourceData>> GetSourceDataAsync(string fund, DateTime startDate, DateTime endDate)
        {
            return await _context.SourceData
                .Where(sd => sd.Fund == fund && sd.Date >= startDate && sd.Date <= endDate)
                .OrderBy(sd => sd.Date)
                .ToListAsync();
        }

        public async Task<List<SourceData>> FetchRowsAsync(string fund, string symbol, DateTime startDate, DateTime endDate)
        {
            return await _context.SourceData
                .Where(sd => sd.Fund == fund
                    && sd.Symbol == symbol
                    && sd.Date >= startDate
                    && sd.Date <= endDate)
                .OrderBy(sd => sd.Date)
                .ToListAsync();
        }

        public async Task SaveCalculatedResultsAsync(ProcessingResult result)
        {
            try
            {
                _logger.LogInformation($"Processing result for TaskId: {result.TaskId}, Success: {result.Success}");

                // Save CalculatedResults
                if (result.Success && result.CalculatedRows != null && result.CalculatedRows.Any())
                {
                    var calculatedResults = result.CalculatedRows.Select(r => new CalculatedResult
                    {
                        TaskId = result.TaskId,
                        JobId = result.JobId,
                        Fund = result.Fund ?? "Unknown",
                        Symbol = result.Symbol ?? "Unknown",
                        Date = r.Date,
                        Price = r.Price,
                        Returns = r.Returns,
                        CumulativeReturns = r.CumulativeReturns,
                        CreatedAt = DateTime.UtcNow
                    }).ToList();

                    _logger.LogInformation($"Adding {calculatedResults.Count} calculated results");
                    await _context.CalculatedResults.AddRangeAsync(calculatedResults);
                    await _context.SaveChangesAsync();

                    _logger.LogInformation($"Saved {calculatedResults.Count} results successfully");

                    // Update TaskLog
                    await UpdateTaskLogCompletedAsync(result.TaskId, result.WorkerId, calculatedResults.Count, result.Fund, result.Symbol);

                    // Update JobExecution
                    await UpdateJobExecutionProgressAsync(result.JobId);
                }
                else if (!result.Success)
                {
                    _logger.LogWarning($"Task {result.TaskId} failed: {result.ErrorMessage}");
                    await UpdateTaskLogFailedAsync(result.TaskId, result.WorkerId, result.ErrorMessage);
                    await UpdateJobExecutionProgressAsync(result.JobId);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error saving calculated results for task {result.TaskId}");
                throw;
            }
        }

        private async Task UpdateTaskLogCompletedAsync(string taskId, string workerId, int rowsProcessed, string fund, string symbol)
        {
            try
            {
                var sql = @"
                    UPDATE TaskLogs 
                    SET Status = 'Completed', 
                        CompletedAt = GETUTCDATE(), 
                        RowsProcessed = {0},
                        WorkerId = {1},
                        Fund = COALESCE(Fund, {2}),
                        Symbol = COALESCE(Symbol, {3})
                    WHERE TaskId = {4}";

                var rowsAffected = await _context.Database.ExecuteSqlRawAsync(
                    sql,
                    rowsProcessed,
                    workerId ?? "Unknown",
                    fund ?? "Unknown",
                    symbol ?? "Unknown",
                    taskId);

                _logger.LogInformation($"Updated TaskLog: {rowsAffected} row(s) for TaskId: {taskId}");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error updating completed task log for {taskId}");
            }
        }

        private async Task UpdateTaskLogFailedAsync(string taskId, string workerId, string errorMessage)
        {
            try
            {
                var sql = @"
                    UPDATE TaskLogs 
                    SET Status = 'Failed', 
                        CompletedAt = GETUTCDATE(), 
                        ErrorMessage = {0},
                        WorkerId = {1}
                    WHERE TaskId = {2}";

                var rowsAffected = await _context.Database.ExecuteSqlRawAsync(
                    sql,
                    errorMessage ?? "Unknown error",
                    workerId ?? "Unknown",
                    taskId);

                _logger.LogInformation($"Updated TaskLog: {rowsAffected} row(s) for failed TaskId: {taskId}");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error updating failed task log for {taskId}");
            }
        }

        private async Task UpdateJobExecutionProgressAsync(string jobId)
        {
            try
            {
                var sql = @"
                    UPDATE JobExecutions
                    SET CompletedTasks = (SELECT COUNT(*) FROM TaskLogs WHERE JobId = {0} AND Status = 'Completed'),
                        FailedTasks = (SELECT COUNT(*) FROM TaskLogs WHERE JobId = {0} AND Status = 'Failed'),
                        ProcessedRows = (SELECT ISNULL(SUM(RowsProcessed), 0) FROM TaskLogs WHERE JobId = {0} AND Status = 'Completed'),
                        Status = CASE 
                            WHEN (SELECT COUNT(*) FROM TaskLogs WHERE JobId = {0} AND Status IN ('Pending', 'Processing')) = 0 
                            THEN 'Completed'
                            ELSE 'Processing'
                        END,
                        CompletedAt = CASE 
                            WHEN (SELECT COUNT(*) FROM TaskLogs WHERE JobId = {0} AND Status IN ('Pending', 'Processing')) = 0 
                            THEN GETUTCDATE()
                            ELSE CompletedAt
                        END
                    WHERE JobId = {0}";

                var rowsAffected = await _context.Database.ExecuteSqlRawAsync(sql, jobId);

                _logger.LogInformation($"Updated JobExecution: {rowsAffected} row(s) for JobId: {jobId}");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error updating JobExecution progress for {jobId}");
            }
        }
    }
}
