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

                    // NOTE: We DON'T update to "Completed" here anymore
                    // CollectorService will update to "Collected" status
                    // A separate process can mark as "Completed" after verification
                }
                else if (!result.Success)
                {
                    _logger.LogWarning($"Task {result.TaskId} failed: {result.ErrorMessage}");
                    // Failed tasks are already marked as "Failed" by worker
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error saving calculated results for task {result.TaskId}");
                throw;
            }
        }
    }
}
