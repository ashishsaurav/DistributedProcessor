using DistributedProcessor.Data;
using DistributedProcessor.Data.Models;
using DistributedProcessor.Data.Services;
using DistributedProcessor.Shared.Models;
using Microsoft.EntityFrameworkCore;

namespace DistributedProcessor.API.Services
{
    public interface ISimpleJobOrchestrator
    {
        Task<SimpleJobResponse> SubmitJobAsync(SimpleJobRequest request);
        Task<SimpleJobResponse> SubmitBatchJobAsync(BatchJobRequest request);
    }

    public class SimpleJobOrchestrator : ISimpleJobOrchestrator
    {
        private readonly ITaskOrchestrator _taskOrchestrator;
        private readonly IDbService _dbService;
        private readonly ApplicationDbContext _context;
        private readonly ILogger<SimpleJobOrchestrator> _logger;

        public SimpleJobOrchestrator(
            ITaskOrchestrator taskOrchestrator,
            IDbService dbService,
            ApplicationDbContext context,
            ILogger<SimpleJobOrchestrator> logger)
        {
            _taskOrchestrator = taskOrchestrator;
            _dbService = dbService;
            _context = context;
            _logger = logger;
        }

        public async Task<SimpleJobResponse> SubmitJobAsync(SimpleJobRequest request)
        {
            try
            {
                _logger.LogInformation($"Submitting job for Fund: {request.Fund}");

                var jobId = Guid.NewGuid().ToString();

                // Get all distinct symbols for the fund
                var sourceData = await _dbService.GetSourceDataAsync(request.Fund, request.StartDate, request.EndDate);
                var symbols = sourceData.Select(sd => sd.Symbol).Distinct().ToList();

                if (!symbols.Any())
                {
                    _logger.LogWarning($"No data found for Fund: {request.Fund}");
                    return new SimpleJobResponse
                    {
                        Success = false,
                        Message = $"No data found for fund {request.Fund}",
                        JobId = string.Empty,
                        Fund = request.Fund
                    };
                }

                _logger.LogInformation($"Found {symbols.Count} symbols for fund {request.Fund}");

                // CREATE JOB EXECUTION RECORD
                await CreateJobExecutionAsync(jobId, request.Fund);

                // Create tasks for each symbol
                int totalTasksCreated = 0;
                int totalRowsCount = 0;

                foreach (var symbol in symbols)
                {
                    var taskRequest = new UserJobRequest
                    {
                        JobId = jobId,
                        Fund = request.Fund,
                        Symbol = symbol,
                        StartDate = request.StartDate,
                        EndDate = request.EndDate
                    };

                    var (success, tasksCreated, rowsCount) = await _taskOrchestrator.CreateAndDistributeTasksAsync(taskRequest);

                    if (success)
                    {
                        totalTasksCreated += tasksCreated;
                        totalRowsCount += rowsCount;
                        _logger.LogInformation($"Created {tasksCreated} tasks for {request.Fund}/{symbol}");
                    }
                }

                // Update JobExecution with totals
                await UpdateJobExecutionTotalsAsync(jobId, totalTasksCreated, totalRowsCount);

                // Calculate estimated completion (rough estimate: 1000 rows per second)
                var estimatedSeconds = totalRowsCount / 1000.0;
                var estimatedCompletion = DateTime.UtcNow.AddSeconds(estimatedSeconds);

                _logger.LogInformation($"Job {jobId} submitted with {totalTasksCreated} tasks ({totalRowsCount} rows) for fund {request.Fund}");

                return new SimpleJobResponse
                {
                    Success = true,
                    Message = $"Job submitted successfully for fund {request.Fund}",
                    JobId = jobId,
                    Fund = request.Fund,
                    SymbolsToProcess = symbols.Count,
                    TotalRows = totalRowsCount,
                    EstimatedCompletion = estimatedCompletion
                };
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error submitting job for fund {request.Fund}");
                return new SimpleJobResponse
                {
                    Success = false,
                    Message = $"Error: {ex.Message}",
                    JobId = string.Empty,
                    Fund = request.Fund
                };
            }
        }

        public async Task<SimpleJobResponse> SubmitBatchJobAsync(BatchJobRequest request)
        {
            try
            {
                var jobId = Guid.NewGuid().ToString();
                var allFunds = string.Join(", ", request.Funds);

                _logger.LogInformation($"Submitting batch job for Funds: {allFunds}");

                // CREATE SINGLE JOB EXECUTION RECORD FOR ALL FUNDS
                await CreateJobExecutionAsync(jobId, allFunds);

                int totalTasksCreated = 0;
                int totalRowsCount = 0;
                int totalSymbols = 0;

                // Process each fund
                foreach (var fund in request.Funds)
                {
                    var sourceData = await _dbService.GetSourceDataAsync(fund, request.StartDate, request.EndDate);
                    var symbols = sourceData.Select(sd => sd.Symbol).Distinct().ToList();

                    if (!symbols.Any())
                    {
                        _logger.LogWarning($"No data found for Fund: {fund}");
                        continue;
                    }

                    _logger.LogInformation($"Found {symbols.Count} symbols for fund {fund}");
                    totalSymbols += symbols.Count;

                    // Create tasks for each symbol
                    foreach (var symbol in symbols)
                    {
                        var taskRequest = new UserJobRequest
                        {
                            JobId = jobId,
                            Fund = fund,
                            Symbol = symbol,
                            StartDate = request.StartDate,
                            EndDate = request.EndDate
                        };

                        var (success, tasksCreated, rowsCount) = await _taskOrchestrator.CreateAndDistributeTasksAsync(taskRequest);

                        if (success)
                        {
                            totalTasksCreated += tasksCreated;
                            totalRowsCount += rowsCount;
                            _logger.LogInformation($"Created {tasksCreated} tasks for {fund}/{symbol}");
                        }
                    }
                }

                // Update JobExecution with totals
                await UpdateJobExecutionTotalsAsync(jobId, totalTasksCreated, totalRowsCount);

                // Calculate estimated completion
                var estimatedSeconds = totalRowsCount / 1000.0;
                var estimatedCompletion = DateTime.UtcNow.AddSeconds(estimatedSeconds);

                _logger.LogInformation($"Batch job {jobId} submitted with {totalTasksCreated} tasks ({totalRowsCount} rows) for funds: {allFunds}");

                return new SimpleJobResponse
                {
                    Success = true,
                    Message = $"Batch job submitted successfully for funds: {allFunds}",
                    JobId = jobId,
                    Fund = allFunds, 
                    SymbolsToProcess = totalSymbols,
                    TotalRows = totalRowsCount,
                    EstimatedCompletion = estimatedCompletion
                };
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error submitting batch job");
                return new SimpleJobResponse
                {
                    Success = false,
                    Message = $"Error: {ex.Message}",
                    JobId = string.Empty,
                    Fund = string.Join(", ", request.Funds)
                };
            }
        }

        private async Task CreateJobExecutionAsync(string jobId, string fund)
        {
            try
            {
                var jobExecution = new JobExecution
                {
                    JobId = jobId,
                    Fund = fund,
                    Status = "Pending",
                    TotalTasks = 0,
                    CompletedTasks = 0,
                    FailedTasks = 0,
                    TotalRows = 0,
                    ProcessedRows = 0,
                    SubmittedAt = DateTime.UtcNow
                };

                _context.JobExecutions.Add(jobExecution);
                await _context.SaveChangesAsync();

                _logger.LogInformation($"Created JobExecution: JobId={jobId}, Fund={fund}");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error creating JobExecution for JobId: {jobId}");
                throw;
            }
        }

        private async Task UpdateJobExecutionTotalsAsync(string jobId, int totalTasks, int totalRows)
        {
            try
            {
                var jobExecution = await _context.JobExecutions
                    .FirstOrDefaultAsync(j => j.JobId == jobId);

                if (jobExecution != null)
                {
                    jobExecution.TotalTasks = totalTasks;
                    jobExecution.TotalRows = totalRows;
                    jobExecution.Status = "Processing";
                    jobExecution.StartedAt = DateTime.UtcNow;

                    await _context.SaveChangesAsync();

                    _logger.LogInformation($"Updated JobExecution: JobId={jobId}, TotalTasks={totalTasks}, TotalRows={totalRows}");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error updating JobExecution totals for JobId: {jobId}");
            }
        }
    }
}
