using DistributedProcessor.Data;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace DistributedProcessor.API.Services
{
    public class JobStatusUpdateService : BackgroundService
    {
        private readonly IServiceProvider _serviceProvider;
        private readonly ILogger<JobStatusUpdateService> _logger;

        public JobStatusUpdateService(
            IServiceProvider serviceProvider,
            ILogger<JobStatusUpdateService> logger)
        {
            _serviceProvider = serviceProvider;
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("Job Status Update Service started - Updating every 3 seconds");

            // Wait a bit before starting
            await Task.Delay(2000, stoppingToken);

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    using var scope = _serviceProvider.CreateScope();
                    var context = scope.ServiceProvider.GetRequiredService<ApplicationDbContext>();

                    var activeJobs = await context.JobExecutions
                        .Where(j => j.Status == "Pending" || j.Status == "Processing")
                        .ToListAsync(stoppingToken);

                    foreach (var job in activeJobs)
                    {
                        // Get all tasks for this job
                        var tasks = await context.TaskLogs
                            .Where(t => t.JobId == job.JobId)
                            .ToListAsync(stoppingToken);

                        if (tasks.Count == 0)
                        {
                            continue;
                        }

                        var totalTasks = tasks.Count;
                        var completedTasks = tasks.Count(t => t.Status == "Completed");
                        var failedTasks = tasks.Count(t => t.Status == "Failed");
                        var processingTasks = tasks.Count(t => t.Status == "Processing");
                        var pendingTasks = tasks.Count(t => t.Status == "Pending");

                        // Calculate processed rows
                        var processedRows = tasks.Sum(t => t.RowsProcessed ?? 0);

                        // Determine job status based on task states
                        string newStatus = job.Status;
                        DateTime? startedAt = job.StartedAt;
                        DateTime? completedAt = job.CompletedAt;

                        if (completedTasks + failedTasks == totalTasks)
                        {
                            // All tasks are done
                            newStatus = failedTasks > 0 ? "Completed with Errors" : "Completed";
                            completedAt = DateTime.UtcNow;
                        }
                        else if (processingTasks > 0 || completedTasks > 0)
                        {
                            // At least one task has started - mark as Processing
                            newStatus = "Processing";

                            // Set StartedAt if not already set
                            if (startedAt == null)
                            {
                                startedAt = DateTime.UtcNow;
                            }
                        }
                        // else stays "Pending"

                        // Update job statistics
                        job.Status = newStatus;
                        job.CompletedTasks = completedTasks;
                        job.FailedTasks = failedTasks;
                        job.ProcessedRows = processedRows;
                        job.StartedAt = startedAt;
                        job.CompletedAt = completedAt;

                        // Calculate PendingTasks (if you add this property to JobExecution)
                        // job.PendingTasks = pendingTasks;

                        _logger.LogDebug($"Job {job.JobId.Substring(0, 8)}: Status={newStatus}, Tasks={completedTasks}/{totalTasks}, Rows={processedRows}/{job.TotalRows}");
                    }

                    // Save all changes
                    var changedCount = await context.SaveChangesAsync(stoppingToken);

                    if (changedCount > 0)
                    {
                        _logger.LogDebug($"Updated {changedCount} job records");
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error updating job statuses");
                }

                // Check every 3 seconds
                await Task.Delay(TimeSpan.FromSeconds(3), stoppingToken);
            }
        }
    }
}
