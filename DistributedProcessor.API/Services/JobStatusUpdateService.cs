using DistributedProcessor.Data;
using DistributedProcessor.API.Hubs;
using Microsoft.AspNetCore.SignalR;
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
            _logger.LogInformation("Job Status Update Service started - Updating every 500ms for real-time visibility");

            await Task.Delay(1000, stoppingToken);

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
                        var tasks = await context.TaskLogs
                            .Where(t => t.JobId == job.JobId)
                            .ToListAsync(stoppingToken);

                        if (tasks.Count == 0)
                        {
                            continue;
                        }

                        var totalTasks = tasks.Count;
                        var completedTasks = tasks.Count(t => t.Status == "Completed");
                        var collectedTasks = tasks.Count(t => t.Status == "Collected");
                        var failedTasks = tasks.Count(t => t.Status == "Failed");
                        var processingTasks = tasks.Count(t => t.Status == "Processing");
                        var processedTasks = tasks.Count(t => t.Status == "Processed");
                        var pendingTasks = tasks.Count(t => t.Status == "Pending");

                        // Calculate processed rows
                        var processedRows = tasks
                            .Where(t => t.Status == "Completed" || t.Status == "Collected" || t.Status == "Processed")
                            .Sum(t => t.RowsProcessed ?? 0);

                        // FIXED: Determine job status - treat Collected as finished
                        string newStatus = job.Status;
                        DateTime? startedAt = job.StartedAt;
                        DateTime? completedAt = job.CompletedAt;

                        // Count truly finished tasks (Collected + Completed + Failed)
                        int finishedTasks = completedTasks + collectedTasks + failedTasks;

                        if (finishedTasks == totalTasks)
                        {
                            // All tasks are done
                            newStatus = failedTasks > 0 ? "Completed with Errors" : "Completed";
                            completedAt = DateTime.UtcNow;
                        }
                        else if (processingTasks > 0 || processedTasks > 0 || collectedTasks > 0 || completedTasks > 0)
                        {
                            // At least one task has started or is in progress
                            newStatus = "Processing";
                            if (startedAt == null)
                            {
                                startedAt = DateTime.UtcNow;
                            }
                        }
                        // else stays "Pending"

                        // FIXED: Update completed tasks to include Collected
                        job.Status = newStatus;
                        job.CompletedTasks = completedTasks + collectedTasks; // Include both
                        job.FailedTasks = failedTasks;
                        job.ProcessedRows = processedRows;
                        job.StartedAt = startedAt;
                        job.CompletedAt = completedAt;

                        _logger.LogDebug($"Job {job.JobId.Substring(0, 8)}: Status={newStatus}, " +
                            $"Tasks=Cm:{completedTasks}/Cl:{collectedTasks}/P:{processingTasks}/Pd:{processedTasks}/Pn:{pendingTasks}/F:{failedTasks}, " +
                            $"Rows={processedRows}/{job.TotalRows}");
                    }

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

                await Task.Delay(TimeSpan.FromMilliseconds(500), stoppingToken);
            }
        }

    }
}
