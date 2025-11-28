using Confluent.Kafka;
using System.Text.Json;
using DistributedProcessor.Shared.Models;
using DistributedProcessor.Data.Services;
using DistributedProcessor.Data;
using DistributedProcessor.Data.Models;
using System.Threading.Tasks;
using static Confluent.Kafka.ConfigPropertyNames;

namespace DistributedProcessor.API.Services
{
    public interface ITaskOrchestrator
    {
        Task<(bool success, int tasksCreated, int rowsCount)> CreateAndDistributeTasksAsync(UserJobRequest request);
    }

    public class TaskOrchestrator : ITaskOrchestrator
    {
        private readonly IProducer<string, string> _producer;
        private readonly IDbService _dbService;
        private readonly ApplicationDbContext _context;
        private readonly ILogger<TaskOrchestrator> _logger;
        private const string TaskTopic = "processing-tasks";

        public TaskOrchestrator(
            IDbService dbService,
            ILogger<TaskOrchestrator> logger,
            IConfiguration config,
            ApplicationDbContext context)
        {
            _dbService = dbService;
            _logger = logger;
            _context = context;

            var producerConfig = new ProducerConfig
            {
                BootstrapServers = config["Kafka:BootstrapServers"] ?? "localhost:5081"
            };
            _producer = new ProducerBuilder<string, string>(producerConfig).Build();
        }

        public async Task<(bool success, int tasksCreated, int rowsCount)> CreateAndDistributeTasksAsync(UserJobRequest request)
        {
            try
            {
                _logger.LogInformation($"Creating tasks for JobId: {request.JobId}, Fund: {request.Fund}, Symbol: {request.Symbol}");

                var rows = await _dbService.FetchRowsAsync(request.Fund, request.Symbol, request.StartDate, request.EndDate);

                if (!rows.Any())
                {
                    _logger.LogWarning($"No rows found for {request.Fund}/{request.Symbol}");
                    return (false, 0, 0);
                }

                int batchSize = 1000;
                int totalBatches = 0;
                int totalRows = rows.Count;

                foreach (var batch in rows.Chunk(batchSize))
                {
                    var task = new ProcessingTask
                    {
                        TaskId = Guid.NewGuid().ToString(),
                        JobId = request.JobId,
                        Fund = request.Fund,
                        Symbol = request.Symbol,
                        StartDate = request.StartDate,
                        EndDate = request.EndDate,
                        Rows = batch.Select(r => new SourceDataRow
                        {
                            Id = r.Id,
                            Fund = r.Fund,
                            Symbol = r.Symbol,
                            Date = r.Date,
                            Value = r.ClosePrice
                        }).ToList(),
                        AssignedWorkerId = null
                    };

                    var key = Guid.NewGuid().ToString();

                    var message = new Message<string, string>
                    {
                        Key = key,
                        Value = JsonSerializer.Serialize(task)
                    };

                    var result = await _producer.ProduceAsync(TaskTopic, message);
                    totalBatches++;

                    _logger.LogInformation($"Task {task.TaskId}: Partition {result.Partition} (Fund: {task.Fund}, Symbol: {task.Symbol})");

                    await CreateTaskLogAsync(task);
                }

                _producer.Flush(TimeSpan.FromSeconds(5));

                _logger.LogInformation($"Created {totalBatches} tasks ({totalRows} rows) for {request.Fund}/{request.Symbol}");

                return (true, totalBatches, totalRows);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error creating tasks for job {request.JobId}");
                return (false, 0, 0);
            }
        }

        private async Task CreateTaskLogAsync(ProcessingTask task)
        {
            try
            {
                var taskLog = new TaskLog
                {
                    TaskId = task.TaskId,
                    JobId = task.JobId,
                    Fund = task.Fund,
                    Symbol = task.Symbol,
                    Status = "Pending",
                    CreatedAt = DateTime.UtcNow
                };

                _context.TaskLogs.Add(taskLog);
                await _context.SaveChangesAsync();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error creating TaskLog for {task.TaskId}");
            }
        }

        public void Dispose()
        {
            _producer?.Dispose();
        }
    }
}
