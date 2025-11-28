using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace DistributedProcessor.API.Services
{
    public interface IKafkaAdminService
    {
        Task CreateTopicsAsync();
    }

    public class KafkaAdminService : IKafkaAdminService
    {
        private readonly ILogger<KafkaAdminService> _logger;
        private readonly IConfiguration _configuration;

        public KafkaAdminService(ILogger<KafkaAdminService> logger, IConfiguration configuration)
        {
            _logger = logger;
            _configuration = configuration;
        }

        public async Task CreateTopicsAsync()
        {
            var bootstrapServers = _configuration["Kafka:BootstrapServers"];

            using var adminClient = new AdminClientBuilder(new AdminClientConfig
            {
                BootstrapServers = bootstrapServers
            }).Build();

            var topics = new List<TopicSpecification>
            {
                new() { Name = "processing-tasks", NumPartitions = 10, ReplicationFactor = 1 },
                new() { Name = "processing-results", NumPartitions = 5, ReplicationFactor = 1 }
            };

            try
            {
                await adminClient.CreateTopicsAsync(topics);
                _logger.LogInformation("Kafka topics created successfully");
            }
            catch (CreateTopicsException ex)
            {
                foreach (var result in ex.Results)
                {
                    if (result.Error.Code != ErrorCode.TopicAlreadyExists)
                        _logger.LogError($"Error creating topic {result.Topic}: {result.Error.Reason}");
                    else
                        _logger.LogInformation($"Topic {result.Topic} already exists");
                }
            }
        }
    }
}
