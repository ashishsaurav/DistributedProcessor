using Confluent.Kafka;
using DistributedProcessor.API.Services;
using DistributedProcessor.Data.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Text;

namespace DistributedProcessor.API.Services
{
    public class OutboxPublisherService : BackgroundService
    {
        private readonly IServiceScopeFactory _scopeFactory;
        private readonly ILogger<OutboxPublisherService> _logger;
        private readonly IProducer<string, string> _producer;
        private readonly TimeSpan _pollingInterval = TimeSpan.FromSeconds(1);

        public OutboxPublisherService(
            IServiceScopeFactory scopeFactory,
            IConfiguration configuration,
            ILogger<OutboxPublisherService> logger)
        {
            _scopeFactory = scopeFactory;
            _logger = logger;

            var producerConfig = new ProducerConfig
            {
                BootstrapServers = configuration["Kafka:BootstrapServers"] ?? "localhost:5081",
                Acks = Acks.All,
                EnableIdempotence = true,
                MessageSendMaxRetries = 3,
                CompressionType = CompressionType.Lz4
            };

            _producer = new ProducerBuilder<string, string>(producerConfig).Build();
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("Outbox Publisher Service started");

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    await ProcessOutboxMessagesAsync(stoppingToken);
                    await Task.Delay(_pollingInterval, stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error processing outbox messages");
                    await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
                }
            }

            _logger.LogInformation("Outbox Publisher Service stopped");
        }

        private async Task ProcessOutboxMessagesAsync(CancellationToken stoppingToken)
        {
            using var scope = _scopeFactory.CreateScope();
            var outboxService = scope.ServiceProvider.GetRequiredService<IOutboxService>();

            var pendingMessages = await outboxService.GetPendingMessagesAsync(100);

            if (pendingMessages.Count == 0)
                return;

            _logger.LogDebug("Processing {Count} outbox messages", pendingMessages.Count);

            foreach (var outboxMessage in pendingMessages)
            {
                try
                {
                    var kafkaMessage = new Message<string, string>
                    {
                        Key = outboxMessage.MessageKey ?? outboxMessage.MessageId,
                        Value = outboxMessage.Payload,
                        Headers = new Headers
                        {
                            { "message-id", Encoding.UTF8.GetBytes(outboxMessage.MessageId) },
                            { "message-type", Encoding.UTF8.GetBytes(outboxMessage.MessageType) },
                            { "created-at", Encoding.UTF8.GetBytes(outboxMessage.CreatedAt.ToString("O")) }
                        }
                    };

                    var deliveryResult = await _producer.ProduceAsync(
                        outboxMessage.Topic,
                        kafkaMessage,
                        stoppingToken);

                    await outboxService.MarkAsSentAsync(outboxMessage.Id);

                    _logger.LogDebug(
                        "Published outbox message {MessageId} to {Topic} at offset {Offset}",
                        outboxMessage.MessageId, outboxMessage.Topic, deliveryResult.Offset);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex,
                        "Failed to publish outbox message {MessageId}",
                        outboxMessage.MessageId);

                    await outboxService.MarkAsFailedAsync(outboxMessage.Id, ex.Message);
                }
            }
        }

        public override void Dispose()
        {
            _producer?.Flush(TimeSpan.FromSeconds(10));
            _producer?.Dispose();
            base.Dispose();
        }
    }
}
