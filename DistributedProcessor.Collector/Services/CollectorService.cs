using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using DistributedProcessor.Shared.Models;
using DistributedProcessor.Data.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace DistributedProcessor.Collector.Services
{
    public class CollectorService : BackgroundService
    {
        private readonly ILogger<CollectorService> _logger;
        private readonly IServiceScopeFactory _scopeFactory;
        private readonly IConsumer<string, string> _consumer;

        public CollectorService(ILogger<CollectorService> logger, IServiceScopeFactory scopeFactory, IConfiguration configuration)
        {
            _logger = logger;
            _scopeFactory = scopeFactory;

            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = configuration["Kafka:BootstrapServers"] ?? "localhost:5081",
                GroupId = "collector-group",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false
            };
            _consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _consumer.Subscribe("processing-results");
            _logger.LogInformation("CollectorService subscribed to processing-results topic");

            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    try
                    {
                        var consumeResult = _consumer.Consume(stoppingToken);

                        if (consumeResult?.Message?.Value == null)
                        {
                            _logger.LogWarning("Received null message");
                            continue;
                        }

                        _logger.LogInformation($"Consumed message at offset {consumeResult.Offset}");

                        var result = JsonSerializer.Deserialize<ProcessingResult>(consumeResult.Message.Value);

                        if (result == null)
                        {
                            _logger.LogWarning("Deserialized ProcessingResult is null");
                            _consumer.Commit(consumeResult);
                            continue;
                        }

                        using var scope = _scopeFactory.CreateScope();
                        var dbService = scope.ServiceProvider.GetRequiredService<IDbService>();

                        _logger.LogInformation($"Saving results for TaskId: {result.TaskId}");

                        // DbService will set CreatedAt automatically
                        await dbService.SaveCalculatedResultsAsync(result);

                        _logger.LogInformation("Results saved successfully");
                        _consumer.Commit(consumeResult);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error in CollectorService");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation("CollectorService stopping");
            }
            finally
            {
                _logger.LogInformation("Closing Kafka consumer");
                _consumer.Close();
            }
        }

        public override void Dispose()
        {
            _logger.LogInformation("Disposing CollectorService.");
            _consumer.Dispose();
            base.Dispose();
        }
    }
}
