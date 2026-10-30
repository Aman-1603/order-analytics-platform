using System.Text.Json;
using Confluent.Kafka;
using OrderAnalytics.Api.Models;

namespace OrderAnalytics.Api.Services;

// Reads order.created events and moves each order to PROCESSING.
// Retries and the dead-letter queue are not implemented yet.
public class OrderEventConsumer : BackgroundService
{
    private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNameCaseInsensitive = true };

    private readonly string _bootstrapServers;
    private readonly OrderRepository _repo;
    private readonly OrderCache _cache;
    private readonly ILogger<OrderEventConsumer> _logger;

    public OrderEventConsumer(
        string bootstrapServers,
        OrderRepository repo,
        OrderCache cache,
        ILogger<OrderEventConsumer> logger)
    {
        _bootstrapServers = bootstrapServers;
        _repo = repo;
        _cache = cache;
        _logger = logger;
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Kafka's Consume call blocks, so run the loop on its own thread.
        return Task.Run(() => ConsumeLoopAsync(stoppingToken), stoppingToken);
    }

    private async Task ConsumeLoopAsync(CancellationToken ct)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = "order-processing-group",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
        };

        using var consumer = new ConsumerBuilder<string, string>(config).Build();
        consumer.Subscribe(OrderEventPublisher.OrderCreatedTopic);
        _logger.LogInformation("Consumer subscribed to {Topic}", OrderEventPublisher.OrderCreatedTopic);

        while (!ct.IsCancellationRequested)
        {
            ConsumeResult<string, string> result;
            try
            {
                result = consumer.Consume(ct);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (ConsumeException ex)
            {
                _logger.LogError("Kafka consume error: {Message}", ex.Error.Reason);
                continue;
            }

            try
            {
                var evt = JsonSerializer.Deserialize<OrderCreatedEvent>(result.Message.Value, JsonOptions);
                if (evt is null)
                {
                    _logger.LogWarning("Skipping empty message at offset {Offset}", result.Offset);
                    consumer.Commit(result);
                    continue;
                }

                await _repo.UpdateStatusAsync(evt.OrderId, "PROCESSING", ct);
                await _cache.RemoveAsync(evt.OrderId);
                consumer.Commit(result);

                _logger.LogInformation("Order {OrderId} moved to PROCESSING", evt.OrderId);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                // No retry yet: log and move on so the consumer keeps running.
                _logger.LogError(ex, "Failed to process message at offset {Offset}", result.Offset);
                consumer.Commit(result);
            }
        }

        consumer.Close();
    }
}
