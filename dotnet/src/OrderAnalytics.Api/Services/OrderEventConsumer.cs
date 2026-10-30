using System.Text.Json;
using Confluent.Kafka;
using OrderAnalytics.Api.Models;

namespace OrderAnalytics.Api.Services;

// Reads order.created events and moves each order to PROCESSING.
// A failed event is retried with exponential backoff (500ms, 1s, 2s).
// After the last retry fails, it goes to the dead-letter topic and the order is marked FAILED.
public class OrderEventConsumer : BackgroundService
{
    public const string DeadLetterTopic = "order.created.dlq";
    public const int MaxRetries = 3;
    private const int BaseDelayMs = 500;

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

    // Exponential backoff: attempt 0 waits 500ms, attempt 1 waits 1000ms, attempt 2 waits 2000ms.
    public static TimeSpan GetBackoffDelay(int attempt) =>
        TimeSpan.FromMilliseconds(BaseDelayMs * Math.Pow(2, attempt));

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

        var dlqConfig = new ProducerConfig
        {
            BootstrapServers = _bootstrapServers,
            ClientId = "order-api-dlq",
            Acks = Acks.All,
        };

        using var consumer = new ConsumerBuilder<string, string>(config).Build();
        using var dlqProducer = new ProducerBuilder<string, string>(dlqConfig).Build();

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

            OrderCreatedEvent? evt;
            try
            {
                evt = JsonSerializer.Deserialize<OrderCreatedEvent>(result.Message.Value, JsonOptions);
            }
            catch (JsonException ex)
            {
                // A malformed message will never succeed, so send it straight to the DLQ.
                _logger.LogError("Malformed message at offset {Offset}: {Message}", result.Offset, ex.Message);
                await SendToDeadLetterAsync(dlqProducer, result.Message.Key, result.Message.Value, ex.Message, ct);
                consumer.Commit(result);
                continue;
            }

            if (evt is null)
            {
                _logger.LogWarning("Skipping empty message at offset {Offset}", result.Offset);
                consumer.Commit(result);
                continue;
            }

            var processed = await ProcessWithRetryAsync(evt, ct);
            if (!processed)
            {
                await SendToDeadLetterAsync(dlqProducer, evt.OrderId, result.Message.Value, "retries exhausted", ct);
                await _repo.MarkFailedAsync(evt.OrderId, "Processing failed after retries", MaxRetries, ct);
                await _cache.RemoveAsync(evt.OrderId);
            }

            consumer.Commit(result);
        }

        consumer.Close();
    }

    // Returns true if the order was moved to PROCESSING, false if all attempts failed.
    private async Task<bool> ProcessWithRetryAsync(OrderCreatedEvent evt, CancellationToken ct)
    {
        for (var attempt = 0; attempt < MaxRetries; attempt++)
        {
            try
            {
                await _repo.UpdateStatusAsync(evt.OrderId, "PROCESSING", ct);
                await _cache.RemoveAsync(evt.OrderId);
                _logger.LogInformation("Order {OrderId} moved to PROCESSING", evt.OrderId);
                return true;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                var delay = GetBackoffDelay(attempt);
                _logger.LogWarning(
                    "Attempt {Attempt}/{Max} failed for order {OrderId}. Retrying in {Delay}ms. Error: {Message}",
                    attempt + 1, MaxRetries, evt.OrderId, delay.TotalMilliseconds, ex.Message);
                await Task.Delay(delay, ct);
            }
        }

        return false;
    }

    private static async Task SendToDeadLetterAsync(
        IProducer<string, string> producer,
        string? key,
        string value,
        string reason,
        CancellationToken ct)
    {
        var dlqPayload = JsonSerializer.Serialize(new
        {
            originalMessage = value,
            reason,
            failedAt = DateTime.UtcNow,
        });

        await producer.ProduceAsync(
            DeadLetterTopic,
            new Message<string, string>
            {
                Key = key ?? string.Empty,
                Value = dlqPayload,
                Headers = new Headers { { "retriesExhausted", System.Text.Encoding.UTF8.GetBytes(MaxRetries.ToString()) } },
            },
            ct);
    }
}
