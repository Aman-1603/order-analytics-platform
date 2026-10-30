using System.Text.Json;
using Confluent.Kafka;
using OrderAnalytics.Api.Models;

namespace OrderAnalytics.Api.Services;

// Publishes order.created events to Kafka. Messages are keyed by order id,
// so all events for one order go to the same partition.
public class OrderEventPublisher : IDisposable
{
    public const string OrderCreatedTopic = "order.created";

    private readonly IProducer<string, string> _producer;

    public OrderEventPublisher(string bootstrapServers)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = bootstrapServers,
            ClientId = "order-api",
            Acks = Acks.All,
        };

        _producer = new ProducerBuilder<string, string>(config).Build();
    }

    public async Task PublishOrderCreatedAsync(Order order, CancellationToken ct = default)
    {
        var payload = JsonSerializer.Serialize(new
        {
            orderId = order.Id,
            userId = order.UserId,
            items = order.Items,
            status = order.Status,
        });

        await _producer.ProduceAsync(
            OrderCreatedTopic,
            new Message<string, string> { Key = order.Id!, Value = payload },
            ct);
    }

    public void Dispose() => _producer.Dispose();
}
