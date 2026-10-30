namespace OrderAnalytics.Api.Models;

// Shape of the message published to the order.created topic.
public record OrderCreatedEvent(string OrderId, string UserId, List<string> Items, string Status);
