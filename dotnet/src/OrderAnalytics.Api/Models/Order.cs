using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace OrderAnalytics.Api.Models;

public class Order
{
    [BsonId]
    [BsonRepresentation(BsonType.ObjectId)]
    public string? Id { get; set; }

    public string UserId { get; set; } = string.Empty;

    public List<string> Items { get; set; } = new();

    public string Status { get; set; } = "CREATED";

    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;

    public DateTime UpdatedAt { get; set; } = DateTime.UtcNow;
}

public record CreateOrderRequest(string? UserId, List<string>? Items);
