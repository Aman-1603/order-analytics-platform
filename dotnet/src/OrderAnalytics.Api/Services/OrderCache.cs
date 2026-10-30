using System.Text.Json;
using StackExchange.Redis;

namespace OrderAnalytics.Api.Services;

// Cache-aside: read from Redis first, fall back to MongoDB, then write back with a TTL.
public class OrderCache
{
    private static readonly TimeSpan Ttl = TimeSpan.FromSeconds(60);
    private readonly IDatabase _db;

    public OrderCache(IConnectionMultiplexer redis)
    {
        _db = redis.GetDatabase();
    }

    public async Task<string?> GetAsync(string orderId)
    {
        var value = await _db.StringGetAsync(Key(orderId));
        return value.HasValue ? value.ToString() : null;
    }

    public Task SetAsync(OrderAnalytics.Api.Models.Order order)
    {
        var json = JsonSerializer.Serialize(order);
        return _db.StringSetAsync(Key(order.Id!), json, Ttl);
    }

    // Drop the cached copy so the next read gets the latest status from MongoDB.
    public Task RemoveAsync(string orderId) => _db.KeyDeleteAsync(Key(orderId));

    private static string Key(string orderId) => $"order:{orderId}";
}
