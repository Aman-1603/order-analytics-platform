using MongoDB.Bson;
using MongoDB.Driver;
using OrderAnalytics.Api.Models;

namespace OrderAnalytics.Api.Services;

public class OrderRepository
{
    private readonly IMongoCollection<Order> _orders;

    public OrderRepository(MongoDB.Driver.IMongoClient client)
    {
        var database = client.GetDatabase("orders");
        _orders = database.GetCollection<Order>("orders");

        _orders.Indexes.CreateOne(new CreateIndexModel<Order>(
            Builders<Order>.IndexKeys.Ascending(o => o.UserId)));
        _orders.Indexes.CreateOne(new CreateIndexModel<Order>(
            Builders<Order>.IndexKeys.Ascending(o => o.Status)));
    }

    public async Task<Order> CreateAsync(Order order, CancellationToken ct = default)
    {
        await _orders.InsertOneAsync(order, cancellationToken: ct);
        return order;
    }

    public async Task<Order?> FindByIdAsync(string id, CancellationToken ct = default)
    {
        if (!ObjectId.TryParse(id, out _))
        {
            return null;
        }

        return await _orders
            .Find(o => o.Id == id)
            .FirstOrDefaultAsync(ct);
    }

    public async Task UpdateStatusAsync(string id, string status, CancellationToken ct = default)
    {
        if (!ObjectId.TryParse(id, out _))
        {
            return;
        }

        var update = Builders<Order>.Update
            .Set(o => o.Status, status)
            .Set(o => o.UpdatedAt, DateTime.UtcNow);

        await _orders.UpdateOneAsync(o => o.Id == id, update, cancellationToken: ct);
    }

    public async Task MarkFailedAsync(string id, string errorMessage, int retryCount, CancellationToken ct = default)
    {
        if (!ObjectId.TryParse(id, out _))
        {
            return;
        }

        var update = Builders<Order>.Update
            .Set(o => o.Status, "FAILED")
            .Set(o => o.ErrorMessage, errorMessage)
            .Set(o => o.RetryCount, retryCount)
            .Set(o => o.UpdatedAt, DateTime.UtcNow);

        await _orders.UpdateOneAsync(o => o.Id == id, update, cancellationToken: ct);
    }
}
