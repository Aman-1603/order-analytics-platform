using MongoDB.Bson;
using MongoDB.Driver;
using OrderAnalytics.Api.Models;

namespace OrderAnalytics.Api.Services;

public class OrderRepository
{
    private readonly IMongoCollection<Order> _orders;

    public OrderRepository(IMongoClient client)
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
}
