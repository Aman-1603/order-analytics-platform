using System.Text.Json;
using Microsoft.AspNetCore.Mvc;
using OrderAnalytics.Api.Models;
using OrderAnalytics.Api.Services;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

var mongoUri = Environment.GetEnvironmentVariable("MONGODB_URI")
    ?? throw new InvalidOperationException("MONGODB_URI is not set");
var redisUrl = Environment.GetEnvironmentVariable("REDIS_URL") ?? "localhost:6379";

builder.Services.AddSingleton<MongoDB.Driver.IMongoClient>(_ => new MongoDB.Driver.MongoClient(mongoUri));
builder.Services.AddSingleton<IConnectionMultiplexer>(_ => ConnectionMultiplexer.Connect(redisUrl));
builder.Services.AddSingleton<OrderRepository>();
builder.Services.AddSingleton<OrderCache>();

var app = builder.Build();

app.MapGet("/health", () => Results.Ok(new { status = "Order Service is running" }));

app.MapPost("/orders", async (
    [FromBody] CreateOrderRequest request,
    OrderRepository repo) =>
{
    if (string.IsNullOrWhiteSpace(request.UserId)
        || request.Items is null
        || request.Items.Count == 0)
    {
        return Results.BadRequest(new { message = "Missing or invalid required fields" });
    }

    var order = new Order
    {
        UserId = request.UserId.Trim(),
        Items = request.Items,
        Status = "CREATED",
    };

    var created = await repo.CreateAsync(order);
    return Results.Created($"/orders/{created.Id}", created);
});

app.MapGet("/orders/{id}", async (
    string id,
    OrderRepository repo,
    OrderCache cache) =>
{
    var cached = await cache.GetAsync(id);
    if (cached is not null)
    {
        return Results.Content(cached, "application/json");
    }

    var order = await repo.FindByIdAsync(id);
    if (order is null)
    {
        return Results.NotFound(new { message = "Order not found" });
    }

    await cache.SetAsync(order);
    return Results.Ok(order);
});

app.Run();
