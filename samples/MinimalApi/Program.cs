using System.Text.Json;
using Microsoft.Extensions.Caching.Hybrid;
using RedisNearCache;
using RedisNearCache.HybridCache;

var builder = WebApplication.CreateBuilder(args);

// System.Text.Json default options, except PropertyNameCaseInsensitive so that JSON written by hand from
// redis-cli (e.g. {"id":1,"name":"changed"}) deserializes into our PascalCase Product record without a
// custom naming policy on both sides.
var jsonOptions = new JsonSerializerOptions(JsonSerializerOptions.Default)
{
    PropertyNameCaseInsensitive = true,
};

builder.Services.AddRedisNearCache("localhost:6379", options =>
{
    options.Serializer = new JsonRedisNearCacheSerializer(jsonOptions);
});
builder.Services.AddRedisNearCacheHybridCache();

builder.Services.AddSingleton<ProductRepository>();

var app = builder.Build();

var nearCache = app.Services.GetRequiredService<IRedisNearCache>();
await nearCache.Ready;

app.Logger.LogInformation("RedisNearCache MinimalApi sample is ready.");
app.Logger.LogInformation("Try:");
app.Logger.LogInformation("  curl http://localhost:PORT/products/1        (first call: L1 miss, reads through the repository)");
app.Logger.LogInformation("  curl http://localhost:PORT/products/1        (second call: served from L1, no Redis round trip)");
app.Logger.LogInformation("  curl http://localhost:PORT/stats");
app.Logger.LogInformation("Then, from another terminal, invalidate product:1 directly in Redis (bypassing this app entirely):");
app.Logger.LogInformation(
    "  docker exec redis-near-cache-redis redis-cli SET product:1 '{Json}'",
    """{"id":1,"name":"changed"}""");
app.Logger.LogInformation("  curl http://localhost:PORT/products/1        (server pushed the invalidation; this reflects the change)");

app.MapGet("/products/{id:int}", async (int id, IRedisNearCache cache, ProductRepository repository) =>
{
    var key = $"product:{id}";

    var product = await cache.GetAsync<Product>(key);
    if (product is null)
    {
        product = await repository.FindAsync(id);
        if (product is null)
        {
            return Results.NotFound();
        }

        await cache.SetAsync(key, product);
    }

    return Results.Ok(product);
});

app.MapPut("/products/{id:int}", async (int id, Product product, IRedisNearCache cache) =>
{
    var key = $"product:{id}";
    await cache.SetAsync(key, product);
    return Results.Ok(product);
});

app.MapGet("/stats", (IRedisNearCache cache) => Results.Ok(cache.Statistics));

app.MapGet("/hybrid/{id:int}", async (int id, HybridCache hybridCache, ProductRepository repository) =>
{
    var key = $"product:{id}";

    var product = await hybridCache.GetOrCreateAsync(key, async cancellationToken =>
        await repository.FindAsync(id, cancellationToken) ?? new Product(id, "unknown"));

    return Results.Ok(product);
});

app.Run();

/// <summary>A small record shape stored as JSON under keys like <c>product:1</c>.</summary>
public sealed record Product(int Id, string Name);

/// <summary>Stands in for a real data store: an in-memory dictionary with a simulated 50 ms lookup delay.</summary>
public sealed class ProductRepository
{
    private readonly Dictionary<int, Product> _products = new()
    {
        [1] = new Product(1, "Widget"),
        [2] = new Product(2, "Gadget"),
        [3] = new Product(3, "Gizmo"),
    };

    public async Task<Product?> FindAsync(int id, CancellationToken cancellationToken = default)
    {
        await Task.Delay(TimeSpan.FromMilliseconds(50), cancellationToken);
        return _products.TryGetValue(id, out var product) ? product : null;
    }
}
