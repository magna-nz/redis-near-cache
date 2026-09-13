using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.DependencyInjection;
using StackExchange.Redis;

namespace RedisNearCache.Bench;

/// <summary>A small JSON payload (5 fields) used alongside the 1 KB string value.</summary>
public sealed record SampleRecord(string Name, int Age, bool Active, double Score, string Description);

/// <summary>Which of the two payload shapes a given benchmark iteration exercises.</summary>
public enum DataKind
{
    String,
    Json,
}

/// <summary>
/// Compares reading through <see cref="IRedisNearCache"/> (hit, miss, and pure-L1 paths) against a plain
/// StackExchange.Redis <see cref="IDatabase.StringGetAsync(RedisKey, CommandFlags)"/> baseline. Requires a
/// Redis server reachable at localhost:6379 (the repo's `redis-near-cache-redis` container).
/// Job configuration (warmup/iteration counts and the in-process toolchain, needed because this machine
/// does not have the dotnet SDK on PATH) is applied in <see cref="Program"/>, not via a job attribute here.
/// </summary>
[MemoryDiagnoser]
public class ReadBenchmarks
{
    private const string ConnectionString = "localhost:6379";
    private const string StringKey = "bench:near-cache:string";
    private const string JsonKey = "bench:near-cache:json";

    // A 1 KB string value.
    private static readonly string StringValue = new('x', 1024);

    private static readonly SampleRecord JsonValue =
        new("bench-user", 42, true, 3.14159, "A small sample JSON payload used for benchmarking.");

    private ServiceProvider _provider = null!;
    private IRedisNearCache _cache = null!;
    private ConnectionMultiplexer _plainMultiplexer = null!;
    private IDatabase _plainDb = null!;

    /// <summary>Which payload shape this benchmark run exercises. BenchmarkDotNet runs every benchmark once per value.</summary>
    [Params(DataKind.String, DataKind.Json)]
    public DataKind Kind { get; set; }

    private string Key => Kind == DataKind.String ? StringKey : JsonKey;

    [GlobalSetup]
    public void GlobalSetup()
    {
        var services = new ServiceCollection();
        services.AddRedisNearCache(ConnectionString);
        _provider = services.BuildServiceProvider();
        _cache = _provider.GetRequiredService<IRedisNearCache>();
        _cache.Ready.GetAwaiter().GetResult();

        _plainMultiplexer = ConnectionMultiplexer.Connect(ConnectionString);
        _plainDb = _plainMultiplexer.GetDatabase();

        // Seed both keys through the near cache so SetAsync's serializer is used consistently, then read each
        // once so the value is tracked and present in L1 for the Hit/TryGetLocal benchmarks.
        _cache.SetAsync(StringKey, StringValue).AsTask().GetAwaiter().GetResult();
        _cache.SetAsync(JsonKey, JsonValue).AsTask().GetAwaiter().GetResult();
        _cache.GetAsync<string>(StringKey).AsTask().GetAwaiter().GetResult();
        _cache.GetAsync<SampleRecord>(JsonKey).AsTask().GetAwaiter().GetResult();
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _provider.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _plainMultiplexer.Dispose();
    }

    /// <summary>Baseline: a plain StackExchange.Redis multiplexer, no near cache involved.</summary>
    [Benchmark(Baseline = true)]
    public Task<RedisValue> Plain_StringGet() => _plainDb.StringGetAsync(Key);

    /// <summary>Read of a key already present in L1 (the fast path).</summary>
    [Benchmark]
    public async Task<object?> NearCache_Hit() =>
        Kind == DataKind.String
            ? await _cache.GetAsync<string>(StringKey)
            : await _cache.GetAsync<SampleRecord>(JsonKey);

    /// <summary>Evicts the local copy first, so the read falls through to the tracked GET on the private connection.</summary>
    [Benchmark]
    public async Task<object?> NearCache_Miss()
    {
        _cache.EvictLocal(Key);
        return Kind == DataKind.String
            ? await _cache.GetAsync<string>(StringKey)
            : await _cache.GetAsync<SampleRecord>(JsonKey);
    }

    /// <summary>The pure L1 path: never touches Redis.</summary>
    [Benchmark]
    public object? NearCache_TryGetLocal()
    {
        if (Kind == DataKind.String)
        {
            _cache.TryGetLocal<string>(StringKey, out var value);
            return value;
        }

        _cache.TryGetLocal<SampleRecord>(JsonKey, out var jsonValue);
        return jsonValue;
    }
}
