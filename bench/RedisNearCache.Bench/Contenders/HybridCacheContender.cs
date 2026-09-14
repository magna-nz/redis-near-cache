using Microsoft.Extensions.Caching.Hybrid;
using Microsoft.Extensions.Caching.StackExchangeRedis;
using Microsoft.Extensions.DependencyInjection;
using StackExchange.Redis;

namespace RedisNearCache.Bench.Contenders;

/// <summary>
/// <see cref="ContenderKind.HybridCache"/>: <c>AddHybridCache</c> with its default in-process L1 and the official
/// <c>Microsoft.Extensions.Caching.StackExchangeRedis</c> <c>RedisCache</c> as L2 (entries under
/// <c>bench:l2:hybridcache:</c>, shared by every instance). The factory loads the source-of-truth key. HybridCache has no
/// cross-instance invalidation: another instance's L1 keeps a value until <c>LocalCacheExpiration</c>.
/// </summary>
public sealed class HybridCacheContender : IContender, ILocalTierPeek
{
    public const string L2Prefix = ContenderRedis.OwnedKeyPrefix + "hybridcache:";

    /// <summary>
    /// Local-only lookup: no L2 read or write, no factory, and no L1 write for a miss. Verified in the smoke checks to
    /// complete synchronously and never invoke the factory.
    /// </summary>
    private static readonly HybridCacheEntryOptions PeekOptions = new()
    {
        Flags = HybridCacheEntryFlags.DisableDistributedCache
                | HybridCacheEntryFlags.DisableUnderlyingData
                | HybridCacheEntryFlags.DisableLocalCacheWrite,
    };

    private readonly ContenderSettings _settings;
    private ConnectionMultiplexer? _mux;
    private IDatabase? _db;
    private ServiceProvider? _provider;
    private Microsoft.Extensions.Caching.Hybrid.HybridCache? _hybrid;
    private long _sourceLoads;

    public HybridCacheContender(ContenderSettings settings) => _settings = settings;

    public ContenderKind Kind => ContenderKind.HybridCache;

    public bool LocalHitsCompleteSynchronously => true;

    public long SourceLoads => Interlocked.Read(ref _sourceLoads);

    public ServiceProvider Provider => _provider ?? throw new InvalidOperationException("not initialized");

    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        // One multiplexer for the source-of-truth reads and the L2, as an application would share it.
        _mux = await ContenderRedis.ConnectAsync(_settings, $"bench-{_settings.InstanceName}-hybrid").ConfigureAwait(false);
        _db = _mux.GetDatabase();

        var services = new ServiceCollection();
        // LocalSizeLimit is NOT applied here. HybridCache takes IMemoryCache from DI and gives each L1 entry a Size equal
        // to its payload length in bytes (measured: 500 for a 500-char string), not 1, so MemoryCacheOptions.SizeLimit
        // would be a byte budget, not an entry count. The L1 is left at AddHybridCache's default (unbounded); the load
        // test keeps its key count below LocalSizeLimit, so no kind evicts for size anyway.
        services.AddStackExchangeRedisCache(o =>
        {
            o.InstanceName = L2Prefix;
            var mux = _mux;
            o.ConnectionMultiplexerFactory = () => Task.FromResult<IConnectionMultiplexer>(mux);
        });
        services.AddHybridCache(o => o.DefaultEntryOptions = new HybridCacheEntryOptions
        {
            Expiration = _settings.Ttl,
            LocalCacheExpiration = _settings.Ttl,
        });
        _provider = services.BuildServiceProvider();
        _hybrid = _provider.GetRequiredService<Microsoft.Extensions.Caching.Hybrid.HybridCache>();
    }

    public ValueTask<T?> GetAsync<T>(string key, CancellationToken cancellationToken = default) =>
        _hybrid!.GetOrCreateAsync(key, (Self: this, Key: key), Loader<T>.Load, cancellationToken: cancellationToken)!;

    public async ValueTask SetAsync<T>(string key, T value, CancellationToken cancellationToken = default)
    {
        await _db!.StringSetAsync(key, ContenderRedis.Encode(value)).ConfigureAwait(false);
        await _hybrid!.SetAsync(key, value, cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Treats a locally held <c>default</c> as absent (the benchmark never stores null/default values).</summary>
    public bool TryPeekLocal<T>(string key, out T? value)
    {
        var pending = _hybrid!.GetOrCreateAsync(key, (Self: this, Key: key), Loader<T>.MustNotRun, PeekOptions);
        if (!pending.IsCompletedSuccessfully)
            throw new InvalidOperationException("HybridCache local-only lookup did not complete synchronously");
        value = pending.Result;
        return value is not null;
    }

    public async ValueTask DisposeAsync()
    {
        if (_provider is not null) await _provider.DisposeAsync().ConfigureAwait(false);
        if (_mux is not null) await _mux.DisposeAsync().ConfigureAwait(false);
    }

    private static class Loader<T>
    {
        public static readonly Func<(HybridCacheContender Self, string Key), CancellationToken, ValueTask<T>> Load =
            static async (state, _) =>
            {
                Interlocked.Increment(ref state.Self._sourceLoads);
                return ContenderRedis.Decode<T>(await state.Self._db!.StringGetAsync(state.Key).ConfigureAwait(false))!;
            };

        public static readonly Func<(HybridCacheContender Self, string Key), CancellationToken, ValueTask<T>> MustNotRun =
            static (state, _) => throw new InvalidOperationException($"HybridCache ran the factory for a local-only peek of {state.Key}");
    }
}
