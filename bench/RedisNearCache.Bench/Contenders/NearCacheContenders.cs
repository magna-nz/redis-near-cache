using Microsoft.Extensions.Caching.Hybrid;
using Microsoft.Extensions.DependencyInjection;
using RedisNearCache.HybridCache;
using StackExchange.Redis;

namespace RedisNearCache.Bench.Contenders;

/// <summary>The RedisNearCache kinds, exposing the library objects so the load test can read statistics and hook diagnostics.</summary>
public interface IRedisNearCacheContender : IContender
{
    ServiceProvider Provider { get; }

    IRedisNearCache Cache { get; }
}

/// <summary><see cref="ContenderKind.NearCache"/>: <see cref="IRedisNearCache"/> used directly.</summary>
public sealed class NearCacheContender : IRedisNearCacheContender, ILocalTierPeek
{
    private readonly ContenderSettings _settings;
    private ServiceProvider? _provider;
    private IRedisNearCache? _cache;

    public NearCacheContender(ContenderSettings settings) => _settings = settings;

    public ContenderKind Kind => ContenderKind.NearCache;

    public bool LocalHitsCompleteSynchronously => true;

    public long SourceLoads => Cache.Statistics.Misses;

    public ServiceProvider Provider => _provider ?? throw new InvalidOperationException("not initialized");

    public IRedisNearCache Cache => _cache ?? throw new InvalidOperationException("not initialized");

    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        var services = new ServiceCollection();
        services.AddRedisNearCache(o =>
        {
            o.Configuration = ContenderRedis.BuildConfiguration(_settings);
            o.L1SizeLimit = _settings.LocalSizeLimit;
            // ClientNamePrefix stays "rnc": the load test's chaos mode kills clients by that prefix.
        });
        _provider = services.BuildServiceProvider();
        // Resolving connects synchronously; keep that off the caller's thread.
        _cache = await Task.Run(() => _provider.GetRequiredService<IRedisNearCache>(), cancellationToken).ConfigureAwait(false);
        await _cache.Ready.WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    public ValueTask<T?> GetAsync<T>(string key, CancellationToken cancellationToken = default) =>
        _cache!.GetAsync<T>(key, cancellationToken);

    public ValueTask SetAsync<T>(string key, T value, CancellationToken cancellationToken = default) =>
        _cache!.SetAsync(key, value, cancellationToken: cancellationToken);

    public bool TryPeekLocal<T>(string key, out T? value) => _cache!.TryGetLocal(key, out value);

    public async ValueTask DisposeAsync()
    {
        if (_provider is not null) await _provider.DisposeAsync().ConfigureAwait(false);
    }
}

/// <summary>
/// <see cref="ContenderKind.NearCacheHybridCache"/>: <c>HybridCache</c> over <c>AddRedisNearCacheHybridCache</c>
/// (HybridCache's own L1 disabled, RedisNearCache's tracked L1 behind <c>IDistributedCache</c>). HybridCache entries live at
/// <c>bench:l2:hc:{key}</c>; the factory loads the source-of-truth key over a separate plain connection.
/// <para>
/// A foreign write to the source key does not touch the HybridCache entry, so in <see cref="WriteMode.Foreign"/> this
/// kind serves the old value until <c>Expiration</c>. That is the adapter's real behaviour and is measured as such.
/// </para>
/// <para>
/// No <see cref="ILocalTierPeek"/>: the only local tier holds HybridCache's L2 payload (a header plus the serialized
/// value) and reading it back through HybridCache would go through <c>IDistributedCache</c>, which may touch the network.
/// </para>
/// </summary>
public sealed class NearCacheHybridCacheContender : IRedisNearCacheContender
{
    public const string HybridKeyPrefix = ContenderRedis.OwnedKeyPrefix + "hc:";

    private readonly ContenderSettings _settings;
    private ConnectionMultiplexer? _sourceMux;
    private IDatabase? _db;
    private ServiceProvider? _provider;
    private IRedisNearCache? _cache;
    private Microsoft.Extensions.Caching.Hybrid.HybridCache? _hybrid;
    private long _sourceLoads;

    public NearCacheHybridCacheContender(ContenderSettings settings) => _settings = settings;

    public ContenderKind Kind => ContenderKind.NearCacheHybridCache;

    /// <summary>
    /// False. A lone caller's L1 hit does complete synchronously, but with <c>DisableLocalCache</c> HybridCache routes
    /// every read through its stampede coordination, and a concurrent caller for the same key joins the in-flight
    /// operation and completes asynchronously even though nothing touched the network. Measured in a 4 x 2-reader smoke
    /// run: only 3-4 % of reads completed synchronously, while the "remote" reads had a p50 of 5.8 µs (a real round trip
    /// is ~300 µs here) and the server saw 48 k GETs for 6.2 M reads. Synchronous completion therefore cannot classify
    /// this kind's reads; RedisNearCache's own <c>Statistics.Hits</c> is the L1 hit count.
    /// </summary>
    public bool LocalHitsCompleteSynchronously => false;

    public long SourceLoads => Interlocked.Read(ref _sourceLoads);

    public ServiceProvider Provider => _provider ?? throw new InvalidOperationException("not initialized");

    public IRedisNearCache Cache => _cache ?? throw new InvalidOperationException("not initialized");

    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        _sourceMux = await ContenderRedis.ConnectAsync(_settings, $"bench-{_settings.InstanceName}-nchc-source").ConfigureAwait(false);
        _db = _sourceMux.GetDatabase();

        var services = new ServiceCollection();
        services.AddRedisNearCache(o =>
        {
            o.Configuration = ContenderRedis.BuildConfiguration(_settings);
            o.L1SizeLimit = _settings.LocalSizeLimit;
        });
        services.AddRedisNearCacheHybridCache(o => o.DefaultEntryOptions = new HybridCacheEntryOptions
        {
            Flags = HybridCacheEntryFlags.DisableLocalCache,
            Expiration = _settings.Ttl,
        });
        _provider = services.BuildServiceProvider();
        _cache = await Task.Run(() => _provider.GetRequiredService<IRedisNearCache>(), cancellationToken).ConfigureAwait(false);
        await _cache.Ready.WaitAsync(cancellationToken).ConfigureAwait(false);
        _hybrid = _provider.GetRequiredService<Microsoft.Extensions.Caching.Hybrid.HybridCache>();
    }

    public ValueTask<T?> GetAsync<T>(string key, CancellationToken cancellationToken = default) =>
        _hybrid!.GetOrCreateAsync(HybridKeyPrefix + key, (Self: this, Key: key), Loader<T>.Load, cancellationToken: cancellationToken)!;

    public async ValueTask SetAsync<T>(string key, T value, CancellationToken cancellationToken = default)
    {
        await _db!.StringSetAsync(key, ContenderRedis.Encode(value)).ConfigureAwait(false);
        await _hybrid!.SetAsync(HybridKeyPrefix + key, value, cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        if (_provider is not null) await _provider.DisposeAsync().ConfigureAwait(false);
        if (_sourceMux is not null) await _sourceMux.DisposeAsync().ConfigureAwait(false);
    }

    private static class Loader<T>
    {
        public static readonly Func<(NearCacheHybridCacheContender Self, string Key), CancellationToken, ValueTask<T>> Load =
            static async (state, _) =>
            {
                Interlocked.Increment(ref state.Self._sourceLoads);
                return ContenderRedis.Decode<T>(await state.Self._db!.StringGetAsync(state.Key).ConfigureAwait(false))!;
            };
    }
}
