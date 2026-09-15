using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Caching.StackExchangeRedis;
using Microsoft.Extensions.DependencyInjection;
using StackExchange.Redis;
using ZiggyCreatures.Caching.Fusion;
using ZiggyCreatures.Caching.Fusion.Backplane.StackExchangeRedis;
using ZiggyCreatures.Caching.Fusion.Serialization.SystemTextJson;

namespace RedisNearCache.Bench.Contenders;

/// <summary>
/// <see cref="ContenderKind.FusionCache"/>: FusionCache with its in-process L1, the official
/// <c>Microsoft.Extensions.Caching.StackExchangeRedis</c> <c>RedisCache</c> as L2 (entries under
/// <c>bench:l2:fusioncache:</c>, shared by every instance), the System.Text.Json serializer, and the official Redis
/// backplane. Writes made through <see cref="SetAsync{T}"/> are published on the backplane; foreign writes are not
/// seen until <c>Duration</c> elapses.
/// </summary>
public sealed class FusionCacheContender : IContender, ILocalTierPeek
{
    public const string L2Prefix = ContenderRedis.OwnedKeyPrefix + "fusioncache:";

    private static readonly FusionCacheEntryOptions PeekOptions = new()
    {
        SkipDistributedCacheRead = true,
        SkipDistributedCacheWrite = true,
        SkipBackplaneNotifications = true,
    };

    private readonly ContenderSettings _settings;
    private ConnectionMultiplexer? _mux;
    private IDatabase? _db;
    private ServiceProvider? _provider;
    private IFusionCache? _fusion;
    private long _sourceLoads;

    public FusionCacheContender(ContenderSettings settings) => _settings = settings;

    public ContenderKind Kind => ContenderKind.FusionCache;

    public bool LocalHitsCompleteSynchronously => true;

    public long SourceLoads => Interlocked.Read(ref _sourceLoads);

    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        // One multiplexer for source-of-truth reads, the L2 and the backplane, as an application would share it.
        _mux = await ContenderRedis.ConnectAsync(_settings, $"bench-{_settings.InstanceName}-fusion").ConfigureAwait(false);
        _db = _mux.GetDatabase();
        var mux = _mux;

        var services = new ServiceCollection();
        services.AddFusionCache()
            .WithOptions(o =>
            {
                // Changed from the default (false): without it the first backplane notifications of a run can be lost
                // while the subscription is still being set up in the background. Affects startup only.
                o.WaitForInitialBackplaneSubscribe = true;
            })
            .WithDefaultEntryOptions(new FusionCacheEntryOptions
            {
                Duration = _settings.Ttl,
                // Required by MemoryCacheOptions.SizeLimit below (every entry must declare a size).
                Size = 1,
            })
            // Changed from the default (an unbounded MemoryCache) so every kind's local tier has the same entry limit.
            .WithMemoryCache(new MemoryCache(new MemoryCacheOptions { SizeLimit = _settings.LocalSizeLimit }))
            .WithSerializer(new FusionCacheSystemTextJsonSerializer())
            .WithDistributedCache(new RedisCache(new RedisCacheOptions
            {
                InstanceName = L2Prefix,
                ConnectionMultiplexerFactory = () => Task.FromResult<IConnectionMultiplexer>(mux),
            }))
            .WithBackplane(new RedisBackplane(new RedisBackplaneOptions
            {
                ConnectionMultiplexerFactory = () => Task.FromResult<IConnectionMultiplexer>(mux),
            }));
        _provider = services.BuildServiceProvider();
        _fusion = _provider.GetRequiredService<IFusionCache>();

        // Resolving IFusionCache does not start the backplane; the first operation does (and, with
        // WaitForInitialBackplaneSubscribe, waits for the subscription). Do that here, not inside the measured run.
        await _fusion.TryGetAsync<string>(ContenderRedis.OwnedKeyPrefix + "fusioncache-warmup", token: cancellationToken).ConfigureAwait(false);
    }

    public ValueTask<T?> GetAsync<T>(string key, CancellationToken cancellationToken = default) =>
        _fusion!.GetOrSetAsync<T?>(
            key,
            async (_, _) =>
            {
                Interlocked.Increment(ref _sourceLoads);
                return ContenderRedis.Decode<T>(await _db!.StringGetAsync(key).ConfigureAwait(false));
            },
            token: cancellationToken);

    public async ValueTask SetAsync<T>(string key, T value, CancellationToken cancellationToken = default)
    {
        await _db!.StringSetAsync(key, ContenderRedis.Encode(value)).ConfigureAwait(false);
        await _fusion!.SetAsync(key, value, token: cancellationToken).ConfigureAwait(false);
    }

    public bool TryPeekLocal<T>(string key, out T? value)
    {
        var maybe = _fusion!.TryGet<T>(key, PeekOptions);
        value = maybe.HasValue ? maybe.Value : default;
        return maybe.HasValue;
    }

    public async ValueTask DisposeAsync()
    {
        if (_provider is not null) await _provider.DisposeAsync().ConfigureAwait(false);
        if (_mux is not null) await _mux.DisposeAsync().ConfigureAwait(false);
    }
}
