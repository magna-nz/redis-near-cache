using Microsoft.Extensions.Caching.Memory;
using StackExchange.Redis;

namespace RedisNearCache.Bench.Contenders;

/// <summary>
/// <see cref="ContenderKind.MemoryCacheTtl"/>: the hand-rolled near cache. <c>IMemoryCache</c> with an absolute TTL in
/// front of a plain GET; nothing invalidates it, so a value can be served up to <see cref="ContenderSettings.Ttl"/> after
/// it changed. Deliberately naive, like the code it stands for (no stampede protection, no in-flight race handling).
/// </summary>
public sealed class MemoryCacheTtlContender : IContender, ILocalTierPeek
{
    private readonly ContenderSettings _settings;
    private readonly MemoryCache _cache;
    private readonly MemoryCacheEntryOptions _entryOptions;
    private ConnectionMultiplexer? _mux;
    private IDatabase? _db;
    private long _sourceLoads;

    public MemoryCacheTtlContender(ContenderSettings settings)
    {
        _settings = settings;
        _cache = new MemoryCache(new MemoryCacheOptions { SizeLimit = settings.LocalSizeLimit });
        _entryOptions = new MemoryCacheEntryOptions { Size = 1, AbsoluteExpirationRelativeToNow = settings.Ttl };
    }

    public ContenderKind Kind => ContenderKind.MemoryCacheTtl;

    public bool LocalHitsCompleteSynchronously => true;

    public long SourceLoads => Interlocked.Read(ref _sourceLoads);

    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        _mux = await ContenderRedis.ConnectAsync(_settings, $"bench-{_settings.InstanceName}-memttl").ConfigureAwait(false);
        _db = _mux.GetDatabase();
    }

    public ValueTask<T?> GetAsync<T>(string key, CancellationToken cancellationToken = default)
    {
        if (_cache.TryGetValue(key, out var cached) && cached is T typed) return new ValueTask<T?>(typed);
        return LoadAsync<T>(key);
    }

    private async ValueTask<T?> LoadAsync<T>(string key)
    {
        Interlocked.Increment(ref _sourceLoads);
        var value = ContenderRedis.Decode<T>(await _db!.StringGetAsync(key).ConfigureAwait(false));
        if (value is not null) _cache.Set(key, value, _entryOptions);
        return value;
    }

    public async ValueTask SetAsync<T>(string key, T value, CancellationToken cancellationToken = default)
    {
        await _db!.StringSetAsync(key, ContenderRedis.Encode(value)).ConfigureAwait(false);
        _cache.Set(key, value, _entryOptions);
    }

    public bool TryPeekLocal<T>(string key, out T? value)
    {
        if (_cache.TryGetValue(key, out var cached) && cached is T typed)
        {
            value = typed;
            return true;
        }

        value = default;
        return false;
    }

    public async ValueTask DisposeAsync()
    {
        _cache.Dispose();
        if (_mux is not null) await _mux.DisposeAsync().ConfigureAwait(false);
    }
}
