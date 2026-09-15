using StackExchange.Redis;

namespace RedisNearCache.Bench.Contenders;

/// <summary><see cref="ContenderKind.Plain"/>: GET/SET on a plain multiplexer. Every read is a source load.</summary>
public sealed class PlainContender : IContender
{
    private readonly ContenderSettings _settings;
    private ConnectionMultiplexer? _mux;
    private IDatabase? _db;
    private long _sourceLoads;

    public PlainContender(ContenderSettings settings) => _settings = settings;

    public ContenderKind Kind => ContenderKind.Plain;

    /// <summary>False: there is no local tier, so a local hit ratio is not applicable (reported as n/a, not 0 %).</summary>
    public bool LocalHitsCompleteSynchronously => false;

    public long SourceLoads => Interlocked.Read(ref _sourceLoads);

    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        _mux = await ContenderRedis.ConnectAsync(_settings, $"bench-{_settings.InstanceName}-plain").ConfigureAwait(false);
        _db = _mux.GetDatabase();
    }

    public ValueTask<T?> GetAsync<T>(string key, CancellationToken cancellationToken = default)
    {
        Interlocked.Increment(ref _sourceLoads);
        return ReadAsync<T>(key);
    }

    private async ValueTask<T?> ReadAsync<T>(string key) =>
        ContenderRedis.Decode<T>(await _db!.StringGetAsync(key).ConfigureAwait(false));

    public async ValueTask SetAsync<T>(string key, T value, CancellationToken cancellationToken = default) =>
        await _db!.StringSetAsync(key, ContenderRedis.Encode(value)).ConfigureAwait(false);

    public async ValueTask DisposeAsync()
    {
        if (_mux is not null) await _mux.DisposeAsync().ConfigureAwait(false);
    }
}
