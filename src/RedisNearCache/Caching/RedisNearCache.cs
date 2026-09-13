using System.Net;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RedisNearCache.Internal;
using StackExchange.Redis;

namespace RedisNearCache.Caching;

/// <summary>
/// The public facade: L1 (<see cref="L1Cache"/>) in front of Redis, kept coherent by
/// <see cref="ITrackingArmer"/> and <see cref="IInvalidationListener"/>.
/// </summary>
internal sealed class RedisNearCache : IRedisNearCache
{
    private readonly RedisNearCacheConnection _connection;
    private readonly ITrackingArmer _armer;
    private readonly IInvalidationListener _listener;
    private readonly RedisNearCacheOptions _options;
    private readonly ILogger<RedisNearCache> _logger;

    private readonly L1Cache _l1;
    private readonly InFlightTracker _inflight;
    private volatile bool _degraded;

    public RedisNearCacheStatistics Statistics { get; } = new();

    public Task Ready { get; }

    public RedisNearCache(
        RedisNearCacheConnection connection,
        ITrackingArmer armer,
        IInvalidationListener listener,
        IOptions<RedisNearCacheOptions> options,
        ILogger<RedisNearCache> logger)
    {
        _connection = connection;
        _armer = armer;
        _listener = listener;
        _options = options.Value;
        _logger = logger;

        _l1 = new L1Cache(_options);
        _inflight = new InFlightTracker();

        _listener.KeyInvalidated += OnKeyInvalidated;
        _listener.FlushAll += OnFlushAll;
        _armer.Armed += OnArmed;
        _armer.TrackingLost += OnTrackingLost;

        Ready = StartAsync();
    }

    private async Task StartAsync()
    {
        try
        {
            // Listener first: the subscriber connection must exist and be subscribed before tracking is
            // armed, otherwise an invalidation could be redirected before anyone is listening for it.
            await _listener.StartAsync(CancellationToken.None).ConfigureAwait(false);
            await _armer.StartAsync(CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "RedisNearCache failed to start tracking; the near cache will not be coherent.");
            throw;
        }
    }

    private void OnKeyInvalidated(string key)
    {
        _l1.Remove(key);
        _inflight.MarkInvalidated(key);
        Statistics.Invalidation();
    }

    private void OnFlushAll()
    {
        _l1.Clear();
        _inflight.MarkAllInvalidated();
        Statistics.Flush();
    }

    private void OnArmed(TrackingArmedEvent e)
    {
        if (e.Reason != ArmReason.Initial)
        {
            _l1.Clear();
            _inflight.MarkAllInvalidated();
            Statistics.Flush();
            Statistics.Rearm();
        }
    }

    private void OnTrackingLost(EndPoint endPoint)
    {
        // Conservative: nothing can be trusted while tracking is down on this endpoint.
        _l1.Clear();
        _inflight.MarkAllInvalidated();
        Statistics.Flush();
    }

    private bool MatchesPrefixes(string key)
    {
        var prefixes = _options.KeyPrefixes;
        if (prefixes.Count == 0)
        {
            return true;
        }

        foreach (var prefix in prefixes)
        {
            if (key.StartsWith(prefix, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    public async ValueTask<T?> GetAsync<T>(string key, CancellationToken cancellationToken = default)
    {
        try
        {
            await Ready.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch
        {
            // Tracking could not be armed. Degrade to a pass-through: every read goes to Redis, nothing is
            // cached, so we can never serve stale data. The failure was already logged by StartAsync.
            _degraded = true;
        }

        if (!_degraded && _l1.TryGet(key, out var cached))
        {
            Statistics.Hit();
            return _options.Serializer.Deserialize<T>(cached);
        }

        Statistics.Miss();
        long token = _inflight.Begin(key);
        try
        {
            var db = _connection.Multiplexer.GetDatabase();
            RedisValue value = await db.StringGetAsync(key).WaitAsync(cancellationToken).ConfigureAwait(false);

            var afterRead = _options.TestHooks.AfterRedisReadBeforeStore;
            if (afterRead is not null)
            {
                await afterRead(key).ConfigureAwait(false);
            }

            byte[]? bytes = (byte[]?)value;
            if (bytes is null)
            {
                return default;
            }

            if (!_degraded && MatchesPrefixes(key))
            {
                if (_inflight.WasInvalidated(key, token))
                {
                    Statistics.RaceDiscard();
                }
                else
                {
                    _l1.Set(key, bytes);
                    // Re-check after the store: an invalidation that landed between the check above and the
                    // Set would have found nothing to remove, so remove it ourselves.
                    if (_inflight.WasInvalidated(key, token))
                    {
                        _l1.Remove(key);
                        Statistics.RaceDiscard();
                    }
                }
            }

            return _options.Serializer.Deserialize<T>(bytes);
        }
        finally
        {
            _inflight.End(key, token);
        }
    }

    public async ValueTask SetAsync<T>(string key, T value, TimeSpan? expiry = null, CancellationToken cancellationToken = default)
    {
        byte[] bytes = _options.Serializer.Serialize(value);
        var db = _connection.Multiplexer.GetDatabase();
        // Tracking is armed with NOLOOP, so the server will not echo this write back as an invalidation.
        // Any read of this key in flight before or during the write must therefore be discarded by us:
        // mark before the write (reads already on the wire) and after it (reads that raced the send).
        _inflight.MarkInvalidated(key);
        _l1.Remove(key);
        await db.StringSetAsync(key, bytes, expiry, When.Always).WaitAsync(cancellationToken).ConfigureAwait(false);
        _inflight.MarkInvalidated(key);
        _l1.Remove(key);
    }

    public async ValueTask<bool> RemoveAsync(string key, CancellationToken cancellationToken = default)
    {
        var db = _connection.Multiplexer.GetDatabase();
        _inflight.MarkInvalidated(key);
        _l1.Remove(key);
        bool removed = await db.KeyDeleteAsync(key).WaitAsync(cancellationToken).ConfigureAwait(false);
        _inflight.MarkInvalidated(key);
        _l1.Remove(key);
        return removed;
    }

    public void EvictLocal(string key) => _l1.Remove(key);

    public bool TryGetLocal<T>(string key, out T? value)
    {
        if (_l1.TryGet(key, out var bytes))
        {
            value = _options.Serializer.Deserialize<T>(bytes);
            return true;
        }

        value = default;
        return false;
    }

    public async ValueTask DisposeAsync()
    {
        _listener.KeyInvalidated -= OnKeyInvalidated;
        _listener.FlushAll -= OnFlushAll;
        _armer.Armed -= OnArmed;
        _armer.TrackingLost -= OnTrackingLost;

        await _armer.DisposeAsync().ConfigureAwait(false);
        await _listener.DisposeAsync().ConfigureAwait(false);
        _l1.Dispose();
        await _connection.DisposeAsync().ConfigureAwait(false);
    }
}
