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
    private readonly System.Collections.Concurrent.ConcurrentDictionary<EndPoint, bool> _lostEndpoints = new();
    private int _disposed;

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
        _armer.EndpointRemoved += OnEndpointRemoved;

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

    // Ordering rule: mark the in-flight tracker FIRST, then touch L1. A read that stores its reply between the
    // two steps re-checks the tracker after storing (see GetAsync), so mark-then-clear closes every
    // interleaving; clear-then-mark leaves the "store after clear, re-check before mark" window open.

    private void OnKeyInvalidated(string key)
    {
        _inflight.MarkInvalidated(key);
        _l1.Remove(key);
        Statistics.Invalidation();
    }

    /// <summary>The one whole-cache flush. Every path that cannot trust L1 comes through here.</summary>
    private void FlushLocal()
    {
        _inflight.MarkAllInvalidated();
        _options.TestHooks.InsideFlushHandler?.Invoke();
        _l1.Clear();
        Statistics.Flush();
    }

    private void OnFlushAll() => FlushLocal();

    private void OnArmed(TrackingArmedEvent e)
    {
        if (e.Reason != ArmReason.Initial)
        {
            FlushLocal();
            Statistics.Rearm();
        }
        // Re-enable caching only AFTER the flush, so no concurrent read can hit an entry the flush discards.
        _lostEndpoints.TryRemove(e.EndPoint, out _);
        _degraded = false;
    }

    private void OnTrackingLost(EndPoint endPoint)
    {
        // Nothing can be trusted while tracking is down on any endpoint: stop populating L1 first, then flush.
        _lostEndpoints[endPoint] = true;
        FlushLocal();
    }

    private void OnEndpointRemoved(EndPoint endPoint)
    {
        // The node left the deployment; it will never be re-armed, so it must not keep us in pass-through.
        if (_lostEndpoints.TryRemove(endPoint, out _)) FlushLocal();
    }

    /// <summary>L1 may only be read or populated while tracking is believed to be armed everywhere.</summary>
    private bool CachingEnabled => !_degraded && _lostEndpoints.IsEmpty;

    private void ThrowIfDisposed()
    {
        if (Volatile.Read(ref _disposed) == 1) throw new ObjectDisposedException(nameof(RedisNearCache));
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
        ThrowIfDisposed();
        if (!Ready.IsCompletedSuccessfully)
        {
            if (Ready.IsCompleted)
            {
                // Faulted or cancelled startup: degrade to a pass-through (every read goes to Redis, nothing is
                // cached) until the armer's background retry raises Armed. No exception per call.
                _degraded = true;
            }
            else
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
                    _degraded = true; // already logged by StartAsync
                }
            }
        }

        if (CachingEnabled && _l1.TryGet(key, out var cached))
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

            if (CachingEnabled && MatchesPrefixes(key))
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
        ThrowIfDisposed();
        byte[] bytes = _options.Serializer.Serialize(value);
        var db = _connection.Multiplexer.GetDatabase();
        // Tracking is armed with NOLOOP, so the server will not echo this write back as an invalidation.
        // Any read of this key in flight before or during the write must therefore be discarded by us:
        // mark before the write (reads already on the wire) and after it (reads that raced the send).
        InvalidateLocal(key);
        try
        {
            await db.StringSetAsync(key, bytes, expiry, When.Always).WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            // Even if the reply never came back (timeout, cancellation) the write may have landed.
            InvalidateLocal(key);
        }
    }

    /// <summary>Marks the key for any read in flight, then drops the L1 copy. Order matters; see the note above the handlers.</summary>
    private void InvalidateLocal(string key)
    {
        _inflight.MarkInvalidated(key);
        _l1.Remove(key);
    }

    public async ValueTask<bool> RemoveAsync(string key, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        var db = _connection.Multiplexer.GetDatabase();
        InvalidateLocal(key);
        try
        {
            return await db.KeyDeleteAsync(key).WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            InvalidateLocal(key);
        }
    }

    public void EvictLocal(string key) => _l1.Remove(key);

    public bool TryGetLocal<T>(string key, out T? value)
    {
        if (CachingEnabled && Volatile.Read(ref _disposed) == 0 && _l1.TryGet(key, out var bytes))
        {
            value = _options.Serializer.Deserialize<T>(bytes);
            return true;
        }

        value = default;
        return false;
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
        {
            return;
        }

        _listener.KeyInvalidated -= OnKeyInvalidated;
        _listener.FlushAll -= OnFlushAll;
        _armer.Armed -= OnArmed;
        _armer.TrackingLost -= OnTrackingLost;
        _armer.EndpointRemoved -= OnEndpointRemoved;

        // Disposing the armer cancels any arm in flight, so a startup racing this dispose finishes promptly.
        await _armer.DisposeAsync().ConfigureAwait(false);
        try { await Ready.ConfigureAwait(false); } catch { /* faulted or cancelled startup is fine here */ }
        await _listener.DisposeAsync().ConfigureAwait(false);
        _l1.Dispose();
        await _connection.DisposeAsync().ConfigureAwait(false);
    }
}
