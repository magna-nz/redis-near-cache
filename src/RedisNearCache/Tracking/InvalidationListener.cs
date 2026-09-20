using Microsoft.Extensions.Logging;
using RedisNearCache.Internal;
using StackExchange.Redis;

namespace RedisNearCache.Tracking;

/// <summary>
/// Subscribes to <c>__redis__:invalidate</c> on the private multiplexer and turns the server's messages into
/// events.
/// </summary>
/// <remarks>
/// The spike (E2) established the wire semantics this relies on: StackExchange.Redis splits a multi-key
/// invalidation payload into one handler call per key, so every non-null payload is exactly one key, and a
/// null payload means FLUSHDB/FLUSHALL. On a cluster the subscription only lives on one node, but redirected
/// invalidations from every node reach this handler because the library routes incoming messages by channel
/// name (E7a).
/// </remarks>
internal sealed class InvalidationListener : IInvalidationListener
{
    private static readonly RedisChannel InvalidateChannel = RedisChannel.Literal("__redis__:invalidate");

    private readonly RedisNearCacheConnection _connection;
    private readonly ILogger<InvalidationListener> _logger;
    private readonly Action<RedisChannel, RedisValue> _handler;
    private ISubscriber? _subscriber;
    private int _started;
    private int _disposed;

    public InvalidationListener(RedisNearCacheConnection connection, ILogger<InvalidationListener> logger)
    {
        _connection = connection;
        _logger = logger;
        _handler = OnMessage;
    }

    /// <inheritdoc />
    public event Action<string>? KeyInvalidated;

    /// <inheritdoc />
    public event Action? FlushAll;

    /// <summary>
    /// Subscribes to the invalidation channel. Subscribing is also what makes StackExchange.Redis open the
    /// subscriber connection that <see cref="TrackingArmer"/> uses as the REDIRECT target, so this runs first.
    /// </summary>
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) == 1, this);
        if (Interlocked.Exchange(ref _started, 1) == 1) return;

        var subscriber = _connection.Multiplexer.GetSubscriber();
        try
        {
            await subscriber.SubscribeAsync(InvalidateChannel, _handler).WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            // Not started: the facade tries again (Redis may simply have been unreachable while the application
            // started). Take back whatever StackExchange.Redis kept of the failed attempt first, so the next one does
            // not register the handler a second time and deliver every invalidation twice.
            try { subscriber.Unsubscribe(InvalidateChannel, _handler, CommandFlags.FireAndForget); }
            catch (Exception ex) { _logger.LogDebug(ex, "RedisNearCache could not withdraw a failed subscription to {Channel}", InvalidateChannel.ToString()); }
            Volatile.Write(ref _started, 0);
            throw;
        }

        _subscriber = subscriber;
        _logger.LogInformation("RedisNearCache subscribed to {Channel} as client {ClientName}", InvalidateChannel.ToString(), _connection.ClientName);
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1) return;

        var subscriber = Interlocked.Exchange(ref _subscriber, null);
        if (subscriber is null) return;

        try
        {
            await subscriber.UnsubscribeAsync(InvalidateChannel, _handler).ConfigureAwait(false);
            _logger.LogDebug("RedisNearCache unsubscribed from {Channel}", InvalidateChannel.ToString());
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "RedisNearCache could not unsubscribe from {Channel} while disposing", InvalidateChannel.ToString());
        }
    }

    /// <summary>
    /// One message from the server: a key that was invalidated, or null for FLUSHDB/FLUSHALL.
    /// Runs on a StackExchange.Redis callback thread and must never throw.
    /// </summary>
    private void OnMessage(RedisChannel channel, RedisValue value)
    {
        try
        {
            if (Volatile.Read(ref _disposed) == 1) return;

            if (value.IsNull)
            {
                _logger.LogDebug("RedisNearCache received a null invalidation (FLUSHDB/FLUSHALL)");
                FlushAll?.Invoke();
                return;
            }

            if (value.ToString() is not { Length: > 0 } key)
            {
                _logger.LogDebug("RedisNearCache received an empty invalidation payload on {Channel}; ignored", channel.ToString());
                return;
            }

            _logger.LogTrace("RedisNearCache received an invalidation for {Key}", key);
            KeyInvalidated?.Invoke(key);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "RedisNearCache failed to handle an invalidation message on {Channel}", channel.ToString());
        }
    }
}
