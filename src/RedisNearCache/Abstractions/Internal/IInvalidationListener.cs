namespace RedisNearCache.Internal;

/// <summary>
/// Subscribes to <c>__redis__:invalidate</c> on the private multiplexer and turns the server's messages into
/// events. The server delivers one pub/sub message per invalidated key (StackExchange.Redis splits multi-key
/// payloads into one handler call per key) and a null payload for FLUSHDB/FLUSHALL.
/// </summary>
internal interface IInvalidationListener : IAsyncDisposable
{
    /// <summary>Subscribes and starts delivering events.</summary>
    Task StartAsync(CancellationToken cancellationToken);

    /// <summary>One key was invalidated by the server.</summary>
    event Action<string>? KeyInvalidated;

    /// <summary>The server asked for everything to be dropped (null payload).</summary>
    event Action? FlushAll;
}
