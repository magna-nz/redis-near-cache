#pragma warning disable CS0067 // stub
using Microsoft.Extensions.Logging;
using RedisNearCache.Internal;

namespace RedisNearCache.Tracking;

// STUB: replaced by the Tracking implementer. Constructor signature is the contract used by DI.
internal sealed class InvalidationListener : IInvalidationListener
{
    public InvalidationListener(RedisNearCacheConnection connection, ILogger<InvalidationListener> logger) { }
    public event Action<string>? KeyInvalidated;
    public event Action? FlushAll;
    public Task StartAsync(CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask DisposeAsync() => default;
}
