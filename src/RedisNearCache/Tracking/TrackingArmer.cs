#pragma warning disable CS0067 // stub
using System.Net;
using Microsoft.Extensions.Logging;
using RedisNearCache.Internal;

namespace RedisNearCache.Tracking;

// STUB: replaced by the Tracking implementer. Constructor signature is the contract used by DI.
internal sealed class TrackingArmer : ITrackingArmer
{
    public TrackingArmer(RedisNearCacheConnection connection, ILogger<TrackingArmer> logger) { }
    public event Action<TrackingArmedEvent>? Armed;
    public event Action<EndPoint>? TrackingLost;
    public IReadOnlyDictionary<EndPoint, long> RedirectTargets => throw new NotImplementedException();
    public Task StartAsync(CancellationToken cancellationToken) => throw new NotImplementedException();
    public Task RearmAsync(EndPoint endPoint, ArmReason reason, CancellationToken cancellationToken) => throw new NotImplementedException();
    public Task RearmAllAsync(ArmReason reason, CancellationToken cancellationToken) => throw new NotImplementedException();
    public ValueTask DisposeAsync() => default;
}
