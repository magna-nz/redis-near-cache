using System.Net;

namespace RedisNearCache.Internal;

/// <summary>Why tracking was (re)armed on an endpoint.</summary>
internal enum ArmReason
{
    /// <summary>First arm after connect.</summary>
    Initial,
    /// <summary>The interactive connection to this endpoint reconnected; the server had dropped tracking state.</summary>
    InteractiveRestored,
    /// <summary>The subscriber connection reconnected with a new client id; the redirect target had to be re-pointed.</summary>
    SubscriptionRestored,
    /// <summary>Requested explicitly (tests, diagnostics).</summary>
    Manual,
    /// <summary>A master appeared after a cluster configuration change; slots may have moved, so L1 is flushed.</summary>
    TopologyChanged,
    /// <summary>A background retry succeeded on an endpoint whose earlier arm had failed; L1 is flushed.</summary>
    Recovered,
    /// <summary>
    /// A replica that was pre-armed while it was a replica is now a master. Its tracking has been on since before any
    /// read could reach it, so no re-arm (and no pass-through gap) is needed; L1 is still flushed once, for the
    /// entries that were read from the demoted master.
    /// </summary>
    Promoted,
}

/// <summary>Raised after CLIENT TRACKING ON REDIRECT succeeded on one endpoint.</summary>
internal readonly record struct TrackingArmedEvent(EndPoint EndPoint, long RedirectClientId, ArmReason Reason);

/// <summary>
/// Owns the CLIENT TRACKING lifecycle on the private multiplexer.
/// For every master endpoint: find this multiplexer's subscriber connection on that node via CLIENT LIST
/// (matched by client name and the PubSubSubscriber flag), then issue CLIENT TRACKING OFF followed by
/// CLIENT TRACKING ON REDIRECT &lt;subscriber id&gt; over that node's interactive connection.
/// Re-arms on ConnectionRestored for both connection types, because the spike showed that an interactive
/// reconnect silently turns tracking off, and a subscriber reconnect silently changes the redirect id.
/// </summary>
internal interface ITrackingArmer : IAsyncDisposable
{
    /// <summary>Arms every connected master and subscribes to the multiplexer's connection events.</summary>
    Task StartAsync(CancellationToken cancellationToken);

    /// <summary>Raised after each successful arm, initial or re-arm. Consumers flush L1 for any reason other than Initial.</summary>
    event Action<TrackingArmedEvent>? Armed;

    /// <summary>
    /// Raised when tracking on an endpoint is known or about to be unreliable: its connection failed, or a
    /// non-initial arm attempt is starting. Consumers stop populating L1 until <see cref="Armed"/> for that endpoint.
    /// </summary>
    event Action<EndPoint>? TrackingLost;

    /// <summary>Raised when an endpoint is no longer a master of this deployment; consumers forget it entirely.</summary>
    event Action<EndPoint>? EndpointRemoved;

    /// <summary>Current redirect client id per armed master endpoint, for diagnostics and tests.</summary>
    IReadOnlyDictionary<EndPoint, long> RedirectTargets { get; }

    /// <summary>
    /// Redirect client id per pre-armed replica. A replica is armed ahead of time so that a failover that promotes
    /// it needs no re-arm and no flush; the entry is dropped when a connection to it fails.
    /// </summary>
    IReadOnlyDictionary<EndPoint, long> ReplicaRedirectTargets { get; }

    /// <summary>Re-arms one endpoint now.</summary>
    Task RearmAsync(EndPoint endPoint, ArmReason reason, CancellationToken cancellationToken);

    /// <summary>Re-arms every connected master now.</summary>
    Task RearmAllAsync(ArmReason reason, CancellationToken cancellationToken);
}
