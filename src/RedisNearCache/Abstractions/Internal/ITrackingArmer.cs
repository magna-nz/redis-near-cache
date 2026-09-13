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

    /// <summary>Raised when tracking on an endpoint is known to be lost (connection failed) and not yet re-armed.</summary>
    event Action<EndPoint>? TrackingLost;

    /// <summary>Current redirect client id per endpoint, for diagnostics and tests.</summary>
    IReadOnlyDictionary<EndPoint, long> RedirectTargets { get; }

    /// <summary>Re-arms one endpoint now.</summary>
    Task RearmAsync(EndPoint endPoint, ArmReason reason, CancellationToken cancellationToken);

    /// <summary>Re-arms every connected master now.</summary>
    Task RearmAllAsync(ArmReason reason, CancellationToken cancellationToken);
}
