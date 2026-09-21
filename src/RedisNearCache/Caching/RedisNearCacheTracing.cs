using System.Diagnostics;
using System.Net;
using RedisNearCache.Internal;

namespace RedisNearCache.Caching;

/// <summary>
/// The spans RedisNearCache starts, on an <see cref="ActivitySource"/> named
/// <see cref="RedisNearCacheStatistics.ActivitySourceName"/> - deliberately the same string as the meter's, so one
/// instrumentation scope covers both signals. One source per cache instance, disposed with the cache, exactly as
/// <see cref="RedisNearCacheMetrics"/>'s <see cref="System.Diagnostics.Metrics.Meter"/> is.
/// </summary>
/// <remarks>
/// Two spans, and no more: <see cref="ReadSpanName"/> around the Redis round trip of a MISS, and
/// <see cref="ArmSpanName"/> around the arm of one endpoint. Both wrap work that is already going to the network, so
/// the span is never the expensive part of what it measures.
/// <para>
/// Nothing here is reached on a path an L1 hit can take. <c>RedisNearCache.GetStoredBytesAsync</c> returns the
/// hit before it calls <see cref="StartRead"/> at all, which matters because
/// <see cref="ActivitySource.StartActivity(string, ActivityKind)"/> is not free: it is several field reads and a
/// branch with no listener, and an allocation with one. Everything else follows the same rule as the metrics -
/// there is no flush span, because a flush happens on the invalidation-handler path, which must touch nothing
/// observability-related (DESIGN.md), and is already visible through <c>redisnearcache.flushes</c> and its
/// <c>reason</c> tag.
/// </para>
/// <para>
/// A span carries values no metric here is allowed to: the full cache key, and the endpoint address. That is not an
/// inconsistency to tidy up. A metric tag becomes a permanent time series per distinct value, which is why DESIGN.md
/// keeps addresses out of them; a span attribute is stored with the one trace that recorded it and aggregated by
/// nobody, so high cardinality costs that trace's storage and nothing else. It is also the only thing that makes a
/// span worth having here.
/// </para>
/// </remarks>
internal sealed class RedisNearCacheTracing : IDisposable
{
    /// <summary>The activity source name; the same constant the public <see cref="RedisNearCacheStatistics"/> exposes to callers.</summary>
    public const string ActivitySourceName = RedisNearCacheStatistics.ActivitySourceName;

    /// <summary>
    /// The span around the Redis round trip of a read that MISSED L1. Named like the instruments
    /// (<c>redisnearcache.*</c>) rather than after OpenTelemetry's database conventions, for the reason DESIGN.md
    /// gives for the instrument names.
    /// </summary>
    public const string ReadSpanName = "redisnearcache.read";

    /// <summary>
    /// The span around arming <c>CLIENT TRACKING</c> on one endpoint - the initial arm and every re-arm; which one it
    /// was is the <see cref="ArmReasonTag"/> attribute, so a slow arm is attributable without two span names.
    /// </summary>
    public const string ArmSpanName = "redisnearcache.arm";

    /// <summary>The instance tags every span carries, the same two keys and values every measurement carries.</summary>
    public const string ClientNameTag = RedisNearCacheMetrics.ClientNameTag;

    /// <inheritdoc cref="RedisNearCacheMetrics.InstanceTag"/>
    public const string InstanceTag = RedisNearCacheMetrics.InstanceTag;

    /// <summary>The full key as sent to Redis, key namespace included (see the cardinality note on the type).</summary>
    public const string KeyTag = "rnc.key";

    /// <summary>True when the reply was handed to L1, false when it was served to the caller and nothing was stored.</summary>
    public const string StoredTag = "rnc.stored";

    /// <summary>
    /// Why nothing was stored, present only when <see cref="StoredTag"/> is false. This is the one thing a span can
    /// tell an operator that no counter can: which read, by key, was served correctly and cached anyway not.
    /// </summary>
    public const string NotStoredReasonTag = "rnc.not_stored_reason";

    /// <summary>The endpoint being armed. Allowed on a span, never on a metric tag (see the cardinality note on the type).</summary>
    public const string EndpointTag = "rnc.endpoint";

    /// <summary>
    /// The <see cref="ArmReason"/> member name. Its values are exactly those of the <c>reason</c> tag on
    /// <c>redisnearcache.rearms</c>, but the key is prefixed and qualified: the read span carries a reason of its own
    /// (<see cref="NotStoredReasonTag"/>), and a bare <c>reason</c> across two span names would be ambiguous in a
    /// trace search.
    /// </summary>
    public const string ArmReasonTag = "rnc.arm_reason";

    /// <summary>
    /// The client id of the subscriber connection the server is being told to redirect invalidations to. Present only
    /// on a <c>REDIRECT</c>-mode arm: a <c>BCAST</c> socket tracks for itself, so there is no redirect target, and the
    /// attribute is left off rather than emitted as a placeholder a query would have to know to ignore.
    /// </summary>
    public const string RedirectClientIdTag = "rnc.redirect_client_id";

    /// <summary>The status description of an arm whose <c>CLIENT TRACKINGINFO</c> read back did not confirm it.</summary>
    public const string ArmNotVerified = "CLIENT TRACKINGINFO did not confirm the arm";

    // Resolved once: Enum.GetName allocates, and this is read on every arm.
    private static readonly string[] ArmReasonNames =
        Array.ConvertAll(Enum.GetValues<ArmReason>(), r => Enum.GetName(r) ?? r.ToString());

    /// <summary>
    /// The <see cref="ReadNotStored"/> member names as they are emitted, lower_snake_case like every other attribute
    /// value here rather than the enum's own spelling, and resolved once for the reason above.
    /// </summary>
    private static readonly string[] NotStoredReasonNames =
    [
        "key_missing",
        "race_discard",
        "ttl_unknown",
        "caching_disabled",
        "outside_key_prefixes",
        "caching_resumed_mid_read",
    ];

    private readonly ActivitySource _source;
    private readonly KeyValuePair<string, object?>[] _identity;

    public RedisNearCacheTracing(string clientName, string? instanceName = null)
    {
        _identity = instanceName is null
            ? [new KeyValuePair<string, object?>(ClientNameTag, clientName)]
            : [new KeyValuePair<string, object?>(ClientNameTag, clientName), new KeyValuePair<string, object?>(InstanceTag, instanceName)];
        // The same version as the meter's, from the same place: one instrumentation scope, one version.
        _source = new ActivitySource(ActivitySourceName, RedisNearCacheMetrics.InstrumentationVersion);
    }

    /// <summary>
    /// Starts the span around one read's Redis round trip. Null - and nothing allocated, no timestamp taken, no
    /// <see cref="Activity.Current"/> looked at - unless an <see cref="ActivityListener"/> is listening to this source
    /// and samples the span in. Called only once the read is known to be going to Redis; see the type's remarks.
    /// </summary>
    public Activity? StartRead(string fullKey)
    {
        var activity = _source.StartActivity(ReadSpanName, ActivityKind.Client);
        if (activity is null) return null;

        // The FULL key, as sent to Redis. High cardinality on purpose: this is a span, not a metric tag - see the
        // cardinality note on the type before "fixing" this by copying the rule the instruments follow.
        activity.SetTag(KeyTag, fullKey);
        return Identify(activity);
    }

    /// <summary>
    /// Starts the span around arming one endpoint - one attempt of it - whichever mode is in use, so a trace query
    /// does not have to know which: in <c>REDIRECT</c> mode <c>CLIENT TRACKING OFF</c>, <c>CLIENT TRACKING ON
    /// REDIRECT</c> and the <c>CLIENT TRACKINGINFO</c> that verifies them; in <c>BCAST</c> mode the handshake on the
    /// fresh socket (<c>HELLO 3</c>, <c>CLIENT ID</c>), <c>CLIENT TRACKING ON BCAST</c> and the same verification.
    /// Null when nobody is listening, as <see cref="StartRead"/>.
    /// </summary>
    /// <param name="endPoint">The endpoint being armed.</param>
    /// <param name="reason">Why, as the <see cref="ArmReasonTag"/> attribute.</param>
    /// <param name="redirectClientId">
    /// The redirect target, or null in <c>BCAST</c> mode, where <see cref="RedirectClientIdTag"/> is then omitted
    /// entirely.
    /// </param>
    public Activity? StartArm(EndPoint endPoint, ArmReason reason, long? redirectClientId = null)
    {
        var activity = _source.StartActivity(ArmSpanName, ActivityKind.Client);
        if (activity is null) return null;

        activity.SetTag(EndpointTag, endPoint.ToString());
        activity.SetTag(ArmReasonTag, Name(reason));
        if (redirectClientId is { } id) activity.SetTag(RedirectClientIdTag, id);
        return Identify(activity);
    }

    /// <summary>The reply was handed to L1. (Whether L1 then kept it is <c>redisnearcache.l1.store_refusals</c>'s business.)</summary>
    public static void RecordStored(Activity? activity) => activity?.SetTag(StoredTag, true);

    /// <summary>The reply was served to the caller and not stored, for <paramref name="reason"/>.</summary>
    public static void RecordNotStored(Activity? activity, ReadNotStored reason)
    {
        if (activity is null) return;
        activity.SetTag(StoredTag, false);
        activity.SetTag(NotStoredReasonTag, Name(reason));
    }

    /// <summary>
    /// Records that the traced region threw. The caller rethrows untouched: nothing here swallows or wraps an
    /// exception, and nothing here counts one either - a serializer throw is counted where it is caught
    /// (<c>redisnearcache.serializer_failures</c>) and happens outside the read span anyway.
    /// </summary>
    public static void RecordFailure(Activity? activity, Exception error) =>
        activity?.SetStatus(ActivityStatusCode.Error, error.Message);

    /// <summary>Records an arm that neither threw nor took: the server did not report the redirect back.</summary>
    public static void RecordArmNotVerified(Activity? activity) =>
        activity?.SetStatus(ActivityStatusCode.Error, ArmNotVerified);

    /// <summary>The <see cref="ArmReason"/> name as the <see cref="ArmReasonTag"/> attribute carries it.</summary>
    public static string Name(ArmReason reason) =>
        (uint)reason < (uint)ArmReasonNames.Length ? ArmReasonNames[(int)reason] : reason.ToString();

    /// <summary>The <see cref="ReadNotStored"/> name as the <see cref="NotStoredReasonTag"/> attribute carries it.</summary>
    public static string Name(ReadNotStored reason) =>
        (uint)reason < (uint)NotStoredReasonNames.Length ? NotStoredReasonNames[(int)reason] : reason.ToString();

    /// <summary>
    /// Adds the instance tags. A <c>foreach</c> over the array built in the constructor, so a span costs no
    /// allocation beyond the <see cref="Activity"/> itself, and only when something is listening.
    /// </summary>
    private Activity Identify(Activity activity)
    {
        foreach (var tag in _identity)
        {
            activity.SetTag(tag.Key, tag.Value);
        }

        return activity;
    }

    public void Dispose() => _source.Dispose();
}

/// <summary>
/// Why a Redis reply that reached the facade was not put in L1. Reported by the read span only; every one of these
/// is a cache that is serving correctly and populating nothing, which is otherwise invisible per key.
/// </summary>
/// <remarks>The member order is the order <see cref="RedisNearCacheTracing"/> spells the emitted names in.</remarks>
internal enum ReadNotStored
{
    /// <summary>The key does not exist in Redis: there was nothing to store, which is not a refusal to store.</summary>
    KeyMissing,

    /// <summary>
    /// An invalidation for the key arrived while the read was in flight, or <c>PTTL</c> said the key had already gone:
    /// the reply may be stale, so it is served and dropped. Counted by <c>redisnearcache.race_discards</c>.
    /// </summary>
    RaceDiscarded,

    /// <summary>
    /// The remaining TTL could not be read at all, so with <c>RespectServerTtl</c> on there is no cap to store the
    /// entry under. A lasting one of these is what <c>redisnearcache.ttl_cap.abandoned</c> eventually latches.
    /// </summary>
    TtlUnknown,

    /// <summary>
    /// The cache is in pass-through - starting, degraded, or waiting on a lost endpoint - so nothing may be stored.
    /// <c>redisnearcache.coherent</c> is 0 and <c>redisnearcache.pass_through.seconds</c> is running.
    /// </summary>
    CachingDisabled,

    /// <summary>The key is outside <c>KeyPrefixes</c>: deliberately neither tracked by the server nor stored here.</summary>
    OutsideKeyPrefixes,

    /// <summary>
    /// The read went out in pass-through (so it carries no TTL to cap by) and caching came back while it was in
    /// flight. Storing it for the full <c>L1MaxAge</c> would ignore <c>RespectServerTtl</c>, so it is left to the next
    /// read. Rare, and only ever transient.
    /// </summary>
    CachingResumedMidRead,
}
