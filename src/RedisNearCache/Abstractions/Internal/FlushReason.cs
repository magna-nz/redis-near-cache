namespace RedisNearCache.Internal;

/// <summary>
/// Why the whole of L1 was thrown away. One member per <c>FlushLocal</c> call site in the facade; the total
/// (<see cref="RedisNearCacheStatistics.Flushes"/>) counts them all, and the breakdown is published as the
/// <c>reason</c> tag on <c>redisnearcache.flushes</c>, never as public API.
/// </summary>
internal enum FlushReason
{
    /// <summary>A null invalidation from the server: something ran <c>FLUSHDB</c> or <c>FLUSHALL</c>.</summary>
    ServerFlush,

    /// <summary>
    /// Tracking was (re)armed on an endpoint for any reason other than <see cref="ArmReason.Initial"/>. Includes
    /// <see cref="ArmReason.Promoted"/>, which is not a re-arm but still flushes once, for the entries read from the
    /// master that was demoted.
    /// </summary>
    Rearm,

    /// <summary>Tracking on an endpoint became unreliable, so nothing already in L1 can be trusted.</summary>
    TrackingLost,

    /// <summary>
    /// An endpoint is no longer a master of the deployment. Entries read from it are protected by nothing now, so it
    /// flushes even when tracking there was never reported lost.
    /// </summary>
    EndpointRemoved,

    /// <summary>The application called <see cref="IRedisNearCache.EvictAllLocal"/>.</summary>
    Manual,
}
