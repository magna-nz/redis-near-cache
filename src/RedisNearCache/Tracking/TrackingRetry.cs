namespace RedisNearCache.Tracking;

/// <summary>
/// Retry timing shared by both tracking modes, so a change to the ladder reaches <c>Redirect</c> and
/// <c>Broadcast</c> alike: a short fast ladder for transient failures, then a slow background loop.
/// </summary>
internal static class TrackingRetry
{
    /// <summary>Delays between fast arm attempts. One fewer entry than the number of attempts.</summary>
    public static readonly TimeSpan[] Delays =
    [
        TimeSpan.FromMilliseconds(100),
        TimeSpan.FromMilliseconds(300),
        TimeSpan.FromSeconds(1),
        TimeSpan.FromSeconds(1),
    ];

    /// <summary>Interval between background retries once the fast ladder has been exhausted.</summary>
    public static readonly TimeSpan SlowInterval = TimeSpan.FromSeconds(5);
}
