using Xunit;

namespace RedisNearCache.Tests.Broadcast;

/// <summary>
/// Key prefix shared by every Broadcast-suite fixture and test: <see cref="RedisNearCacheOptions.KeyPrefixes"/> is
/// armed with exactly this prefix, so keys generated through <see cref="New"/> are the ones the server actually
/// broadcasts invalidations for. Deliberately distinct from <see cref="TestHelpers.Key"/>'s <c>t:</c> prefix, so a
/// key built with <see cref="TestHelpers.Key"/> is a convenient "outside the prefix" key for these tests.
/// </summary>
internal static class BroadcastKey
{
    public const string Prefix = "bc:";

    public static string New(string suffix) => $"{Prefix}{Guid.NewGuid():N}:{suffix}";
}

/// <summary>
/// Broadcast-mode fixture against the standalone container. A thin subclass of <see cref="StandaloneCacheFixture"/>
/// that overrides only <see cref="StandaloneCacheFixture.Configure"/>, arming
/// <see cref="RedisNearCacheOptions.TrackingMode"/> as <see cref="TrackingMode.Broadcast"/> and
/// <see cref="RedisNearCacheOptions.KeyPrefixes"/> with <see cref="BroadcastKey.Prefix"/>. Everything else
/// (<c>ConnectionString</c>, <c>Server()</c>, registration/disposal) is inherited unchanged.
/// </summary>
public sealed class BroadcastStandaloneCacheFixture : StandaloneCacheFixture
{
    protected override void Configure(RedisNearCacheOptions options)
    {
        options.TrackingMode = TrackingMode.Broadcast;
        options.KeyPrefixes.Add(BroadcastKey.Prefix);
    }
}

/// <summary>Same idea as <see cref="BroadcastStandaloneCacheFixture"/> but against the 3-master cluster.</summary>
public sealed class BroadcastClusterCacheFixture : ClusterCacheFixture
{
    protected override void Configure(RedisNearCacheOptions options)
    {
        options.TrackingMode = TrackingMode.Broadcast;
        options.KeyPrefixes.Add(BroadcastKey.Prefix);
    }
}
