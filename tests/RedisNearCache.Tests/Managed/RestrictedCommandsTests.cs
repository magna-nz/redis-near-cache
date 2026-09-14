using RedisNearCache.Tests.EdgeCases;
using RedisNearCache.Tests.Resilience;
using Xunit;
using Xunit.Abstractions;

namespace RedisNearCache.Tests.Managed;

/// <summary>
/// Managed-style EMULATION of AWS ElastiCache's / Azure Managed Redis's disabled admin commands: the
/// standalone container <c>managed-up.sh</c> starts has <c>CONFIG</c>, <c>DEBUG</c>, <c>MONITOR</c>,
/// <c>SAVE</c>, <c>BGSAVE</c>, <c>BGREWRITEAOF</c>, <c>SHUTDOWN</c>, <c>REPLICAOF</c>/<c>SLAVEOF</c>,
/// <c>SYNC</c>/<c>PSYNC</c>, <c>MIGRATE</c> and <c>MODULE</c> renamed to nothing - the shape of what those
/// services do to their customers. <c>CLIENT</c>, <c>INFO</c>, <c>SUBSCRIBE</c> and <c>CLUSTER</c> - every
/// command <see cref="RedisNearCache.Internal.RedisNearCacheConnection"/> and
/// <see cref="RedisNearCache.Tracking.TrackingArmer"/> actually issue - are left alone, so a failure here
/// would be a real incompatibility and not an artifact of the emulation. Nothing in this file claims to test
/// ElastiCache or Azure Managed Redis themselves; see the class doc on
/// <see cref="ExternalManagedEndpointTests"/> for the opt-in test against a real one.
/// </summary>
public class RestrictedCommandsTests
{
    private readonly ITestOutputHelper _out;

    public RestrictedCommandsTests(ITestOutputHelper output) => _out = output;

    [Fact]
    public async Task StartsArmsReadsInvalidatesAndReArmsAfterASubscriberKillWithAdminCommandsDisabled()
    {
        // Precondition: CONFIG really is refused against this container, so everything below proves something.
        var refused = ManagedSupport.Restricted("CONFIG", "GET", "maxmemory");
        Assert.Contains("unknown command", refused, StringComparison.OrdinalIgnoreCase);
        _out.WriteLine($"precondition: CONFIG GET maxmemory -> {refused}");

        EdgeCaseProvider? handle = null;
        var key = TestHelpers.Key("restricted");
        try
        {
            handle = await EdgeCaseSupport.BuildAsync(ManagedSupport.RestrictedConnectionString);
            var cache = handle.Cache;

            // Ready completed and the master got armed without ever needing a disabled command.
            Assert.NotEmpty(handle.Armer.RedirectTargets);
            _out.WriteLine($"armed with CONFIG etc. disabled: {handle.Armer.RedirectTargets.Count} redirect target(s)");

            await cache.SetAsync(key, "v1");
            Assert.True(await TestHelpers.ReadUntilCachedAsync(cache, key, "v1"), $"{key} was not cached against the restricted container.");

            var hitsBefore = cache.Statistics.Hits;
            Assert.Equal("v1", await cache.GetAsync<string>(key));
            Assert.Equal(hitsBefore + 1, cache.Statistics.Hits);

            // A foreign write must still invalidate L1.
            ManagedSupport.Restricted("SET", key, "v2");
            var evicted = await Poll.UntilAsync(() => !cache.TryGetLocal<string>(key, out _), TimeSpan.FromSeconds(5));
            Assert.True(evicted, "an external write did not evict L1 against the restricted container.");
            Assert.Equal("v2", await cache.GetAsync<string>(key));

            // ElastiCache allows CLIENT KILL even though CONFIG etc. are gone (CLIENT is never renamed);
            // the subscriber-reconnect re-arm path must still work with everything else disabled.
            var clientList = ManagedSupport.Restricted("CLIENT", "LIST");
            var subscriberIds = ResilienceSupport.SubscriberIdsNamed(clientList, handle.Connection.ClientName);
            Assert.NotEmpty(subscriberIds);
            var oldRedirectId = handle.Armer.RedirectTargets.Values.Single();
            var rearmsBefore = cache.Statistics.Rearms;

            foreach (var id in subscriberIds) ManagedSupport.Restricted("CLIENT", "KILL", "ID", id.ToString());

            var rearmed = await Poll.UntilAsync(
                () => cache.Statistics.Rearms > rearmsBefore
                      && handle.Armer.RedirectTargets.Count > 0
                      && handle.Armer.RedirectTargets.Values.Single() != oldRedirectId,
                TimeSpan.FromSeconds(15));
            Assert.True(rearmed, $"the subscriber was not re-armed with a new redirect id after CLIENT KILL. stats={cache.Statistics}");
            _out.WriteLine($"re-armed with redirect id {handle.Armer.RedirectTargets.Values.Single()} (was {oldRedirectId})");

            Assert.True(await TestHelpers.ReadUntilCachedAsync(cache, key, "v2"), "the key did not re-cache after the subscriber re-arm.");
            ManagedSupport.Restricted("SET", key, "v3");
            var evictedAfterKill = await Poll.UntilAsync(() => !cache.TryGetLocal<string>(key, out _), TimeSpan.FromSeconds(5));
            Assert.True(evictedAfterKill, "invalidations stopped working after the subscriber connection was killed and re-armed.");
            Assert.Equal("v3", await cache.GetAsync<string>(key));
            _out.WriteLine($"restricted container: {cache.Statistics}");
        }
        finally
        {
            if (handle is not null) await handle.DisposeAsync();
            ManagedSupport.Restricted("DEL", key);
        }
    }
}
