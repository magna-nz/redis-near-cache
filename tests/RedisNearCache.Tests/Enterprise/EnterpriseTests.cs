using Microsoft.Extensions.DependencyInjection;
using RedisNearCache.Internal;
using RedisNearCache.Tests.Chaos;
using RedisNearCache.Tests.EdgeCases;
using StackExchange.Redis;
using Xunit;

namespace RedisNearCache.Tests.Enterprise;

/// <summary>
/// Runs only against a real Redis Enterprise-based proxy (<c>RNC_ENTERPRISE_REDIS</c>, e.g. <c>localhost:12000</c>
/// from <c>./enterprise-up.sh</c>). Foreign writes go through a second, completely separate
/// <see cref="ConnectionMultiplexer"/> to the same connection string, per the test plan (simpler than shelling
/// into the container and works the same way <see cref="Managed.ExternalManagedEndpointTests"/> proves foreign
/// writes against a real external endpoint).
/// </summary>
[Collection("enterprise")]
public class EnterpriseTests
{
    private static string ConnectionString => Environment.GetEnvironmentVariable("RNC_ENTERPRISE_REDIS")!;

    private static string UniquePrefix() => $"ent:{Guid.NewGuid():N}:";

    /// <summary>E1: arms (RedirectTargets non-empty) and a foreign write evicts.</summary>
    [EnterpriseFact]
    public async Task ArmsAndForeignWriteEvicts()
    {
        var prefix = UniquePrefix();
        EdgeCaseProvider? handle = null;
        ForeignClient? foreign = null;
        try
        {
            handle = await EdgeCaseSupport.BuildAsync(ConnectionString, o =>
            {
                o.TrackingMode = TrackingMode.Broadcast;
                o.KeyPrefixes.Add(prefix);
            });
            Assert.NotEmpty(handle.Armer.RedirectTargets);

            foreign = await ForeignClient.ConnectAsync(ConnectionString);

            var key = prefix + Guid.NewGuid().ToString("N");
            await handle.Cache.SetAsync(key, "v1");
            Assert.True(await TestHelpers.ReadUntilCachedAsync(handle.Cache, key, "v1"), $"{key} was not cached against the Enterprise endpoint.");

            await foreign.Db.StringSetAsync(key, "v2");

            var evicted = await Poll.UntilAsync(() => !handle.Cache.TryGetLocal<string>(key, out _), TimeSpan.FromSeconds(15));
            Assert.True(evicted, "a foreign write did not evict L1 within the deadline.");
            Assert.Equal("v2", await handle.Cache.GetAsync<string>(key));
        }
        finally
        {
            if (handle is not null) await handle.DisposeAsync();
            if (foreign is not null) await foreign.DisposeAsync();
        }
    }

    /// <summary>
    /// E1b: the raw-bytes members through the proxy, with a serializer that fails if used: the bytes are stored as
    /// given, served from L1, and a foreign write evicts them.
    /// </summary>
    [EnterpriseFact]
    public async Task BytesRoundTripAndForeignWriteEvicts()
    {
        var prefix = UniquePrefix();
        EdgeCaseProvider? handle = null;
        ForeignClient? foreign = null;
        try
        {
            handle = await EdgeCaseSupport.BuildAsync(ConnectionString, o =>
            {
                o.TrackingMode = TrackingMode.Broadcast;
                o.KeyPrefixes.Add(prefix);
                o.Serializer = new RefusingSerializer();
            });
            foreign = await ForeignClient.ConnectAsync(ConnectionString);

            var key = prefix + Guid.NewGuid().ToString("N");
            var payload = RawBytes.Payload();
            await handle.Cache.SetBytesAsync(key, payload);

            Assert.Equal(payload, (byte[]?)await foreign.Db.StringGetAsync(key));
            Assert.True(await RawBytes.ReadUntilCachedAsync(handle.Cache, key, payload, TimeSpan.FromSeconds(15)),
                $"{key} was not cached against the Enterprise endpoint.");

            await foreign.Db.StringSetAsync(key, new byte[] { 1, 2 });

            Assert.True(await RawBytes.ReadUntilCachedAsync(handle.Cache, key, [1, 2], TimeSpan.FromSeconds(15)),
                "a foreign write did not evict the cached bytes within the deadline.");
        }
        finally
        {
            if (handle is not null) await handle.DisposeAsync();
            if (foreign is not null) await foreign.DisposeAsync();
        }
    }

    /// <summary>E2: a foreign FLUSHDB flushes L1.</summary>
    [EnterpriseFact]
    public async Task ForeignFlushDbFlushesL1()
    {
        var prefix = UniquePrefix();
        EdgeCaseProvider? handle = null;
        ForeignClient? foreign = null;
        try
        {
            handle = await EdgeCaseSupport.BuildAsync(ConnectionString, o =>
            {
                o.TrackingMode = TrackingMode.Broadcast;
                o.KeyPrefixes.Add(prefix);
            });

            var key = prefix + Guid.NewGuid().ToString("N");
            await handle.Cache.SetAsync(key, "v1");
            Assert.True(await TestHelpers.ReadUntilCachedAsync(handle.Cache, key, "v1"), $"{key} was not cached against the Enterprise endpoint.");

            foreign = await ForeignClient.ConnectAsync(ConnectionString, allowAdmin: true);
            var flushesBefore = handle.Cache.Statistics.Flushes;
            await foreign.Multiplexer.GetServer(foreign.Multiplexer.GetEndPoints()[0]).FlushDatabaseAsync();

            // Wait for the flush itself: in Broadcast mode the SetAsync above echoes back as a push and can evict the key
            // before the FLUSHDB's null invalidation arrives, so "the key left L1" alone does not prove the flush.
            var flushed = await Poll.UntilAsync(() => handle.Cache.Statistics.Flushes > flushesBefore, TimeSpan.FromSeconds(15));
            Assert.True(flushed, "a foreign FLUSHDB did not flush L1 (no null invalidation handled) within the deadline.");
            Assert.False(handle.Cache.TryGetLocal<string>(key, out _), "the key was still in L1 after the flush.");
        }
        finally
        {
            if (handle is not null) await handle.DisposeAsync();
            if (foreign is not null) await foreign.DisposeAsync();
        }
    }

    /// <summary>
    /// E3: killing the broadcast socket's push connection (via <c>CLIENT KILL ID</c> issued through the foreign
    /// multiplexer) must produce the same TrackingLost -&gt; Armed(PushConnectionRestored) sequence as against the
    /// OSS containers, and invalidations must resume afterwards. If the proxy rejects <c>CLIENT KILL</c>, the
    /// call below throws and the test fails with that real error rather than being silently skipped.
    /// </summary>
    [EnterpriseFact]
    public async Task KillingThePushConnectionRearmsAndResumesInvalidations()
    {
        var prefix = UniquePrefix();
        EdgeCaseProvider? handle = null;
        ForeignClient? foreign = null;
        try
        {
            handle = await EdgeCaseSupport.BuildAsync(ConnectionString, o =>
            {
                o.TrackingMode = TrackingMode.Broadcast;
                o.KeyPrefixes.Add(prefix);
            });
            Assert.NotEmpty(handle.Armer.RedirectTargets);
            var endpoint = handle.Armer.RedirectTargets.Keys.First();
            var oldClientId = handle.Armer.RedirectTargets[endpoint];

            foreign = await ForeignClient.ConnectAsync(ConnectionString, allowAdmin: true);

            var sawTrackingLost = false;
            var sawPushConnectionRestored = false;
            handle.Armer.TrackingLost += ep =>
            {
                if (Equals(ep, endpoint)) sawTrackingLost = true;
            };
            handle.Armer.Armed += e =>
            {
                if (Equals(e.EndPoint, endpoint) && e.Reason == ArmReason.PushConnectionRestored) sawPushConnectionRestored = true;
            };

            // Intentionally not wrapped in try/catch: if the proxy rejects CLIENT KILL ID, this throws and the
            // test fails with the real error instead of being silently skipped.
            await foreign.Db.ExecuteAsync("CLIENT", "KILL", "ID", oldClientId.ToString());

            var rearmed = await Poll.UntilAsync(
                () => sawTrackingLost
                      && sawPushConnectionRestored
                      && handle.Armer.RedirectTargets.TryGetValue(endpoint, out var current)
                      && current != oldClientId,
                TimeSpan.FromSeconds(10));
            Assert.True(rearmed, "expected TrackingLost then an Armed event with reason PushConnectionRestored and a changed client id.");

            var key = prefix + Guid.NewGuid().ToString("N");
            await handle.Cache.SetAsync(key, "v1");
            Assert.True(await TestHelpers.ReadUntilCachedAsync(handle.Cache, key, "v1"), "key was not re-cached after the re-arm.");

            await foreign.Db.StringSetAsync(key, "v2");
            var evicted = await Poll.UntilAsync(() => !handle.Cache.TryGetLocal<string>(key, out _), TimeSpan.FromSeconds(15));
            Assert.True(evicted, "invalidations did not resume after the broadcast socket was re-armed.");
        }
        finally
        {
            if (handle is not null) await handle.DisposeAsync();
            if (foreign is not null) await foreign.DisposeAsync();
        }
    }

    /// <summary>
    /// E4: Redirect mode (the default) against the same Enterprise endpoint fails startup, because its proxy
    /// hides the subscriber connection from CLIENT LIST and rejects REDIRECT.
    /// </summary>
    [EnterpriseFact]
    public async Task RedirectModeFailsStartupWithABroadcastModeHint()
    {
        var services = new ServiceCollection();
        services.AddRedisNearCache(ConnectionString); // default TrackingMode.Redirect
        var provider = services.BuildServiceProvider();
        try
        {
            var cache = provider.GetRequiredService<IRedisNearCache>();
            var ex = await Assert.ThrowsAnyAsync<Exception>(() => cache.Ready);
            var messages = string.Join(" | ", Flatten(ex).Select(e => e.Message));
            Assert.Contains("TrackingMode.Broadcast", messages);
        }
        finally
        {
            await provider.DisposeAsync();
        }
    }

    /// <summary>Every message in an exception's chain, including <see cref="AggregateException.InnerExceptions"/>.</summary>
    private static IEnumerable<Exception> Flatten(Exception ex)
    {
        yield return ex;
        if (ex is AggregateException aggregate)
        {
            foreach (var inner in aggregate.InnerExceptions)
            {
                foreach (var e in Flatten(inner)) yield return e;
            }
        }
        else if (ex.InnerException is { } single)
        {
            foreach (var e in Flatten(single)) yield return e;
        }
    }
}
