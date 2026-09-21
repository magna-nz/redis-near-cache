using System.Collections.Concurrent;
using System.Diagnostics;
using RedisNearCache.Caching;
using RedisNearCache.Internal;
using Xunit;

namespace RedisNearCache.Tests.EdgeCases;

/// <summary>
/// The two spans <see cref="RedisNearCacheTracing"/> starts, driven by real reads and a real invalidation against
/// the standalone container. <c>tests/RedisNearCache.UnitTests/TracingTests.cs</c> proves the same code paths
/// against a fake server (including that an L1 hit never even calls <c>StartActivity</c>); this file is the
/// live-Redis complement, not a copy: it proves the same outward claims hold end to end, plus the one thing the
/// unit suite could not - that a real invalidation racing a real read is reported correctly, and that a real
/// Broadcast-mode arm against a live server produces the same span shape as Redirect.
/// </summary>
public class TracingTests
{
    /// <summary>
    /// Collects the finished <c>redisnearcache.*</c> spans that match a predicate on the <c>rnc.client_name</c> tag.
    /// Other EdgeCases test classes run in parallel in the same process and publish to a source of the same name, so
    /// every assertion below is scoped to one cache instance exactly as <c>MetricsTests.Collect</c> scopes to one
    /// client name.
    /// </summary>
    private sealed class SpanProbe : IDisposable
    {
        private readonly Func<string?, bool> _matchesClient;
        private readonly ActivityListener _listener;
        private readonly ConcurrentQueue<Activity> _finished = new();

        private SpanProbe(Func<string?, bool> matchesClient)
        {
            _matchesClient = matchesClient;
            _listener = new ActivityListener
            {
                ShouldListenTo = source => source.Name == RedisNearCacheStatistics.ActivitySourceName,
                // Everything sampled in, so a test asserting a span exists can never fail because sampling dropped it.
                Sample = static (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
                ActivityStopped = OnStopped,
            };
            ActivitySource.AddActivityListener(_listener);
        }

        /// <summary>Filters on the exact client name of an already-built cache.</summary>
        public static SpanProbe ForClientName(string clientName) => new(name => name == clientName);

        /// <summary>
        /// Filters on a prefix chosen by the test and handed to <see cref="RedisNearCacheOptions.ClientNamePrefix"/>
        /// before the cache is built. Needed for the arm spans: the initial arm happens during <c>Ready</c>, inside
        /// <c>EdgeCaseSupport.BuildAsync</c>, before the real client name (prefix + a generated GUID) exists to filter
        /// on - so the probe is registered first, and matches on the prefix it told the cache to use.
        /// </summary>
        public static SpanProbe ForClientNamePrefix(string prefix) =>
            new(name => name is not null && name.StartsWith(prefix, StringComparison.Ordinal));

        public IReadOnlyList<Activity> Finished(string? name = null) =>
            _finished.Where(a => name is null || a.OperationName == name).ToArray();

        public Activity Single(string name) => Assert.Single(Finished(name));

        private void OnStopped(Activity activity)
        {
            if (activity.Source.Name != RedisNearCacheStatistics.ActivitySourceName) return;
            if (!_matchesClient((string?)activity.GetTagItem(RedisNearCacheTracing.ClientNameTag))) return;
            _finished.Enqueue(activity);
        }

        public void Dispose() => _listener.Dispose();
    }

    // --- the read span, against a live server ---------------------------------------------------------------

    [Fact]
    public async Task AMissAgainstRealRedisProducesOneStoredReadSpanCarryingTheFullKey()
    {
        // A namespace, so the assertion below is meaningfully about the FULL key as sent to Redis, not merely an
        // identity check that would also pass if the span carried the wrong thing.
        var ns = $"trc-{Guid.NewGuid():N}:";
        var handle = await EdgeCaseSupport.BuildAsync(StandaloneCacheFixture.ConnectionString, o => o.KeyNamespace = ns);
        var key = TestHelpers.Key("trace-miss");
        var fullKey = ns + key;
        try
        {
            RedisCli.Standalone("SET", fullKey, "v1");
            using var probe = SpanProbe.ForClientName(handle.Connection.ClientName);

            Assert.Equal("v1", await handle.Cache.GetAsync<string>(key));

            var span = probe.Single(RedisNearCacheTracing.ReadSpanName);
            Assert.Equal(ActivityKind.Client, span.Kind);
            Assert.Equal(fullKey, (string?)span.GetTagItem(RedisNearCacheTracing.KeyTag));
            Assert.Equal(true, (bool?)span.GetTagItem(RedisNearCacheTracing.StoredTag));
            Assert.Null(span.GetTagItem(RedisNearCacheTracing.NotStoredReasonTag));
            Assert.Equal(ActivityStatusCode.Unset, span.Status);
        }
        finally
        {
            await handle.DisposeAsync();
            RedisCli.Standalone("DEL", fullKey);
        }
    }

    [Fact]
    public async Task AnL1HitAgainstRealRedisProducesNoSpan()
    {
        var handle = await EdgeCaseSupport.BuildAsync(StandaloneCacheFixture.ConnectionString);
        var key = TestHelpers.Key("trace-hit");
        try
        {
            RedisCli.Standalone("SET", key, "v1");
            using var probe = SpanProbe.ForClientName(handle.Connection.ClientName);

            // The miss that populates L1: exactly one read span.
            Assert.True(await TestHelpers.ReadUntilCachedAsync(handle.Cache, key, "v1"));
            Assert.Single(probe.Finished(RedisNearCacheTracing.ReadSpanName));

            // The hit: served from L1, no Redis round trip, so no new span - proven against a live server, which is a
            // different claim from the unit suite's proof that StartActivity is never even called.
            Assert.Equal("v1", await handle.Cache.GetAsync<string>(key));
            Assert.Equal("v1", await handle.Cache.GetAsync<string>(key));

            Assert.Single(probe.Finished(RedisNearCacheTracing.ReadSpanName));
        }
        finally
        {
            await handle.DisposeAsync();
            RedisCli.Standalone("DEL", key);
        }
    }

    /// <summary>
    /// A real foreign write invalidates the key while the re-read that would otherwise re-cache it is still on the
    /// wire. Established from <c>src/RedisNearCache/Caching/RedisNearCache.cs</c>: the store branch's condition is
    /// <c>cap == TimeSpan.Zero || _inflight.WasInvalidated(key, token)</c>; the value here has no TTL (a plain SET),
    /// so <c>cap</c> is null, and the only way to land in the "not stored" branch is
    /// <see cref="ReadNotStored.RaceDiscarded"/> - the sole reason for which the key is not simply absent
    /// (<see cref="ReadNotStored.KeyMissing"/>: it exists), not a TTL problem
    /// (<see cref="ReadNotStored.TtlUnknown"/>: PTTL succeeds), not a prefix or pass-through condition
    /// (<see cref="ReadNotStored.OutsideKeyPrefixes"/>, <see cref="ReadNotStored.CachingDisabled"/>,
    /// <see cref="ReadNotStored.CachingResumedMidRead"/>: none of those apply to a coherent cache with no
    /// <c>KeyPrefixes</c> configured). <see cref="RedisNearCacheOptions.TestHooks"/>'s
    /// <c>AfterRedisReadBeforeStore</c> is used only to synchronize timing - to guarantee the real write's real
    /// invalidation has actually landed before the read resumes - never to fake the invalidation itself, which is
    /// what makes this the live-Redis complement of the unit suite's <c>ScriptedListener</c> version rather than a
    /// copy of it.
    /// </summary>
    [Fact]
    public async Task AForeignWriteThatInvalidatesAKeyWhileTheReadIsInFlightProducesARaceDiscardSpan()
    {
        var key = TestHelpers.Key("trace-race");
        IRedisNearCache? cacheRef = null;
        var handle = await EdgeCaseSupport.BuildAsync(StandaloneCacheFixture.ConnectionString, o =>
        {
            o.TestHooks.AfterRedisReadBeforeStore = async _ =>
            {
                var before = cacheRef!.Statistics.Invalidations;
                RedisCli.Standalone("SET", key, "v2");
                Assert.True(
                    await Poll.UntilAsync(() => cacheRef!.Statistics.Invalidations > before, TimeSpan.FromSeconds(5)),
                    "the real foreign write's invalidation never landed within the deadline.");
            };
        });
        cacheRef = handle.Cache;
        try
        {
            RedisCli.Standalone("SET", key, "v1");
            using var probe = SpanProbe.ForClientName(handle.Connection.ClientName);

            // Served (the stale-but-in-flight reply), and rightly not stored: the read must not populate L1 with it.
            Assert.Equal("v1", await handle.Cache.GetAsync<string>(key));
            Assert.False(handle.Cache.TryGetLocal<string>(key, out _), "a race-discarded reply must not populate L1.");

            var span = probe.Single(RedisNearCacheTracing.ReadSpanName);
            Assert.Equal(false, (bool?)span.GetTagItem(RedisNearCacheTracing.StoredTag));
            Assert.Equal(RedisNearCacheTracing.Name(ReadNotStored.RaceDiscarded), (string?)span.GetTagItem(RedisNearCacheTracing.NotStoredReasonTag));
            Assert.Equal(1, handle.Cache.Statistics.RaceDiscards);
        }
        finally
        {
            await handle.DisposeAsync();
            RedisCli.Standalone("DEL", key);
        }
    }

    // --- the arm span, against a live server -----------------------------------------------------------------

    /// <summary>
    /// The INITIAL arm, not a deliberate re-arm: it is the arm a cache stuck in pass-through never finishes, and the
    /// one every deployment does at least once. Observing it requires the listener registered before the cache is
    /// built (the arm happens inside <c>Ready</c>, which <c>EdgeCaseSupport.BuildAsync</c> awaits) - done here via
    /// <see cref="RedisNearCacheOptions.ClientNamePrefix"/>, since the real client name is not known until after the
    /// connection exists.
    /// </summary>
    [Fact]
    public async Task TheInitialArmAgainstRealRedisProducesAnArmSpanCarryingTheEndpointAndRedirectId()
    {
        var prefix = "trc-arm-" + Guid.NewGuid().ToString("N");
        using var probe = SpanProbe.ForClientNamePrefix(prefix);
        var handle = await EdgeCaseSupport.BuildAsync(StandaloneCacheFixture.ConnectionString, o => o.ClientNamePrefix = prefix);
        try
        {
            var span = probe.Single(RedisNearCacheTracing.ArmSpanName);
            Assert.Equal(ActivityKind.Client, span.Kind);
            Assert.Equal(nameof(ArmReason.Initial), (string?)span.GetTagItem(RedisNearCacheTracing.ArmReasonTag));
            var endpoint = handle.Connection.Multiplexer.GetEndPoints()[0];
            Assert.Equal(endpoint.ToString(), (string?)span.GetTagItem(RedisNearCacheTracing.EndpointTag));
            var redirectId = Assert.Single(handle.Armer.RedirectTargets).Value;
            Assert.Equal(redirectId, (long?)span.GetTagItem(RedisNearCacheTracing.RedirectClientIdTag));
            Assert.Equal(ActivityStatusCode.Unset, span.Status);
            Assert.True(handle.Cache.IsCoherent);
        }
        finally
        {
            await handle.DisposeAsync();
        }
    }

    /// <summary>
    /// The same initial arm, but in <see cref="TrackingMode.Broadcast"/>: BCAST support for the arm span was just
    /// added and had unit coverage only. The one mode-specific thing about the span is asserted here -
    /// <see cref="RedisNearCacheTracing.RedirectClientIdTag"/> is present in Redirect (above) and absent in Broadcast,
    /// because a BCAST socket tracks for itself and there is no redirect target to name.
    /// </summary>
    [Fact]
    public async Task ABroadcastArmAgainstRealRedisProducesAnArmSpanWithoutARedirectClientId()
    {
        var prefix = "trc-bcarm-" + Guid.NewGuid().ToString("N");
        using var probe = SpanProbe.ForClientNamePrefix(prefix);
        var handle = await EdgeCaseSupport.BuildAsync(StandaloneCacheFixture.ConnectionString, o =>
        {
            o.ClientNamePrefix = prefix;
            o.TrackingMode = TrackingMode.Broadcast;
            o.KeyPrefixes.Add("trc-bc:");
        });
        try
        {
            var span = probe.Single(RedisNearCacheTracing.ArmSpanName);
            Assert.Equal(ActivityKind.Client, span.Kind);
            Assert.Equal(nameof(ArmReason.Initial), (string?)span.GetTagItem(RedisNearCacheTracing.ArmReasonTag));
            var endpoint = handle.Connection.Multiplexer.GetEndPoints()[0];
            Assert.Equal(endpoint.ToString(), (string?)span.GetTagItem(RedisNearCacheTracing.EndpointTag));
            // The one mode-specific difference: absent, not a placeholder.
            Assert.Null(span.GetTagItem(RedisNearCacheTracing.RedirectClientIdTag));
            Assert.Equal(ActivityStatusCode.Unset, span.Status);
            Assert.True(handle.Cache.IsCoherent);
        }
        finally
        {
            await handle.DisposeAsync();
        }
    }
}
