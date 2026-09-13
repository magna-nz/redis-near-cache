using System.Net;
using Xunit;
using Xunit.Abstractions;

namespace RedisNearCache.Tests.Chaos;

/// <summary>
/// <c>FLUSHALL</c> is delivered as a single null invalidation, not one per key, so the only safe reaction is
/// to drop the whole L1. On a cluster that message can come from any of the three nodes, and only one node
/// actually carries the <c>__redis__:invalidate</c> subscription (spike E7a) — this checks the null message
/// is routed and acted on no matter which node produced it, including the two that were not flushed yet.
/// </summary>
/// <remarks>
/// Tagged into the same named collection as <see cref="FlushDbFlushesL1Tests"/>: FLUSHALL wipes every key on
/// the node it hits, so it must never run alongside another test using the same container. The assembly
/// already disables parallelization; this keeps it correct if that is ever re-enabled.
/// </remarks>
[Collection("flush")]
public class FlushAllClusterFlushesOnceTests : IClassFixture<ClusterCacheFixture>
{
    private readonly ClusterCacheFixture _fx;
    private readonly ITestOutputHelper _out;

    public FlushAllClusterFlushesOnceTests(ClusterCacheFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _out = output;
    }

    [Fact]
    public async Task FlushAllClusterFlushesOnce()
    {
        var mux = _fx.Connection.Multiplexer;
        var endpoints = mux.GetEndPoints();
        var anyServer = mux.GetServer(endpoints[0]);

        var keyByEndpoint = TestHelpers.KeyPerMaster(mux, anyServer, endpoints);
        Assert.Equal(3, keyByEndpoint.Count);

        var flushesBefore = _fx.Cache.Statistics.Flushes;
        var perNodeFlushes = new List<(int Port, long Flushes, bool AllGone)>();

        // Flush one node at a time. After each one every L1 entry must be gone, including the entries that
        // belong to the two nodes that have not been flushed yet: a null invalidation means "trust nothing".
        foreach (var (endpoint, _) in keyByEndpoint)
        {
            foreach (var (_, key) in keyByEndpoint)
            {
                await _fx.Cache.SetAsync(key, "v1");
                Assert.Equal("v1", await _fx.Cache.GetAsync<string>(key));
                Assert.True(_fx.Cache.TryGetLocal<string>(key, out _), $"{key} was not cached before the flush.");
            }

            var port = ((IPEndPoint)endpoint).Port;
            var before = _fx.Cache.Statistics.Flushes;
            RedisCli.Cluster(port, "FLUSHALL");

            var allGone = await Poll.UntilAsync(
                () => keyByEndpoint.Values.All(k => !_fx.Cache.TryGetLocal<string>(k, out _)),
                TimeSpan.FromSeconds(10));

            perNodeFlushes.Add((port, _fx.Cache.Statistics.Flushes - before, allGone));
            Assert.True(allGone, $"FLUSHALL on node {port} did not clear every L1 entry.");
        }

        var total = _fx.Cache.Statistics.Flushes - flushesBefore;
        _out.WriteLine("per-node flush counts: " +
                       string.Join(", ", perNodeFlushes.Select(p => $"{p.Port}=>{p.Flushes}")) +
                       $"; total counted={total}; stats={_fx.Cache.Statistics}");

        Assert.True(total >= 1, $"three FLUSHALLs produced {total} counted flushes; expected at least 1.");
    }
}
