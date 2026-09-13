using System.Net;
using Xunit;
using Xunit.Abstractions;

namespace RedisNearCache.Tests.EdgeCases;

/// <summary>
/// Losing every connection on every master at once, not just one node (see
/// <see cref="RedisNearCache.Tests.Chaos.KillAllOurConnectionsRepeatedlyTests"/> for the single-node,
/// repeated case). Each node's armer state is independent, so this proves the recovery is not accidentally
/// serialized behind a single global lock and that all three nodes really do come back.
/// </summary>
public class ClusterRecoveryTests : IClassFixture<ClusterCacheFixture>
{
    private readonly ClusterCacheFixture _fx;
    private readonly ITestOutputHelper _out;

    public ClusterRecoveryTests(ClusterCacheFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _out = output;
    }

    [Fact]
    public async Task ClusterReadsAfterAllNodesKilledRecover()
    {
        var mux = _fx.Connection.Multiplexer;
        var endpoints = mux.GetEndPoints();
        var anyServer = mux.GetServer(endpoints[0]);
        var keyByEndpoint = TestHelpers.KeyPerMaster(mux, anyServer, endpoints);
        Assert.Equal(3, keyByEndpoint.Count);

        // Seed and cache one key per node before the carnage.
        foreach (var (_, key) in keyByEndpoint)
        {
            await _fx.Cache.SetAsync(key, "v1");
            Assert.Equal("v1", await _fx.Cache.GetAsync<string>(key));
            Assert.True(_fx.Cache.TryGetLocal<string>(key, out _));
        }

        var rearmsBefore = _fx.Cache.Statistics.Rearms;

        foreach (var endpoint in endpoints)
        {
            var port = ((IPEndPoint)endpoint).Port;
            var victims = EdgeCaseSupport.ClientIdsNamed(
                RedisCli.Cluster(port, "CLIENT", "LIST"), _fx.Connection.ClientName);
            _out.WriteLine($"killing {victims.Count} connection(s) named {_fx.Connection.ClientName} on port {port}");
            Assert.NotEmpty(victims);
            foreach (var id in victims) RedisCli.Cluster(port, "CLIENT", "KILL", "ID", id.ToString());
        }

        var recovered = await Poll.UntilAsync(
            () => _fx.Cache.Statistics.Rearms > rearmsBefore
                  && _fx.Armer.RedirectTargets.Count == 3
                  && endpoints.All(e => _fx.Armer.RedirectTargets.ContainsKey(e)),
            TimeSpan.FromSeconds(20));
        _out.WriteLine($"redirect targets after recovery: {_fx.Armer.RedirectTargets.Count}; stats={_fx.Cache.Statistics}");
        Assert.True(recovered, $"not all three masters were re-armed after every connection was killed. stats={_fx.Cache.Statistics}");

        // Killing all six connections flushed L1 (every non-Initial arm clears it): re-seed, then prove
        // reads hit and invalidations still work, per node.
        foreach (var (endpoint, key) in keyByEndpoint)
        {
            await Chaos.ChaosSupport.WithReconnectRetryAsync(async () => await _fx.Cache.SetAsync(key, "v1"));
            Assert.Equal("v1", await Chaos.ChaosSupport.WithReconnectRetryAsync(async () => await _fx.Cache.GetAsync<string>(key)));
            Assert.True(_fx.Cache.TryGetLocal<string>(key, out _), $"key for {endpoint} was not cached after recovery.");
            var hitsBefore = _fx.Cache.Statistics.Hits;
            Assert.Equal("v1", await _fx.Cache.GetAsync<string>(key));
            Assert.Equal(hitsBefore + 1, _fx.Cache.Statistics.Hits);
        }

        foreach (var (endpoint, key) in keyByEndpoint)
        {
            var port = ((IPEndPoint)endpoint).Port;
            RedisCli.Cluster(port, "SET", key, "v2");
            var evicted = await Poll.UntilAsync(() => !_fx.Cache.TryGetLocal<string>(key, out _), TimeSpan.FromSeconds(10));
            Assert.True(evicted, $"node {port} did not deliver an invalidation after the mass reconnect.");
        }
    }
}
