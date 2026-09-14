using System.Net;
using RedisNearCache.Tests.EdgeCases;
using RedisNearCache.Tests.Resilience;
using Xunit;
using Xunit.Abstractions;

namespace RedisNearCache.Tests.Managed;

/// <summary>
/// Managed-style EMULATION of an ElastiCache / Azure Managed Redis OSS-cluster endpoint: the cluster
/// container <c>managed-up.sh</c> starts announces <c>localhost</c> as its preferred endpoint type, so
/// <c>CLUSTER SLOTS</c>/<c>CLUSTER SHARDS</c> (and any <c>MOVED</c> reply) hand back a DNS hostname instead
/// of an IP - exactly what a managed cluster-mode endpoint does. Nothing here claims to test ElastiCache or
/// Azure Managed Redis themselves; it reproduces the one property of them (hostname-based routing) that
/// could plausibly break <see cref="RedisNearCache.Tracking.TrackingArmer"/>, which stores its redirect
/// targets keyed by <see cref="EndPoint"/> and must treat a <see cref="DnsEndPoint"/> exactly like the
/// <see cref="IPEndPoint"/> every other cluster test in this repo uses.
///
/// Key-to-master lookups here deliberately do NOT use <see cref="TestHelpers.KeyForEndPoint"/>/
/// <see cref="TestHelpers.KeyPerMaster"/>: those resolve a slot's owner through
/// <c>IServer.ClusterNodes()?.GetBySlot(slot).EndPoint</c>, and StackExchange.Redis's
/// <c>ClusterConfiguration</c> always parses the bare IP out of <c>CLUSTER NODES</c> there, never the
/// announced hostname, so that <c>.EndPoint</c> never equals one of our <see cref="DnsEndPoint"/> masters and
/// the shared helper finds nothing (verified: it throws "could not find a key hashing to node ..." against
/// this container). That is a limitation of the shared test helper's IP-endpoint assumption, not of
/// RedisNearCache - <see cref="RedisNearCache.Tracking.TrackingArmer"/> never calls
/// <c>ClusterNodes()</c>/<c>GetBySlot</c> - so <see cref="ManagedSupport.KeyForPort"/>/
/// <see cref="ManagedSupport.KeyPerMasterPort"/> look the slot owner up directly from <c>CLUSTER NODES</c>
/// text instead.
///
/// No class fixture: <c>managed-up.sh</c> skips creating this container entirely on a redis:6.x image (see
/// <see cref="SkipOnRedis6ImageFact"/>), and an <c>IClassFixture</c> would try to connect before a skipped
/// test's Skip is honoured.
/// </summary>
public class HostnameAnnouncingClusterTests
{
    private readonly ITestOutputHelper _out;

    public HostnameAnnouncingClusterTests(ITestOutputHelper output) => _out = output;

    [SkipOnRedis6ImageFact]
    public async Task AllThreeMastersAreArmedAndInvalidatePerNode()
    {
        // Precondition: the cluster really is announcing hostnames, so a pass below proves something and is
        // not just an accident of the default IP-announcing layout.
        Assert.True(ManagedSupport.ReportsHostnameEndpoints(),
            "the hostname-announcing cluster container is not reporting hostname endpoints; managed-up.sh's " +
            "--cluster-announce-hostname/--cluster-preferred-endpoint-type flags did not take, or the container is missing.");

        EdgeCaseProvider? handle = null;
        var keys = new List<string>();
        try
        {
            handle = await EdgeCaseSupport.BuildAsync(ManagedSupport.HostnameClusterConnectionString);
            var cache = handle.Cache;
            var mux = handle.Multiplexer;

            var endpoints = await ManagedSupport.WaitForThreeArmedMastersAsync(handle);
            _out.WriteLine($"connected masters: {string.Join(", ", endpoints)}");

            // Seeded with 127.0.0.1:7200 only, so the other two masters can only have come from the announced hostname.
            var discovered = endpoints.Where(e => ResilienceSupport.PortOf(e) != ManagedSupport.HostnameClusterMasterPorts[0]).ToArray();
            Assert.Equal(2, discovered.Length);
            foreach (var endpoint in discovered)
            {
                Assert.Equal("localhost", Assert.IsType<DnsEndPoint>(endpoint).Host);
            }
            foreach (var endpoint in endpoints)
            {
                Assert.True(handle.Armer.RedirectTargets.ContainsKey(endpoint), $"{endpoint} was not armed.");
            }

            var keyByPort = ManagedSupport.KeyPerMasterPort(mux);
            Assert.Equal(3, keyByPort.Count);
            keys.AddRange(keyByPort.Values);

            foreach (var (port, key) in keyByPort)
            {
                await cache.SetAsync(key, "v1");
                Assert.True(await TestHelpers.ReadUntilCachedAsync(cache, key, "v1"), $"key for master port {port} was not cached.");
                _out.WriteLine($"{key} -> slot {mux.GetHashSlot(key)} on port {port}");
            }

            // Each write goes to its own node's hostname:port, so each invalidation comes from a different
            // node's tracking table, exactly like HashTagsAndMultiKeyReadsOnClusterTests does for IPs.
            foreach (var (port, key) in keyByPort)
            {
                ManagedSupport.HostnameCluster(port, "SET", key, "v2");
            }

            var allEvicted = await Poll.UntilAsync(
                () => keyByPort.Values.All(k => !cache.TryGetLocal<string>(k, out _)),
                TimeSpan.FromSeconds(5));
            var survivors = keyByPort.Where(kv => cache.TryGetLocal<string>(kv.Value, out _)).Select(kv => $"{kv.Value}@port{kv.Key}");
            Assert.True(allEvicted, $"not every node's key was evicted after a per-node write on the hostname-announcing cluster: {string.Join(", ", survivors)}");

            foreach (var key in keyByPort.Values)
            {
                Assert.Equal("v2", await cache.GetAsync<string>(key));
            }
        }
        finally
        {
            foreach (var key in keys) ManagedSupport.HostnameCluster(ManagedSupport.HostnameClusterMasterPorts[0], "DEL", key);
            if (handle is not null) await handle.DisposeAsync();
        }
    }

    /// <summary>
    /// A read for a key that does not live on the seed node (<c>127.0.0.1:7200</c>) must be routed to the
    /// right master using the hostname that <c>CLUSTER SLOTS</c>/<c>CLUSTER SHARDS</c> hands back - the same
    /// information a <c>MOVED</c> reply would carry - and still end up cached.
    /// <see cref="AllThreeMastersAreArmedAndInvalidatePerNode"/> already exercises this for two of its three
    /// keys (whichever do not land on the seed master); this test isolates and names that property.
    /// </summary>
    [SkipOnRedis6ImageFact]
    public async Task ReadOfAKeyOnANonSeedMasterIsRoutedByHostnameAndCaches()
    {
        EdgeCaseProvider? handle = null;
        string? key = null;
        try
        {
            handle = await EdgeCaseSupport.BuildAsync(ManagedSupport.HostnameClusterConnectionString);
            var cache = handle.Cache;
            var mux = handle.Multiplexer;

            var endpoints = await ManagedSupport.WaitForThreeArmedMastersAsync(handle);
            var nonSeedPort = ManagedSupport.HostnameClusterMasterPorts.First(p => p != ManagedSupport.HostnameClusterMasterPorts[0]);
            var nonSeedEndpoint = endpoints.First(e => ResilienceSupport.PortOf(e) == nonSeedPort);

            key = ManagedSupport.KeyForPort(mux, nonSeedPort);
            _out.WriteLine($"{key} hashes to master port {nonSeedPort} ({nonSeedEndpoint}), not the seed 127.0.0.1:{ManagedSupport.HostnameClusterMasterPorts[0]}");

            await cache.SetAsync(key, "v1");
            Assert.True(await TestHelpers.ReadUntilCachedAsync(cache, key, "v1"),
                $"a read of a key routed to the non-seed master {nonSeedEndpoint} via its announced hostname was not cached.");
            Assert.Equal("v1", await cache.GetAsync<string>(key));
        }
        finally
        {
            if (key is not null) ManagedSupport.HostnameCluster(ManagedSupport.HostnameClusterMasterPorts[0], "DEL", key);
            if (handle is not null) await handle.DisposeAsync();
        }
    }
}
