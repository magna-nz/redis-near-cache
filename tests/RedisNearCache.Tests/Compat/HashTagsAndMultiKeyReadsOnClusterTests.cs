using Xunit;
using Xunit.Abstractions;

namespace RedisNearCache.Tests.Compat;

/// <summary>
/// Cluster-mode key routing under tracking. Two things are proved: keys sharing a hash tag land on one slot,
/// so a single multi-key write (<c>MSET</c>, which the server rejects across slots) invalidates both of them
/// from the one node that owns them; and keys on DIFFERENT slots - therefore armed on different masters, with
/// a separate <c>CLIENT TRACKING ... REDIRECT</c> per node - are each invalidated by a write routed to their
/// own owner.
/// </summary>
public class HashTagsAndMultiKeyReadsOnClusterTests : IClassFixture<ClusterCacheFixture>
{
    private readonly ClusterCacheFixture _fx;
    private readonly ITestOutputHelper _out;

    public HashTagsAndMultiKeyReadsOnClusterTests(ClusterCacheFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _out = output;
    }

    [Fact]
    public async Task HashTaggedKeysShareASlotAndAreInvalidatedTogether()
    {
        var tag = $"acct{Guid.NewGuid():N}";
        var a = $"{{{tag}}}:a";
        var b = $"{{{tag}}}:b";
        try
        {
            var mux = _fx.Connection.Multiplexer;
            Assert.Equal(mux.GetHashSlot(a), mux.GetHashSlot(b));
            _out.WriteLine($"{a} and {b} both hash to slot {mux.GetHashSlot(a)}");

            await _fx.Cache.SetAsync(a, "a1");
            await _fx.Cache.SetAsync(b, "b1");
            Assert.True(await TestHelpers.ReadUntilCachedAsync(_fx.Cache, a, "a1"), $"{a} was not cached.");
            Assert.True(await TestHelpers.ReadUntilCachedAsync(_fx.Cache, b, "b1"), $"{b} was not cached.");

            // A single cross-key write, legal only because the hash tag puts both keys in one slot.
            RedisCli.Cluster(ClusterCacheFixture.MasterPorts[0], "MSET", a, "1", b, "2");

            var bothEvicted = await Poll.UntilAsync(
                () => !_fx.Cache.TryGetLocal<string>(a, out _) && !_fx.Cache.TryGetLocal<string>(b, out _),
                TimeSpan.FromSeconds(3));
            Assert.True(bothEvicted, "the hash-tagged pair was not both evicted after a single-slot MSET.");

            Assert.Equal("1", await _fx.Cache.GetAsync<string>(a));
            Assert.Equal("2", await _fx.Cache.GetAsync<string>(b));
        }
        finally
        {
            RedisCli.Cluster(ClusterCacheFixture.MasterPorts[0], "DEL", a, b);
        }
    }

    [Fact]
    public async Task KeysOnDifferentSlotsAreEachInvalidatedByTheirOwnMaster()
    {
        var mux = _fx.Connection.Multiplexer;
        var endpoints = mux.GetEndPoints();
        var keyByEndpoint = TestHelpers.KeyPerMaster(mux, mux.GetServer(endpoints[0]), endpoints);
        Assert.True(keyByEndpoint.Count >= 2, "needed keys on at least two masters.");

        var keys = keyByEndpoint.Values.ToArray();
        try
        {
            foreach (var (endpoint, key) in keyByEndpoint)
            {
                await _fx.Cache.SetAsync(key, "v1");
                Assert.True(
                    await TestHelpers.ReadUntilCachedAsync(_fx.Cache, key, "v1"),
                    $"key for {endpoint} was not cached.");
                _out.WriteLine($"{key} -> slot {mux.GetHashSlot(key)} on {endpoint}");
            }

            // Every write goes through a cluster-aware redis-cli (-c) entered at ONE port: the client follows
            // the MOVED redirection to whichever master owns the key, so each invalidation comes from a
            // different node's tracking table.
            foreach (var key in keys)
            {
                RedisCli.Cluster(ClusterCacheFixture.MasterPorts[0], "SET", key, "v2");
            }

            var allEvicted = await Poll.UntilAsync(
                () => keys.All(k => !_fx.Cache.TryGetLocal<string>(k, out _)),
                TimeSpan.FromSeconds(3));
            var survivors = keys.Where(k => _fx.Cache.TryGetLocal<string>(k, out _)).ToArray();
            Assert.True(allEvicted, $"keys not evicted after a cross-slot write: {string.Join(", ", survivors)}");

            foreach (var key in keys)
            {
                Assert.Equal("v2", await _fx.Cache.GetAsync<string>(key));
            }

            // Cross-slot MSET is a server error (redis-cli prints it and still exits 0), not something the
            // cache has to cope with; asserted so the single-slot case above is understood as the only way to
            // invalidate two keys of different slots with one command.
            var crossSlot = RedisCli.Cluster(ClusterCacheFixture.MasterPorts[0], "MSET", keys[0], "x", keys[1], "y");
            Assert.Contains("CROSSSLOT", crossSlot, StringComparison.Ordinal);
            _out.WriteLine($"cross-slot MSET rejected as expected: {crossSlot}");
        }
        finally
        {
            foreach (var key in keys)
            {
                RedisCli.Cluster(ClusterCacheFixture.MasterPorts[0], "DEL", key);
            }
        }
    }
}
