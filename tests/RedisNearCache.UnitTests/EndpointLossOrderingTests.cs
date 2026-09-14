using System.Collections.Concurrent;
using System.Diagnostics;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using RedisNearCache.Tracking;
using StackExchange.Redis;
using Xunit;
using Facade = RedisNearCache.Caching.RedisNearCache;

namespace RedisNearCache.UnitTests;

/// <summary>
/// The real <see cref="TrackingArmer"/> and facade wired to a fake multiplexer, driving the event orderings that
/// failovers produce. Every step that must flush or unblock happens synchronously inside the raised event, so the
/// assertions right after a raise are deterministic; only arming a newly promoted master runs in the background and
/// is polled for.
/// </summary>
public class EndpointLossOrderingTests
{
    private const string Key = "k";

    private sealed class Rig : IAsyncDisposable
    {
        public required FakeMultiplexer Mux { get; init; }
        public required TrackingArmer Armer { get; init; }
        public required Facade Cache { get; init; }
        public ConcurrentQueue<string> Events { get; } = new();

        public static async Task<Rig> StartAsync(Action<FakeMultiplexer> topology)
        {
            var mux = new FakeMultiplexer("rnc-unit");
            topology(mux);
            var connection = FakeRedis.Connection(mux);
            var armer = new TrackingArmer(connection, NullLogger<TrackingArmer>.Instance, Timeout.InfiniteTimeSpan);
            var rig = new Rig
            {
                Mux = mux,
                Armer = armer,
                Cache = new Facade(connection, armer, new SilentListener(), Options.Create(new RedisNearCacheOptions()), NullLogger<Facade>.Instance),
            };
            armer.TrackingLost += ep => rig.Events.Enqueue($"lost {ep}");
            armer.EndpointRemoved += ep => rig.Events.Enqueue($"removed {ep}");
            armer.Armed += e => rig.Events.Enqueue($"armed {e.EndPoint} {e.Reason}");
            await rig.Cache.Ready;
            return rig;
        }

        public bool Cached => Cache.TryGetLocal<string>(Key, out _);

        /// <summary>Reads the key through the cache and reports whether the read populated L1.</summary>
        public async Task<bool> ReadCachesAsync()
        {
            Cache.EvictLocal(Key);
            Assert.Equal("v1", await Cache.GetAsync<string>(Key));
            return Cached;
        }

        public string Describe() => string.Join(" | ", Events);

        public ValueTask DisposeAsync() => Cache.DisposeAsync();
    }

    private static Task<bool> UntilAsync(Func<bool> condition, int timeoutMs = 3000) =>
        UntilAsync(() => Task.FromResult(condition()), timeoutMs);

    private static async Task<bool> UntilAsync(Func<Task<bool>> condition, int timeoutMs = 3000)
    {
        var clock = Stopwatch.StartNew();
        while (clock.ElapsedMilliseconds < timeoutMs)
        {
            if (await condition()) return true;
            await Task.Delay(10);
        }
        return await condition();
    }

    [Fact]
    public async Task ArmedMasterFlaggedReplicaBeforeItsConnectionFailsFlushesL1()
    {
        FakeServer oldMaster = null!, replica = null!;
        await using var rig = await Rig.StartAsync(mux =>
        {
            oldMaster = mux.Add(6401, isReplica: false);
            replica = mux.Add(6402, isReplica: true);
        });
        Assert.True(rig.Armer.RedirectTargets.ContainsKey(oldMaster.EndPoint));
        Assert.True(await rig.ReadCachesAsync(), "precondition: the key is cached while the master is armed");

        // Graceful Sentinel failover, bad ordering: the multiplexer flags the old master as a replica first, then
        // Sentinel kills our connections there. No invalidation will ever arrive for what we read from it.
        oldMaster.IsReplica = true;
        rig.Mux.RaiseConnectionFailed(oldMaster, ConnectionType.Interactive);

        Assert.False(rig.Cached, "L1 still holds an entry read from a node whose tracking was just lost: " + rig.Describe());

        // The configuration change that follows forgets the demoted node and must not leave the cache in pass-through.
        rig.Mux.RaiseConfigurationChanged(oldMaster);
        Assert.Contains($"removed {oldMaster.EndPoint}", rig.Events);
        Assert.False(rig.Armer.RedirectTargets.ContainsKey(oldMaster.EndPoint));
        Assert.True(await rig.ReadCachesAsync(), "caching did not resume after the demoted endpoint was removed: " + rig.Describe());
    }

    [Fact]
    public async Task ConfigurationChangeThatDemotesAnArmedMasterFlushesL1AndKeepsCaching()
    {
        FakeServer oldMaster = null!, replica = null!;
        await using var rig = await Rig.StartAsync(mux =>
        {
            oldMaster = mux.Add(6401, isReplica: false);
            replica = mux.Add(6402, isReplica: true);
        });
        Assert.True(await rig.ReadCachesAsync(), "precondition: the key is cached while the master is armed");

        // No connection failure at all (nothing killed our connections): only the role flag changes.
        oldMaster.IsReplica = true;
        rig.Mux.RaiseConfigurationChanged(oldMaster);

        Assert.Contains($"removed {oldMaster.EndPoint}", rig.Events);
        Assert.False(rig.Cached, "removing an armed endpoint left its entries in L1: " + rig.Describe());

        // The promoted master is armed and the cache keeps working.
        replica.IsReplica = false;
        rig.Mux.RaiseConfigurationChanged(replica);
        Assert.True(await UntilAsync(() => rig.Armer.RedirectTargets.ContainsKey(replica.EndPoint)), "the promoted master was never armed: " + rig.Describe());
        Assert.True(await UntilAsync(() => rig.ReadCachesAsync()), "caching did not resume: " + rig.Describe());
    }

    [Fact]
    public async Task ReconnectOfAnArmedMasterThatIsNowAReplicaIsForgottenNotLeftLost()
    {
        FakeServer oldMaster = null!;
        await using var rig = await Rig.StartAsync(mux =>
        {
            oldMaster = mux.Add(6401, isReplica: false);
            mux.Add(6402, isReplica: true);
        });
        Assert.True(await rig.ReadCachesAsync());

        // Connection drops while still a master, then comes back as a replica. The re-arm finds a replica: it must
        // forget the endpoint (EndpointRemoved), not return silently and leave the facade waiting for an Armed.
        rig.Mux.RaiseConnectionFailed(oldMaster, ConnectionType.Interactive);
        Assert.False(rig.Cached);
        oldMaster.IsReplica = true;
        // What OnConnectionRestored queues for a tracked endpoint, run inline so the outcome is deterministic.
        await rig.Armer.RearmAsync(oldMaster.EndPoint, Internal.ArmReason.InteractiveRestored, CancellationToken.None);

        Assert.Contains($"removed {oldMaster.EndPoint}", rig.Events);
        Assert.True(await rig.ReadCachesAsync(), "the cache stayed in pass-through after re-arming a demoted node: " + rig.Describe());
    }

    [Fact]
    public async Task KilledMasterIsForgottenOnlyOnceAnotherMasterIsConnected()
    {
        FakeServer oldMaster = null!, replica = null!;
        await using var rig = await Rig.StartAsync(mux =>
        {
            oldMaster = mux.Add(6401, isReplica: false);
            replica = mux.Add(6402, isReplica: true);
        });
        Assert.True(await rig.ReadCachesAsync());

        // kill -9: the connection fails; the multiplexer keeps listing the node as a (down) master forever.
        oldMaster.IsConnected = false;
        rig.Mux.RaiseConnectionFailed(oldMaster, ConnectionType.Interactive);
        Assert.False(rig.Cached);

        // Nothing has replaced it yet: stay in pass-through (safe), do not forget it.
        rig.Mux.RaiseConfigurationChanged(replica);
        Assert.DoesNotContain($"removed {oldMaster.EndPoint}", rig.Events);
        Assert.False(await rig.ReadCachesAsync(), "a down master with no replacement must keep the cache in pass-through");

        // Sentinel promotes the replica: the new master is armed and the dead one is forgotten, so caching resumes.
        replica.IsReplica = false;
        rig.Mux.RaiseConfigurationChanged(replica);
        Assert.Contains($"removed {oldMaster.EndPoint}", rig.Events);
        Assert.True(await UntilAsync(() => rig.Armer.RedirectTargets.ContainsKey(replica.EndPoint)), "the promoted master was never armed: " + rig.Describe());
        Assert.True(await UntilAsync(() => rig.ReadCachesAsync()), "caching never resumed after the failover: " + rig.Describe());
    }

    [Fact]
    public async Task DownClusterMasterIsNotForgottenJustBecauseOtherMastersAreConnected()
    {
        FakeServer down = null!;
        await using var rig = await Rig.StartAsync(mux =>
        {
            down = mux.Add(7100, isReplica: false, ServerType.Cluster);
            mux.Add(7101, isReplica: false, ServerType.Cluster);
        });
        Assert.True(await rig.ReadCachesAsync());

        down.IsConnected = false;
        rig.Mux.RaiseConnectionFailed(down, ConnectionType.Interactive);
        rig.Mux.RaiseConfigurationChanged(down);

        // In a cluster the other masters serve other slots; only the slot map can say this node was replaced.
        Assert.DoesNotContain($"removed {down.EndPoint}", rig.Events);
        Assert.False(await rig.ReadCachesAsync(), "a down cluster master that still owns its slots must keep the cache in pass-through");
    }
}
