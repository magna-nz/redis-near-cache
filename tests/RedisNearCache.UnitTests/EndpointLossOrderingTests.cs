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
/// failovers produce. Every step that must flush or stop caching happens synchronously inside the raised event, so
/// the assertions right after a raise are deterministic; arming a newly promoted master, and forgetting a demoted one
/// (which waits until the master that replaced it is tracked, see <see cref="TakeoverGuard"/>), run in the background
/// and are polled for.
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

    /// <summary>
    /// Promotes <paramref name="replica"/> and waits until <paramref name="demoted"/> is forgotten, which must happen only
    /// after the promoted node is armed, and until caching resumes.
    /// </summary>
    private static async Task PromoteAndExpectRetirementAsync(Rig rig, FakeServer replica, FakeServer demoted)
    {
        replica.IsReplica = false;
        rig.Mux.RaiseConfigurationChanged(replica);
        Assert.True(await UntilAsync(() => rig.Events.Contains($"removed {demoted.EndPoint}")), $"{demoted.EndPoint} was never forgotten: " + rig.Describe());
        var events = rig.Events.ToList();
        var armedAt = events.FindIndex(e => e.StartsWith($"armed {replica.EndPoint} ", StringComparison.Ordinal));
        Assert.True(armedAt >= 0 && armedAt < events.IndexOf($"removed {demoted.EndPoint}"),
            "the demoted master was forgotten before the promoted one was armed: " + rig.Describe());
        Assert.False(rig.Armer.RedirectTargets.ContainsKey(demoted.EndPoint));
        Assert.True(await UntilAsync(() => rig.ReadCachesAsync()), "caching did not resume after the failover: " + rig.Describe());
    }

    /// <summary>Gives a background retirement time to (wrongly) happen, then checks it did not.</summary>
    private static async Task ExpectKeptAsync(Rig rig, FakeServer demoted)
    {
        await UntilAsync(() => rig.Events.Contains($"removed {demoted.EndPoint}"), 300);
        Assert.DoesNotContain($"removed {demoted.EndPoint}", rig.Events);
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

        // The configuration change that follows sees no master at all: the demoted node is not forgotten yet, and the
        // cache stays in pass-through, because nothing would track the master that replaces it.
        rig.Mux.RaiseConfigurationChanged(oldMaster);
        await ExpectKeptAsync(rig, oldMaster);
        Assert.False(await rig.ReadCachesAsync(), "the cache resumed caching with no tracked master: " + rig.Describe());

        // Once the promoted master is tracked, the demoted node is forgotten and caching resumes.
        await PromoteAndExpectRetirementAsync(rig, replica, oldMaster);
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

        // No connection failure at all (nothing killed our connections): only the role flag changes. No master has
        // taken over yet, so the demoted node keeps its redirect (its tracking still covers what was read from it).
        oldMaster.IsReplica = true;
        rig.Mux.RaiseConfigurationChanged(oldMaster);
        await ExpectKeptAsync(rig, oldMaster);
        Assert.True(rig.Armer.RedirectTargets.ContainsKey(oldMaster.EndPoint));

        // The promoted master is armed, then the demoted node is forgotten. (That the removal itself flushes is pinned
        // exactly by ReplicaPreArmTests; here the promotion flushes too, so a count would prove nothing.)
        await PromoteAndExpectRetirementAsync(rig, replica, oldMaster);
    }

    [Fact]
    public async Task ReconnectOfAnArmedMasterThatIsNowAReplicaIsForgottenNotLeftLost()
    {
        FakeServer oldMaster = null!, replica = null!;
        await using var rig = await Rig.StartAsync(mux =>
        {
            oldMaster = mux.Add(6401, isReplica: false);
            replica = mux.Add(6402, isReplica: true);
        });
        Assert.True(await rig.ReadCachesAsync());

        // Connection drops while still a master, then comes back as a replica. The re-arm finds a replica: it must
        // eventually forget the endpoint (EndpointRemoved), not return silently and leave the facade waiting for an
        // Armed - but only once the master that replaced it is tracked.
        rig.Mux.RaiseConnectionFailed(oldMaster, ConnectionType.Interactive);
        Assert.False(rig.Cached);
        oldMaster.IsReplica = true;
        // What OnConnectionRestored queues for a tracked endpoint, run inline so the outcome is deterministic.
        await rig.Armer.RearmAsync(oldMaster.EndPoint, Internal.ArmReason.InteractiveRestored, CancellationToken.None);

        await ExpectKeptAsync(rig, oldMaster);
        Assert.False(await rig.ReadCachesAsync(), "the cache resumed caching with no tracked master: " + rig.Describe());

        await PromoteAndExpectRetirementAsync(rig, replica, oldMaster);
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

        // Sentinel promotes the replica: the new master is armed, then the dead one is forgotten, so caching resumes.
        await PromoteAndExpectRetirementAsync(rig, replica, oldMaster);
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
