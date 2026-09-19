using System.Collections.Concurrent;
using System.Diagnostics;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using RedisNearCache.Internal;
using RedisNearCache.Tracking;
using StackExchange.Redis;
using Xunit;
using Facade = RedisNearCache.Caching.RedisNearCache;

namespace RedisNearCache.UnitTests;

/// <summary>
/// Two arms of one endpoint. A node restart restores the interactive and the subscriber connection and each queues
/// an arm; the reconcile sweep and the retry loop can each add another. The gate keeps them from interleaving on the
/// wire, but one <c>Armed</c> answers every loss announced before it, so the facade is caching again by the time the
/// second arm gets the gate and sends <c>CLIENT TRACKING OFF</c>. The real <see cref="TrackingArmer"/> and facade on
/// a fake multiplexer, with <see cref="FakeServer.CommandHook"/> holding an arm at its <c>OFF</c>.
/// </summary>
public class OverlappingArmTests
{
    private const string Key = "k";
    private const string TrackingOff = "CLIENT TRACKING OFF";

    private sealed class Rig : IAsyncDisposable
    {
        public required FakeMultiplexer Mux { get; init; }
        public required TrackingArmer Armer { get; init; }
        public required Facade Cache { get; init; }
        public ConcurrentQueue<string> Events { get; } = new();

        public static async Task<Rig> StartAsync(Action<FakeMultiplexer> topology, TimeSpan? reconcileInterval = null)
        {
            var mux = new FakeMultiplexer("rnc-unit");
            topology(mux);
            var connection = FakeRedis.Connection(mux);
            var armer = new TrackingArmer(connection, NullLogger<TrackingArmer>.Instance, reconcileInterval ?? Timeout.InfiniteTimeSpan);
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

    private static TaskCompletionSource Signal() => new(TaskCreationOptions.RunContinuationsAsynchronously);

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

    private static Task<bool> UntilAsync(Func<bool> condition, int timeoutMs = 3000) =>
        UntilAsync(() => Task.FromResult(condition()), timeoutMs);

    [Fact]
    public async Task SecondArmAnnouncesTheLossAgainBeforeItTurnsTrackingOff()
    {
        FakeServer master = null!;
        await using var rig = await Rig.StartAsync(mux => master = mux.Add(6379, isReplica: false));
        Assert.True(await rig.ReadCachesAsync(), "precondition: the key is cached while the master is armed");

        var offs = 0;
        var firstAtOff = Signal();
        var releaseFirst = Signal();
        bool? coherentAtSecondOff = null;
        master.CommandHook = command =>
        {
            if (command != TrackingOff) return Task.CompletedTask;
            switch (Interlocked.Increment(ref offs))
            {
                case 1:
                    firstAtOff.TrySetResult();
                    return releaseFirst.Task;
                case 2:
                    // What the application sees at the instant the server forgets every key it tracked for us.
                    coherentAtSecondOff = rig.Cache.IsCoherent;
                    return Task.CompletedTask;
                default:
                    return Task.CompletedTask; // the OFF sent on dispose
            }
        };

        // A node restart: both connections come back, one arm each. The first is held at its OFF, under the gate.
        var first = rig.Armer.RearmAsync(master.EndPoint, ArmReason.SubscriptionRestored, CancellationToken.None);
        await firstAtOff.Task.WaitAsync(TimeSpan.FromSeconds(3));
        // Announces its loss and queues on the gate before RearmAsync returns (both happen ahead of its first await).
        var second = rig.Armer.RearmAsync(master.EndPoint, ArmReason.InteractiveRestored, CancellationToken.None);
        Assert.False(rig.Cache.IsCoherent, "two arms are pending, the facade must be in pass-through: " + rig.Describe());

        releaseFirst.SetResult();
        await Task.WhenAll(first, second).WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal(2, Volatile.Read(ref offs));
        Assert.False(coherentAtSecondOff, "the second arm sent CLIENT TRACKING OFF while the cache reported itself coherent: " + rig.Describe());

        // Every arm is preceded by a loss announced after the arm before it.
        var events = rig.Events.ToList();
        var firstArmed = events.IndexOf($"armed {master.EndPoint} {ArmReason.SubscriptionRestored}");
        var secondArmed = events.IndexOf($"armed {master.EndPoint} {ArmReason.InteractiveRestored}");
        Assert.True(firstArmed >= 0 && secondArmed > firstArmed, "expected both arms to complete, in order: " + rig.Describe());
        Assert.Contains($"lost {master.EndPoint}", events.Skip(firstArmed + 1).Take(secondArmed - firstArmed - 1));

        Assert.True(rig.Cache.IsCoherent, "the cache did not come back after the second arm: " + rig.Describe());
        Assert.True(await rig.ReadCachesAsync(), "caching did not resume after the second arm: " + rig.Describe());
    }

    [Fact]
    public async Task SingleArmAnnouncesItsLossOnce()
    {
        FakeServer master = null!;
        await using var rig = await Rig.StartAsync(mux => master = mux.Add(6379, isReplica: false));

        await rig.Armer.RearmAsync(master.EndPoint, ArmReason.InteractiveRestored, CancellationToken.None);

        // The re-announcement is for an arm that found the loss answered by another; an arm on its own must not
        // pay a second flush.
        Assert.Equal(1, rig.Events.Count(e => e == $"lost {master.EndPoint}"));
        Assert.True(await rig.ReadCachesAsync(), rig.Describe());
    }

    [Fact]
    public async Task ConfigurationChangesDoNotQueueArmsBehindOneInFlight()
    {
        FakeServer late = null!;
        await using var rig = await Rig.StartAsync(mux =>
        {
            mux.Add(7000, isReplica: false);
            late = mux.Add(7001, isReplica: false);
            late.IsConnected = false; // down at startup: not armed, not lost
        });
        Assert.False(rig.Armer.RedirectTargets.ContainsKey(late.EndPoint));

        var offs = 0;
        var atOff = Signal();
        var release = Signal();
        late.CommandHook = command =>
        {
            if (command != TrackingOff) return Task.CompletedTask;
            if (Interlocked.Increment(ref offs) != 1) return Task.CompletedTask;
            atOff.TrySetResult();
            return release.Task;
        };

        late.IsConnected = true;
        rig.Mux.RaiseConfigurationChanged(late);
        await atOff.Task.WaitAsync(TimeSpan.FromSeconds(3));
        Assert.False(rig.Cache.IsCoherent, "a master being armed must hold the cache in pass-through: " + rig.Describe());

        // The arm is slow (a loaded node, a long CLIENT LIST) and the topology keeps changing under it.
        for (var i = 0; i < 3; i++) rig.Mux.RaiseConfigurationChanged(late);

        release.SetResult();
        Assert.True(await UntilAsync(() => rig.Armer.RedirectTargets.ContainsKey(late.EndPoint)), "the late master was never armed: " + rig.Describe());
        await Task.Delay(300); // time for arms that were (wrongly) queued behind the first to run

        Assert.Equal(1, Volatile.Read(ref offs));
        Assert.Equal(1, rig.Events.Count(e => e.StartsWith($"armed {late.EndPoint} ", StringComparison.Ordinal)));
        Assert.True(rig.Cache.IsCoherent, rig.Describe());
    }

    [Fact]
    public async Task PeriodicSweepLeavesALostMasterToItsRetryLoop()
    {
        FakeServer late = null!;
        await using var rig = await Rig.StartAsync(mux =>
        {
            mux.Add(7000, isReplica: false);
            late = mux.Add(7001, isReplica: false);
            late.IsConnected = false;
        }, reconcileInterval: TimeSpan.FromMilliseconds(40));

        // A master that cannot be armed: the node rejects the command itself (a proxy without REDIRECT tracking), so
        // the arm gives up at once and hands the endpoint to the retry loop, whose first round is 5 s away.
        var attempts = 0;
        late.CommandHook = command =>
        {
            if (!command.StartsWith("CLIENT TRACKING ON", StringComparison.Ordinal)) return Task.CompletedTask;
            Interlocked.Increment(ref attempts);
            return Task.FromException(new RedisNearCacheTrackingException("rejected CLIENT TRACKING ON REDIRECT"));
        };

        late.IsConnected = true; // found by the next sweep
        Assert.True(await UntilAsync(() => Volatile.Read(ref attempts) >= 1), "the sweep never tried to arm the new master: " + rig.Describe());
        await Task.Delay(600); // fifteen sweeps

        // One from the sweep that found it; at most one more from a sweep landing between that arm ending and the
        // retry loop registering. Every sweep used to add one.
        Assert.InRange(Volatile.Read(ref attempts), 1, 2);
        Assert.False(rig.Cache.IsCoherent, "an unarmed master must hold the cache in pass-through: " + rig.Describe());
    }

    [Fact]
    public async Task FailedArmOfAPreArmedNodeEndsItsPreArm()
    {
        FakeServer replica = null!;
        await using var rig = await Rig.StartAsync(mux =>
        {
            mux.Add(6401, isReplica: false);
            replica = mux.Add(6402, isReplica: true);
        });
        Assert.True(await UntilAsync(() => rig.Armer.ReplicaRedirectTargets.ContainsKey(replica.EndPoint)), "precondition: the replica is pre-armed");

        // Promoted, and something arms it before a reconcile has recorded the promotion. The arm gets as far as OFF,
        // which ends the pre-arm on the server, and then fails.
        replica.IsReplica = false;
        var sentOff = false;
        replica.CommandHook = command =>
        {
            if (command == TrackingOff) sentOff = true;
            return command.StartsWith("CLIENT TRACKING ON", StringComparison.Ordinal)
                ? Task.FromException(new RedisNearCacheTrackingException("rejected CLIENT TRACKING ON REDIRECT"))
                : Task.CompletedTask;
        };
        await Assert.ThrowsAsync<RedisNearCacheTrackingException>(
            () => rig.Armer.RearmAsync(replica.EndPoint, ArmReason.InteractiveRestored, CancellationToken.None));
        Assert.True(sentOff, "precondition: the failed arm turned tracking off on the node");
        Assert.False(rig.Armer.ReplicaRedirectTargets.ContainsKey(replica.EndPoint), "the pre-arm outlived the OFF that ended it");

        // The reconcile that now learns of the promotion must arm the node, not report it as already armed.
        replica.CommandHook = null;
        rig.Mux.RaiseConfigurationChanged(replica);
        Assert.True(await UntilAsync(() => rig.Armer.RedirectTargets.ContainsKey(replica.EndPoint)), "the promoted node was never armed: " + rig.Describe());
        Assert.DoesNotContain($"armed {replica.EndPoint} {ArmReason.Promoted}", rig.Events);
        Assert.Contains($"armed {replica.EndPoint} {ArmReason.TopologyChanged}", rig.Events);
    }
}
