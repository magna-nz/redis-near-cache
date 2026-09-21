using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using RedisNearCache.Internal;
using RedisNearCache.Tracking;
using StackExchange.Redis;
using Xunit;
using Facade = RedisNearCache.Caching.RedisNearCache;

namespace RedisNearCache.UnitTests;

/// <summary>
/// The real <see cref="TrackingArmer"/> and facade wired to a fake multiplexer, exercising replica pre-arm: a
/// replica gets <c>CLIENT TRACKING ON REDIRECT</c> before it is ever promoted, so a promotion needs neither a
/// re-arm nor an L1 flush. Mirrors the rig pattern in <see cref="EndpointLossOrderingTests"/> but drives the
/// reconcile loop by hand (<see cref="Timeout.InfiniteTimeSpan"/>), since these tests assert on the one-shot
/// pre-arm sweep that <see cref="TrackingArmer.StartAsync"/> queues after <c>Ready</c>.
/// </summary>
public class ReplicaPreArmTests
{
    private const string Key = "k";
    private const int MasterPort = 7000;
    private const int ReplicaPort = 7001;

    /// <summary>Records every log entry; the pre-arm warn-once latch is only visible through the log level actually used.</summary>
    private sealed class RecordingLogger : ILogger<TrackingArmer>
    {
        public List<(LogLevel Level, string Message)> Entries { get; } = [];
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            lock (Entries) Entries.Add((logLevel, formatter(state, exception)));
        }
    }

    private static int CountAt(RecordingLogger log, LogLevel level, EndPoint endPoint)
    {
        lock (log.Entries)
        {
            return log.Entries.Count(e => e.Level == level
                && e.Message.Contains(endPoint.ToString()!, StringComparison.Ordinal)
                && e.Message.Contains("pre-arm", StringComparison.OrdinalIgnoreCase));
        }
    }

    private sealed class Rig : IAsyncDisposable
    {
        public required FakeMultiplexer Mux { get; init; }
        public required TrackingArmer Armer { get; init; }
        public required Facade Cache { get; init; }
        public required FakeServer Master { get; init; }
        public required FakeServer Replica { get; init; }
        public required ConcurrentQueue<string> Events { get; init; }

        public static async Task<Rig> StartAsync(Action<FakeServer, FakeServer>? configure = null, ILogger<TrackingArmer>? logger = null)
        {
            var mux = new FakeMultiplexer("rnc-unit");
            var master = mux.Add(MasterPort, isReplica: false);
            var replica = mux.Add(ReplicaPort, isReplica: true);
            configure?.Invoke(master, replica);

            var connection = FakeRedis.Connection(mux);
            var armer = new TrackingArmer(connection, logger ?? NullLogger<TrackingArmer>.Instance, Timeout.InfiniteTimeSpan);
            // Subscribe before the facade is built: its constructor starts the initial arm, and against the fake
            // (every reply completes synchronously) the Initial event is raised before the constructor returns.
            var events = new ConcurrentQueue<string>();
            armer.TrackingLost += ep => events.Enqueue($"lost {ep}");
            armer.EndpointRemoved += ep => events.Enqueue($"removed {ep}");
            armer.Armed += e => events.Enqueue($"armed {e.EndPoint} {e.Reason}");
            var rig = new Rig
            {
                Mux = mux,
                Armer = armer,
                Master = master,
                Replica = replica,
                Events = events,
                Cache = new Facade(connection, armer, new SilentListener(), Options.Create(new RedisNearCacheOptions()), NullLogger<Facade>.Instance),
            };
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
    public async Task ReplicaIsPreArmedAfterStart()
    {
        await using var rig = await Rig.StartAsync();

        Assert.True(
            await UntilAsync(() => rig.Armer.ReplicaRedirectTargets.ContainsKey(rig.Replica.EndPoint), 3000),
            "the replica was never pre-armed: " + rig.Describe());
        Assert.Equal(1000L + ReplicaPort, rig.Armer.ReplicaRedirectTargets[rig.Replica.EndPoint]);

        Assert.Single(rig.Armer.RedirectTargets);
        Assert.True(rig.Armer.RedirectTargets.ContainsKey(rig.Master.EndPoint));

        var armedEvents = rig.Events.Where(e => e.StartsWith("armed ", StringComparison.Ordinal)).ToArray();
        Assert.Equal(new[] { $"armed {rig.Master.EndPoint} {ArmReason.Initial}" }, armedEvents);
    }

    [Fact]
    public async Task PromotionOfAPreArmedReplicaRaisesPromotedWithoutARearm()
    {
        await using var rig = await Rig.StartAsync();
        Assert.True(
            await UntilAsync(() => rig.Armer.ReplicaRedirectTargets.ContainsKey(rig.Replica.EndPoint), 3000),
            "precondition: the replica was never pre-armed: " + rig.Describe());

        Assert.True(await rig.ReadCachesAsync(), "precondition: the key is cached before promotion");

        var flushesBefore = rig.Cache.Statistics.Flushes;

        rig.Master.IsReplica = true;
        rig.Replica.IsReplica = false;
        rig.Mux.RaiseConfigurationChanged(rig.Replica);

        // Reconcile promotes synchronously off the raised event; forgetting the old master waits for that, in the background.
        Assert.Contains($"armed {rig.Replica.EndPoint} {ArmReason.Promoted}", rig.Events);
        Assert.True(await UntilAsync(() => rig.Events.Contains($"removed {rig.Master.EndPoint}"), 3000), "the old master was never forgotten: " + rig.Describe());
        // The rig's handler records "removed" before the facade's flushes, on the retirement's background thread: poll.
        Assert.True(await UntilAsync(() => rig.Cache.Statistics.Flushes == flushesBefore + 2, 3000), // the promotion's flush and the old master's removal
            $"expected 2 flushes, saw {rig.Cache.Statistics.Flushes - flushesBefore}: " + rig.Describe());
        await Task.Delay(50);
        Assert.Equal(flushesBefore + 2, rig.Cache.Statistics.Flushes);
        Assert.Equal(0, rig.Cache.Statistics.Rearms);
        Assert.Single(rig.Armer.RedirectTargets);
        Assert.True(rig.Armer.RedirectTargets.ContainsKey(rig.Replica.EndPoint));
        // The promoted node left the replica set; the demoted old master may already have been pre-armed in its place.
        Assert.False(rig.Armer.ReplicaRedirectTargets.ContainsKey(rig.Replica.EndPoint), "the promoted node is still listed as a pre-armed replica.");

        Assert.True(await rig.ReadCachesAsync(), "caching did not resume after promotion: " + rig.Describe());
    }

    [Fact]
    public async Task ReplicaWhoseConnectionFailedIsNotTrustedAndIsReArmedWithAFlushOnPromotion()
    {
        await using var rig = await Rig.StartAsync();
        Assert.True(
            await UntilAsync(() => rig.Armer.ReplicaRedirectTargets.ContainsKey(rig.Replica.EndPoint), 3000),
            "precondition: the replica was never pre-armed: " + rig.Describe());

        var flushesBeforeFailure = rig.Cache.Statistics.Flushes;
        rig.Mux.RaiseConnectionFailed(rig.Replica, ConnectionType.Interactive);

        // A pre-armed replica is not a tracked master, so losing its connection is not a lifecycle event: no
        // flush, no TrackingLost. Only the (no longer trustworthy) pre-arm entry is dropped.
        Assert.False(rig.Armer.ReplicaRedirectTargets.ContainsKey(rig.Replica.EndPoint));
        Assert.Equal(flushesBeforeFailure, rig.Cache.Statistics.Flushes);
        Assert.DoesNotContain(rig.Events, e => e.StartsWith("lost ", StringComparison.Ordinal));

        rig.Master.IsReplica = true;
        rig.Replica.IsReplica = false;
        rig.Mux.RaiseConfigurationChanged(rig.Replica);

        Assert.True(
            await UntilAsync(() => rig.Events.Contains($"armed {rig.Replica.EndPoint} {ArmReason.TopologyChanged}"), 3000),
            "the replica was never re-armed after losing its pre-arm: " + rig.Describe());
        Assert.DoesNotContain(rig.Events, e => e.StartsWith($"armed {rig.Replica.EndPoint} {ArmReason.Promoted}", StringComparison.Ordinal));
        Assert.True(
            rig.Cache.Statistics.Flushes >= flushesBeforeFailure + 2,
            "expected at least the old master's removal and the topology-changed re-arm to each flush: " + rig.Describe());
    }

    /// <summary>
    /// The pre-arm issues CLIENT TRACKING ON and then ROLE on the same connection; if that second ROLE says master the
    /// promotion may have preceded the ON, so the node must be left to the ordinary arm-and-flush path. The first ROLE
    /// (the "is it a connected replica" gate) says slave here, so the arm proceeds up to the second gate.
    /// </summary>
    [Fact]
    public async Task ReplicaPromotedBetweenTrackingOnAndRoleIsNotRecordedAsPreArmed()
    {
        await using var rig = await Rig.StartAsync((_, replica) =>
        {
            replica.RoleAnswers.Enqueue("slave");   // first ROLE: connected replica, proceed
            replica.RoleAnswers.Enqueue("master");  // second ROLE, after CLIENT TRACKING ON: promoted meanwhile
        });

        Assert.True(await UntilAsync(() => rig.Replica.RoleCalls >= 2, 3000), "the pre-arm never reached its second ROLE check.");
        // The sweep records the pre-arm right after the second ROLE; give it a moment to finish, then it must not be there.
        await UntilAsync(() => rig.Armer.ReplicaRedirectTargets.Count > 0, 300);
        Assert.Empty(rig.Armer.ReplicaRedirectTargets);
        Assert.DoesNotContain(rig.Events, e => e.StartsWith($"armed {rig.Replica.EndPoint}", StringComparison.Ordinal));

        // A later sweep, with the node a replica again, pre-arms it.
        rig.Mux.RaiseConfigurationChanged(rig.Replica);
        Assert.True(await UntilAsync(() => rig.Armer.ReplicaRedirectTargets.ContainsKey(rig.Replica.EndPoint), 3000),
            "the replica was not pre-armed by the next sweep: " + rig.Describe());
    }

    /// <summary>
    /// The per-sweep link check must not mistake a promotion for a broken replication link: between the failover and the
    /// multiplexer's next topology check the node's ROLE says master while the multiplexer still flags it a replica.
    /// Disarming it there would throw away exactly the pre-arm the promotion path needs.
    /// </summary>
    [Fact]
    public async Task SweepLeavesAPreArmedNodeAloneWhenRoleSaysMasterBeforeTheMultiplexerNotices()
    {
        await using var rig = await Rig.StartAsync();
        Assert.True(
            await UntilAsync(() => rig.Armer.ReplicaRedirectTargets.ContainsKey(rig.Replica.EndPoint), 3000),
            "the replica was never pre-armed: " + rig.Describe());
        var preArmId = rig.Armer.ReplicaRedirectTargets[rig.Replica.EndPoint];

        // Promoted on the server, not yet in the multiplexer's view: a sweep runs and re-checks the node.
        rig.Replica.RoleAnswer = "master";
        var roleCalls = rig.Replica.RoleCalls;
        rig.Mux.RaiseConfigurationChanged(rig.Replica);
        Assert.True(await UntilAsync(() => rig.Replica.RoleCalls > roleCalls, 3000), "the sweep never re-checked the pre-armed node.");
        await UntilAsync(() => !rig.Armer.ReplicaRedirectTargets.ContainsKey(rig.Replica.EndPoint), 300);
        Assert.True(rig.Armer.ReplicaRedirectTargets.ContainsKey(rig.Replica.EndPoint),
            "the sweep disarmed a node whose ROLE says master; that is a promotion, not a broken link: " + rig.Describe());

        // Now the multiplexer notices: the pre-arm is what makes this a Promoted arm with no flush.
        var flushesBefore = rig.Cache.Statistics.Flushes;
        rig.Replica.IsReplica = false;
        rig.Master.IsReplica = true;
        rig.Mux.RaiseConfigurationChanged(rig.Replica);
        Assert.Contains($"armed {rig.Replica.EndPoint} {ArmReason.Promoted}", rig.Events);
        Assert.True(await UntilAsync(() => rig.Events.Contains($"removed {rig.Master.EndPoint}"), 3000), "the old master was never forgotten: " + rig.Describe());
        Assert.Equal(preArmId, rig.Armer.RedirectTargets[rig.Replica.EndPoint]);
        Assert.Equal(0, rig.Cache.Statistics.Rearms);
        // The rig's handler records "removed" before the facade's flushes, on the retirement's background thread: poll.
        Assert.True(await UntilAsync(() => rig.Cache.Statistics.Flushes == flushesBefore + 2, 3000), // the promotion's flush and the old master's removal
            $"expected 2 flushes, saw {rig.Cache.Statistics.Flushes - flushesBefore}: " + rig.Describe());
        await Task.Delay(50);
        Assert.Equal(flushesBefore + 2, rig.Cache.Statistics.Flushes);
    }

    /// <summary>A pre-armed replica whose replication link is down is disarmed by the sweep (a full resync would flush L1 for nothing).</summary>
    [Fact]
    public async Task SweepDisarmsAPreArmedReplicaWhoseLinkIsDown()
    {
        await using var rig = await Rig.StartAsync();
        Assert.True(
            await UntilAsync(() => rig.Armer.ReplicaRedirectTargets.ContainsKey(rig.Replica.EndPoint), 3000),
            "the replica was never pre-armed: " + rig.Describe());

        rig.Replica.LinkState = "sync";
        rig.Mux.RaiseConfigurationChanged(rig.Replica);
        Assert.True(await UntilAsync(() => !rig.Armer.ReplicaRedirectTargets.ContainsKey(rig.Replica.EndPoint), 3000),
            "the sweep kept a pre-armed replica whose link is down: " + rig.Describe());
        Assert.Equal(0, rig.Cache.Statistics.Flushes);

        rig.Replica.LinkState = "connected";
        rig.Mux.RaiseConfigurationChanged(rig.Replica);
        Assert.True(await UntilAsync(() => rig.Armer.ReplicaRedirectTargets.ContainsKey(rig.Replica.EndPoint), 3000),
            "the replica was not re-armed once its link came back: " + rig.Describe());
    }

    /// <summary>
    /// A sweep request that arrives while a sweep is still running must not be dropped: it has to cause one more sweep
    /// once the running one finishes. The rig has no reconcile loop, so nothing else would ever retry it. The running
    /// sweep is held on the disarm's CLIENT TRACKING OFF, i.e. after the pre-arm entry is gone but before the sweep ends.
    /// </summary>
    [Fact]
    public async Task ConfigurationChangeDuringARunningSweepRunsAnotherSweep()
    {
        await using var rig = await Rig.StartAsync();
        Assert.True(
            await UntilAsync(() => rig.Armer.ReplicaRedirectTargets.ContainsKey(rig.Replica.EndPoint), 3000),
            "the replica was never pre-armed: " + rig.Describe());

        var offReached = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        rig.Replica.CommandHook = command =>
        {
            if (!string.Equals(command, "CLIENT TRACKING OFF", StringComparison.Ordinal)) return Task.CompletedTask;
            offReached.TrySetResult();
            return release.Task;
        };

        try
        {
            rig.Replica.LinkState = "sync";
            rig.Mux.RaiseConfigurationChanged(rig.Replica);
            Assert.True(await Task.WhenAny(offReached.Task, Task.Delay(3000)) == offReached.Task,
                "the sweep never disarmed the replica whose link is down: " + rig.Describe());
            Assert.False(rig.Armer.ReplicaRedirectTargets.ContainsKey(rig.Replica.EndPoint));

            // The first sweep is still running (held on its OFF): this request must be remembered, not dropped.
            rig.Replica.LinkState = "connected";
            rig.Mux.RaiseConfigurationChanged(rig.Replica);
        }
        finally
        {
            release.TrySetResult();
        }

        Assert.True(await UntilAsync(() => rig.Armer.ReplicaRedirectTargets.ContainsKey(rig.Replica.EndPoint), 3000),
            "the request that arrived during a running sweep was dropped; the replica was not re-armed: " + rig.Describe());
    }

    [Fact]
    public async Task ReplicaThatIsNotAConnectedReplicaOnRoleIsNotTouched()
    {
        await using var rig = await Rig.StartAsync((_, replica) => replica.RoleAnswer = "master");

        Assert.True(await UntilAsync(() => rig.Replica.RoleCalls >= 1, 3000), "the pre-arm sweep never asked the replica for its ROLE.");
        await UntilAsync(() => rig.Armer.ReplicaRedirectTargets.Count > 0, 300);
        Assert.Empty(rig.Armer.ReplicaRedirectTargets);
    }

    // --- pre-arm failure counting and the warn-once latch ------------------------------------------------------

    /// <summary>
    /// The primary counter site (CLIENT TRACKING ON REDIRECT failing): every failed sweep counts, but only the
    /// first one warns; later ones - the sweep runs every 5 s forever - drop to Debug. Recovery clears the latch,
    /// so a genuine later failure warns again instead of staying silent.
    /// </summary>
    [Fact]
    public async Task FailedPreArmCountsEverySweepButWarnsOnceThenTheLatchClearsOnRecoverySoALaterFailureWarnsAgain()
    {
        var recorder = new RecordingLogger();
        var failing = true;
        await using var rig = await Rig.StartAsync((_, replica) =>
        {
            replica.CommandHook = command =>
                command.StartsWith("CLIENT TRACKING ON", StringComparison.Ordinal) && failing
                    ? Task.FromException(new InvalidOperationException("boom"))
                    : Task.CompletedTask;
        }, recorder);

        // Wait on the log entry itself, not on the counter: CountPreArmFailure() and the log call are two separate
        // synchronized operations on the background sweep, so a poller that observes the counter first can still
        // race the log call by a few instructions.
        Assert.True(await UntilAsync(() => CountAt(recorder, LogLevel.Warning, rig.Replica.EndPoint) >= 1, 3000),
            "the failed pre-arm was never warned about: " + rig.Describe());
        Assert.Equal(1, CountAt(recorder, LogLevel.Warning, rig.Replica.EndPoint));
        Assert.True(rig.Armer.PreArmFailures >= 1, "the warned failure must also be counted.");

        rig.Mux.RaiseConfigurationChanged(rig.Master);
        // A later sweep repeats the failure at Debug, never a second Warning: the sweep runs every 5 s forever.
        Assert.True(await UntilAsync(() => CountAt(recorder, LogLevel.Debug, rig.Replica.EndPoint) >= 1, 3000),
            "a second sweep never re-attempted (and logged at Debug) the pre-arm: " + rig.Describe());
        Assert.Equal(1, CountAt(recorder, LogLevel.Warning, rig.Replica.EndPoint));
        Assert.True(rig.Armer.PreArmFailures >= 2, "every failed sweep must be counted, even the ones that only log at Debug.");

        // Recovery: the next sweep succeeds and clears the latch.
        failing = false;
        rig.Mux.RaiseConfigurationChanged(rig.Master);
        Assert.True(await UntilAsync(() => rig.Armer.ReplicaRedirectTargets.ContainsKey(rig.Replica.EndPoint), 3000), "the replica never recovered: " + rig.Describe());

        // Drop the pre-arm (without going through RemoveEndpoint) so the next failure goes through the pre-arm
        // path again rather than the recheck path. OnConnectionFailed runs synchronously on the calling thread.
        rig.Mux.RaiseConnectionFailed(rig.Replica, ConnectionType.Interactive);
        Assert.False(rig.Armer.ReplicaRedirectTargets.ContainsKey(rig.Replica.EndPoint));

        failing = true;
        rig.Mux.RaiseConfigurationChanged(rig.Master);
        Assert.True(await UntilAsync(() => CountAt(recorder, LogLevel.Warning, rig.Replica.EndPoint) == 2, 3000),
            "a genuine later failure after recovery must warn again, not stay silent at Debug: " + rig.Describe());
    }

    /// <summary>
    /// A permanently hidden subscriber connection (an Enterprise proxy, a restricted ACL) is the identical condition
    /// the master arm path already warns about with remediation; it gets the same counter and the same latch.
    /// </summary>
    [Fact]
    public async Task HiddenSubscriberConnectionOnAReplicaCountsAndWarnsOnce()
    {
        var recorder = new RecordingLogger();
        await using var rig = await Rig.StartAsync((_, replica) => replica.SubscriberListed = false, recorder);

        Assert.True(await UntilAsync(() => CountAt(recorder, LogLevel.Warning, rig.Replica.EndPoint) >= 1, 3000),
            "the hidden subscriber was never warned about: " + rig.Describe());
        Assert.Equal(1, CountAt(recorder, LogLevel.Warning, rig.Replica.EndPoint));
        Assert.True(rig.Armer.PreArmFailures >= 1, "the warned failure must also be counted.");
        Assert.Empty(rig.Armer.ReplicaRedirectTargets);

        rig.Mux.RaiseConfigurationChanged(rig.Master);
        Assert.True(await UntilAsync(() => CountAt(recorder, LogLevel.Debug, rig.Replica.EndPoint) >= 1, 3000),
            "a second sweep never re-attempted the pre-arm: " + rig.Describe());
        Assert.Equal(1, CountAt(recorder, LogLevel.Warning, rig.Replica.EndPoint));
    }

    /// <summary>
    /// A pre-armed replica that only lost its connection (site 682) is expected and self-healing (CLAUDE.md: the next
    /// 5 s sweep re-arms it): Information, not Warning, and NOT counted as a pre-arm failure - the pre-arm itself
    /// succeeded.
    /// </summary>
    [Fact]
    public async Task ALostPreArmedConnectionLogsInformationAndDoesNotCountAsAPreArmFailure()
    {
        var recorder = new RecordingLogger();
        await using var rig = await Rig.StartAsync(logger: recorder);
        Assert.True(
            await UntilAsync(() => rig.Armer.ReplicaRedirectTargets.ContainsKey(rig.Replica.EndPoint), 3000),
            "the replica was never pre-armed: " + rig.Describe());

        rig.Mux.RaiseConnectionFailed(rig.Replica, ConnectionType.Interactive);
        Assert.True(await UntilAsync(() => !rig.Armer.ReplicaRedirectTargets.ContainsKey(rig.Replica.EndPoint), 3000));

        Assert.Equal(0, rig.Armer.PreArmFailures);
        lock (recorder.Entries)
        {
            Assert.Contains(recorder.Entries, e => e.Level == LogLevel.Information
                && e.Message.Contains(rig.Replica.EndPoint.ToString()!, StringComparison.Ordinal)
                && e.Message.Contains("pre-armed", StringComparison.Ordinal));
            Assert.DoesNotContain(recorder.Entries, e => e.Level == LogLevel.Warning
                && e.Message.Contains(rig.Replica.EndPoint.ToString()!, StringComparison.Ordinal));
        }
    }

    /// <summary>Broadcast mode never pre-arms a replica at all, so its <see cref="ITrackingArmer.PreArmFailures"/> is a constant 0.</summary>
    [Fact]
    public async Task BroadcastTrackerReportsZeroPreArmFailures()
    {
        var mux = new FakeMultiplexer("rnc-unit-broadcast");
        mux.Add(MasterPort, isReplica: false);
        var connection = FakeRedis.Connection(mux);
        var options = new RedisNearCacheOptions { ConnectionString = "localhost:0" };
        await using var broadcast = new RedisNearCache.Tracking.Broadcast.BroadcastTracker(connection, options, NullLogger<RedisNearCache.Tracking.Broadcast.BroadcastTracker>.Instance);

        ITrackingArmer contract = broadcast;
        Assert.Equal(0, contract.PreArmFailures);
    }
}
