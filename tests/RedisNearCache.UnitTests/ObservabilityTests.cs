using System.Net;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging.Abstractions;
using RedisNearCache.Internal;
using RedisNearCache.Tracking;
using StackExchange.Redis;
using Xunit;
using Facade = RedisNearCache.Caching.RedisNearCache;

#pragma warning disable CS0618 // the simple exception constructors are the clearest way to fake a server reply here

namespace RedisNearCache.UnitTests;

/// <summary>
/// The signals added for the silent failure modes: how long the cache has been in pass-through, which endpoints it is
/// waiting on, the two latches that used to be visible only in a log line said once, a serializer that throws, and the
/// per-reason breakdown of re-arms and flushes. Every one of them is read at collection time from state the cache
/// keeps anyway - nothing here is allowed to mean extra work on a read - so the tests drive real events and then
/// collect, rather than asserting on a recording.
/// </summary>
public class ObservabilityTests
{
    private const string Key = "k";
    private static readonly EndPoint A = new DnsEndPoint("a", 7001);
    private static readonly EndPoint B = new DnsEndPoint("b", 7002);

    /// <summary>An armer whose three lifecycle events are raised by the test, as in <c>LostEndpointGateTests</c>.</summary>
    private sealed class ScriptedArmer : ITrackingArmer
    {
        public event Action<TrackingArmedEvent>? Armed;
        public event Action<EndPoint>? TrackingLost;
        public event Action<EndPoint>? EndpointRemoved;

        public IReadOnlyDictionary<EndPoint, long> RedirectTargets { get; } = new Dictionary<EndPoint, long>();
        public IReadOnlyDictionary<EndPoint, long> ReplicaRedirectTargets { get; } = new Dictionary<EndPoint, long>();
        public long PreArmFailures => 0;

        public void RaiseArmed(EndPoint endPoint, ArmReason reason) => Armed?.Invoke(new TrackingArmedEvent(endPoint, 1, reason));
        public void RaiseLost(EndPoint endPoint) => TrackingLost?.Invoke(endPoint);
        public void RaiseRemoved(EndPoint endPoint) => EndpointRemoved?.Invoke(endPoint);

        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task RearmAsync(EndPoint endPoint, ArmReason reason, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task RearmAllAsync(ArmReason reason, CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    /// <summary>A listener whose <c>FlushAll</c> the test raises: the only way to reach <c>FlushReason.ServerFlush</c>.</summary>
    private sealed class ScriptedListener : IInvalidationListener
    {
        public event Action<string>? KeyInvalidated;
        public event Action? FlushAll;

        public void RaiseFlushAll() => FlushAll?.Invoke();
        public void RaiseInvalidated(string key) => KeyInvalidated?.Invoke(key);

        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    /// <summary>An armer whose start fails, so the cache settles degraded with an EMPTY lost set: pass-through all the same.</summary>
    private sealed class FailingArmer : ITrackingArmer
    {
#pragma warning disable CS0067
        public event Action<TrackingArmedEvent>? Armed;
        public event Action<EndPoint>? TrackingLost;
        public event Action<EndPoint>? EndpointRemoved;
#pragma warning restore CS0067
        public IReadOnlyDictionary<EndPoint, long> RedirectTargets { get; } = new Dictionary<EndPoint, long>();
        public IReadOnlyDictionary<EndPoint, long> ReplicaRedirectTargets { get; } = new Dictionary<EndPoint, long>();
        public long PreArmFailures => 0;

        public Task StartAsync(CancellationToken cancellationToken) =>
            Task.FromException(new RedisConnectionException(ConnectionFailureType.UnableToConnect, "no connection is available"));
        public Task RearmAsync(EndPoint endPoint, ArmReason reason, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task RearmAllAsync(ArmReason reason, CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    /// <summary>
    /// A listener whose <c>StartAsync</c> does not return until the test lets it: the stretch before the FIRST arm,
    /// where the armer has not even been started, nothing is armed and nothing has been announced lost either.
    /// </summary>
    private sealed class BlockingListener(Task gate) : IInvalidationListener
    {
#pragma warning disable CS0067
        public event Action<string>? KeyInvalidated;
        public event Action? FlushAll;
#pragma warning restore CS0067
        public Task StartAsync(CancellationToken cancellationToken) => gate;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class SerializerBoom : Exception
    {
        public SerializerBoom() : base("the serializer refused this value") { }
    }

    private sealed class ThrowingSerializer(bool onSerialize, bool onDeserialize) : IRedisNearCacheSerializer
    {
        public byte[] Serialize<T>(T value) =>
            onSerialize ? throw new SerializerBoom() : JsonRedisNearCacheSerializer.Instance.Serialize(value);

        public T? Deserialize<T>(ReadOnlyMemory<byte> data) =>
            onDeserialize ? throw new SerializerBoom() : JsonRedisNearCacheSerializer.Instance.Deserialize<T>(data);
    }

    private sealed class Rig : IAsyncDisposable
    {
        public required ScriptedArmer Armer { get; init; }
        public required ScriptedListener Listener { get; init; }
        public required Facade Cache { get; init; }
        public required string ClientName { get; init; }

        public static async Task<Rig> StartAsync(Action<RedisNearCacheOptions>? configure = null)
        {
            var clientName = "rnc-obs-" + Guid.NewGuid().ToString("N");
            var mux = new FakeMultiplexer(clientName);
            mux.Add(7000, isReplica: false);
            var armer = new ScriptedArmer();
            var listener = new ScriptedListener();
            var options = new RedisNearCacheOptions();
            configure?.Invoke(options);
            var rig = new Rig
            {
                Armer = armer,
                Listener = listener,
                ClientName = clientName,
                Cache = new Facade(FakeRedis.Connection(mux), armer, listener, Microsoft.Extensions.Options.Options.Create(options), NullLogger<Facade>.Instance),
            };
            await rig.Cache.Ready;
            // The start settled with nothing lost, so the cache is coherent and the pass-through clock is stopped.
            Assert.True(rig.Cache.IsCoherent, "precondition: coherent once the start has returned");
            return rig;
        }

        public MeterSnapshot Collect() => MeterProbe.Collect(ClientName);

        public ValueTask DisposeAsync() => Cache.DisposeAsync();
    }

    // --- pass-through duration ------------------------------------------------------------------------------

    [Fact]
    public async Task PassThroughSecondsIsZeroWhileCoherentAndGrowsOnceCoherenceIsLost()
    {
        await using var rig = await Rig.StartAsync();
        Assert.Equal(0d, rig.Cache.Statistics.PassThroughSeconds);
        Assert.Equal(0d, rig.Collect().Doubles["redisnearcache.pass_through.seconds"]);

        rig.Armer.RaiseLost(A);
        Assert.False(rig.Cache.IsCoherent);
        // Stopwatch ticks are fine-grained but not infinitely so; a short wait makes the difference unambiguous.
        await Task.Delay(30);

        var running = rig.Cache.Statistics.PassThroughSeconds;
        Assert.True(running > 0, $"the pass-through clock did not start: {running}");
        Assert.True(rig.Collect().Doubles["redisnearcache.pass_through.seconds"] > 0);

        rig.Armer.RaiseArmed(A, ArmReason.InteractiveRestored);
        Assert.True(rig.Cache.IsCoherent);
        Assert.Equal(0d, rig.Cache.Statistics.PassThroughSeconds);
        Assert.Equal(0d, rig.Collect().Doubles["redisnearcache.pass_through.seconds"]);
    }

    [Fact]
    public async Task ASecondLossWhileAlreadyInPassThroughDoesNotRestartTheClock()
    {
        await using var rig = await Rig.StartAsync();
        rig.Armer.RaiseLost(A);
        await Task.Delay(40);
        var afterFirst = rig.Cache.Statistics.PassThroughSeconds;

        rig.Armer.RaiseLost(B);

        // Keyed off the loss of coherence, not off each individual loss: a flapping second endpoint must not keep
        // resetting the duration to nearly zero, or the one number that says "this has been broken for a while" never
        // grows past the gap between two events.
        Assert.True(rig.Cache.Statistics.PassThroughSeconds >= afterFirst,
            $"the second loss moved the clock backwards: {afterFirst} then {rig.Cache.Statistics.PassThroughSeconds}");
    }

    [Fact]
    public async Task ACacheThatNeverArmedReportsAGrowingPassThroughDurationNotZero()
    {
        // The case no lost endpoint covers: the start failed, so the cache is degraded with an EMPTY lost set. Keyed
        // off _lostCount this would report 0 seconds of pass-through forever while caching nothing.
        var clientName = "rnc-obs-never-" + Guid.NewGuid().ToString("N");
        var mux = new FakeMultiplexer(clientName);
        mux.Add(7000, isReplica: false);
        var options = new RedisNearCacheOptions();
        await using var cache = new Facade(FakeRedis.Connection(mux), new FailingArmer(), new ScriptedListener(),
            Microsoft.Extensions.Options.Options.Create(options), NullLogger<Facade>.Instance);
        await Assert.ThrowsAsync<RedisConnectionException>(() => cache.Ready);

        await Task.Delay(30);

        Assert.False(cache.IsCoherent);
        Assert.Equal(0, cache.Statistics.LostEndpointCount);
        Assert.True(cache.Statistics.PassThroughSeconds > 0,
            $"a cache that never armed must report a pass-through duration, not {cache.Statistics.PassThroughSeconds}");
        Assert.True(MeterProbe.Collect(clientName).Doubles["redisnearcache.pass_through.seconds"] > 0);
    }

    /// <summary>
    /// The clock has to start at CONSTRUCTION, not at the first loss: a cache is in pass-through from the moment it
    /// exists until its start sequence returns, and no loss is ever announced for that stretch. A cache wedged there
    /// - the subscription cannot be made, which is what <c>abortConnect=false</c> makes possible - is the one worth
    /// alerting on, and it looks identical to a healthy one in every other counter.
    /// </summary>
    [Fact]
    public async Task PassThroughSecondsIsAlreadyRunningBeforeTheFirstArm()
    {
        var clientName = "rnc-obs-starting-" + Guid.NewGuid().ToString("N");
        var mux = new FakeMultiplexer(clientName);
        mux.Add(7000, isReplica: false);
        var connection = FakeRedis.Connection(mux);
        var armer = new TrackingArmer(connection, NullLogger<TrackingArmer>.Instance, Timeout.InfiniteTimeSpan);
        // Completed in the finally below whatever happens, so a failing assertion cannot leave the dispose - which
        // awaits Ready - waiting on this gate forever.
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var cache = new Facade(connection, armer, new BlockingListener(gate.Task),
            Microsoft.Extensions.Options.Options.Create(new RedisNearCacheOptions()), NullLogger<Facade>.Instance);
        try
        {
            await Task.Delay(30);

            Assert.False(cache.IsCoherent, "precondition: the start has not returned, so nothing is cached");
            Assert.Equal(0, cache.Statistics.LostEndpointCount);
            Assert.True(cache.Statistics.PassThroughSeconds > 0,
                $"the clock must run from construction, not from the first loss: {cache.Statistics.PassThroughSeconds}");
            Assert.True(MeterProbe.Collect(clientName).Doubles["redisnearcache.pass_through.seconds"] > 0);
        }
        finally
        {
            gate.TrySetResult();
        }

        await cache.Ready;
        Assert.True(cache.IsCoherent);
        Assert.Equal(0d, cache.Statistics.PassThroughSeconds);
    }

    // --- lost endpoints -------------------------------------------------------------------------------------

    [Fact]
    public async Task LostEndpointCountAndTheGaugeFollowTheLostSet()
    {
        await using var rig = await Rig.StartAsync();
        Assert.Equal(0, rig.Cache.Statistics.LostEndpointCount);
        Assert.Equal(0, rig.Collect().Longs["redisnearcache.endpoints.lost"]);

        rig.Armer.RaiseLost(A);
        rig.Armer.RaiseLost(B);
        Assert.Equal(2, rig.Cache.Statistics.LostEndpointCount);
        Assert.Equal(2, rig.Collect().Longs["redisnearcache.endpoints.lost"]);

        // A duplicate loss is one endpoint, not two: the gauge reports the SET, as the gate does.
        rig.Armer.RaiseLost(A);
        Assert.Equal(2, rig.Collect().Longs["redisnearcache.endpoints.lost"]);

        rig.Armer.RaiseArmed(A, ArmReason.Recovered);
        rig.Armer.RaiseRemoved(B);
        Assert.Equal(0, rig.Cache.Statistics.LostEndpointCount);
        Assert.Equal(0, rig.Collect().Longs["redisnearcache.endpoints.lost"]);
    }

    [Fact]
    public async Task LostEndpointAddressesNamesEveryLostEndpointAndIsEmptyWhenNoneIs()
    {
        await using var rig = await Rig.StartAsync();
        Assert.Equal(string.Empty, rig.Cache.LostEndpointAddresses());

        rig.Armer.RaiseLost(A);
        rig.Armer.RaiseLost(B);

        var addresses = rig.Cache.LostEndpointAddresses();
        Assert.Contains(A.ToString()!, addresses, StringComparison.Ordinal);
        Assert.Contains(B.ToString()!, addresses, StringComparison.Ordinal);
        Assert.Contains(",", addresses, StringComparison.Ordinal);

        // And it is never a metric tag: an endpoint address is unbounded cardinality.
        Assert.DoesNotContain(rig.Collect().ByReason.Keys, k => k.Reason is { } reason && reason.Contains("7001", StringComparison.Ordinal));

        rig.Armer.RaiseArmed(A, ArmReason.Recovered);
        rig.Armer.RaiseRemoved(B);
        Assert.Equal(string.Empty, rig.Cache.LostEndpointAddresses());
    }

    // --- re-arms and flushes by reason ----------------------------------------------------------------------

    [Fact]
    public async Task EveryRearmReasonLandsOnItsOwnTagAndTheySumToTheTotal()
    {
        await using var rig = await Rig.StartAsync();

        // Every reason that counts as a re-arm, each a different number of times so a mis-indexed slot cannot pass.
        var script = new (ArmReason Reason, int Times)[]
        {
            (ArmReason.InteractiveRestored, 1),
            (ArmReason.SubscriptionRestored, 2),
            (ArmReason.Manual, 3),
            (ArmReason.TopologyChanged, 4),
            (ArmReason.Recovered, 5),
            (ArmReason.VerificationFailed, 6),
            (ArmReason.PushConnectionRestored, 7),
        };
        foreach (var (reason, times) in script)
        {
            for (var i = 0; i < times; i++) rig.Armer.RaiseArmed(A, reason);
        }

        // Initial and Promoted are arms but never re-arms, so they must move neither the total nor any slot.
        rig.Armer.RaiseArmed(A, ArmReason.Initial);
        rig.Armer.RaiseArmed(A, ArmReason.Promoted);

        var measurements = rig.Collect();
        foreach (var (reason, times) in script)
        {
            Assert.Equal(times, measurements.Reason("redisnearcache.rearms", reason));
        }

        var expectedTotal = script.Sum(s => s.Times);
        Assert.Equal(expectedTotal, rig.Cache.Statistics.Rearms);
        // The whole point of the tag: summed over it, the instrument still reads exactly what it read before.
        Assert.Equal(rig.Cache.Statistics.Rearms, measurements.Longs["redisnearcache.rearms"]);
        // Neither excluded reason is emitted at all - a permanent 0 would suggest a re-arm reason that never fires.
        Assert.Equal(-1, measurements.Reason("redisnearcache.rearms", ArmReason.Initial));
        Assert.Equal(-1, measurements.Reason("redisnearcache.rearms", ArmReason.Promoted));
    }

    [Fact]
    public async Task EveryRearmReasonIsEmittedEvenBeforeAnyRearmHappens()
    {
        await using var rig = await Rig.StartAsync();

        var measurements = rig.Collect();

        // A dashboard must show 0, not no-data: an absent series reads like a broken exporter.
        foreach (var reason in new[]
                 {
                     ArmReason.InteractiveRestored, ArmReason.SubscriptionRestored, ArmReason.Manual,
                     ArmReason.TopologyChanged, ArmReason.Recovered, ArmReason.VerificationFailed,
                     ArmReason.PushConnectionRestored,
                 })
        {
            Assert.Equal(0, measurements.Reason("redisnearcache.rearms", reason));
        }

        foreach (var reason in Enum.GetValues<FlushReason>())
        {
            Assert.Equal(0, measurements.Reason("redisnearcache.flushes", reason));
        }
    }

    [Fact]
    public async Task AllFiveFlushReasonsAreReachableAndSumToTheTotal()
    {
        await using var rig = await Rig.StartAsync();

        rig.Listener.RaiseFlushAll();                              // ServerFlush
        rig.Armer.RaiseArmed(A, ArmReason.InteractiveRestored);    // Rearm
        rig.Armer.RaiseLost(A);                                    // TrackingLost
        rig.Armer.RaiseRemoved(A);                                 // EndpointRemoved
        rig.Cache.EvictAllLocal();                                 // Manual

        var measurements = rig.Collect();
        foreach (var reason in Enum.GetValues<FlushReason>())
        {
            Assert.Equal(1, measurements.Reason("redisnearcache.flushes", reason));
        }

        Assert.Equal(5, rig.Cache.Statistics.Flushes);
        Assert.Equal(rig.Cache.Statistics.Flushes, measurements.Longs["redisnearcache.flushes"]);

        // An Initial arm still does not flush, so the reason breakdown cannot have invented one.
        rig.Armer.RaiseArmed(B, ArmReason.Initial);
        Assert.Equal(5, rig.Cache.Statistics.Flushes);
    }

    [Fact]
    public async Task APromotedArmFlushesUnderTheRearmReasonWithoutCountingAsARearm()
    {
        await using var rig = await Rig.StartAsync();

        rig.Armer.RaiseArmed(A, ArmReason.Promoted);

        var measurements = rig.Collect();
        Assert.Equal(1, rig.Cache.Statistics.Flushes);
        Assert.Equal(1, measurements.Reason("redisnearcache.flushes", FlushReason.Rearm));
        Assert.Equal(0, rig.Cache.Statistics.Rearms);
        Assert.Equal(0, measurements.Longs["redisnearcache.rearms"]);
    }

    // --- the two silent latches -----------------------------------------------------------------------------

    [Fact]
    public async Task TtlCapAbandonedFlipsWhenTheServerRejectsPttl()
    {
        var clientName = "rnc-obs-ttl-" + Guid.NewGuid().ToString("N");
        var mux = new FakeMultiplexer(clientName);
        mux.Add(7000, isReplica: false);
        var connection = FakeRedis.Connection(mux);
        var armer = new TrackingArmer(connection, NullLogger<TrackingArmer>.Instance, Timeout.InfiniteTimeSpan);
        await using var cache = new Facade(connection, armer, new SilentListener(),
            Microsoft.Extensions.Options.Options.Create(new RedisNearCacheOptions()), NullLogger<Facade>.Instance);
        await cache.Ready;

        Assert.False(cache.Statistics.TtlCapAbandoned);
        Assert.Equal(0, MeterProbe.Collect(clientName).Longs["redisnearcache.ttl_cap.abandoned"]);

        mux.PttlFailure = new RedisServerException("NOPERM this user has no permissions to run the 'pttl' command");
        Assert.Equal("v1", await cache.GetAsync<string>(Key));

        // The cap is gone for good and was said once in a log line; this is how an operator sees it afterwards.
        Assert.True(cache.Statistics.TtlCapAbandoned);
        Assert.Equal(1, MeterProbe.Collect(clientName).Longs["redisnearcache.ttl_cap.abandoned"]);
    }

    [Fact]
    public async Task UntrackedReadsUnavailableFlipsWhenTheServerRefusesClientCachingInATransaction()
    {
        var clientName = "rnc-obs-untracked-" + Guid.NewGuid().ToString("N");
        var mux = new FakeMultiplexer(clientName);
        mux.Add(7000, isReplica: false);
        var connection = FakeRedis.Connection(mux);
        var armer = new TrackingArmer(connection, NullLogger<TrackingArmer>.Instance, Timeout.InfiniteTimeSpan);
        var options = new RedisNearCacheOptions();
        // A prefix makes the key below uncacheable, which is the only thing that takes the untracked read path.
        options.KeyPrefixes.Add("app:");
        await using var cache = new Facade(connection, armer, new SilentListener(),
            Microsoft.Extensions.Options.Options.Create(options), NullLogger<Facade>.Instance);
        await cache.Ready;

        Assert.False(cache.Statistics.UntrackedReadsUnavailable);
        Assert.Equal(0, MeterProbe.Collect(clientName).Longs["redisnearcache.untracked_reads.unavailable"]);

        mux.ExecFailure = new RedisServerException("EXECABORT Transaction discarded because of previous errors.");
        Assert.Equal("v1", await cache.GetAsync<string>("outside-the-prefixes"));

        Assert.True(cache.Statistics.UntrackedReadsUnavailable);
        Assert.Equal(1, MeterProbe.Collect(clientName).Longs["redisnearcache.untracked_reads.unavailable"]);
    }

    // --- serializer failures --------------------------------------------------------------------------------

    [Fact]
    public async Task ASerializerThatThrowsOnWriteIsCountedAndTheExceptionStillReachesTheCaller()
    {
        await using var rig = await Rig.StartAsync(o => o.Serializer = new ThrowingSerializer(onSerialize: true, onDeserialize: false));

        // Unchanged, not swallowed and not wrapped: the caller sees exactly what its own serializer threw.
        await Assert.ThrowsAsync<SerializerBoom>(async () => await rig.Cache.SetAsync(Key, "v"));
        Assert.Equal(1, rig.Cache.Statistics.SerializerFailures);

        await Assert.ThrowsAsync<SerializerBoom>(async () => await rig.Cache.SetAsync(Key, "v", When.NotExists));
        Assert.Equal(2, rig.Cache.Statistics.SerializerFailures);
        Assert.Equal(2, rig.Collect().Longs["redisnearcache.serializer_failures"]);
    }

    [Fact]
    public async Task ASerializerThatThrowsOnReadIsCountedOnBothReadPathsAndStillThrows()
    {
        await using var rig = await Rig.StartAsync(o => o.Serializer = new ThrowingSerializer(onSerialize: false, onDeserialize: true));

        // GetAsync stores the bytes it read BEFORE deserializing them, so L1 holds the entry that TryGetLocal below
        // then trips over: one failure per read path, from one Redis read.
        await Assert.ThrowsAsync<SerializerBoom>(async () => await rig.Cache.GetAsync<string>(Key));
        Assert.Equal(1, rig.Cache.Statistics.SerializerFailures);

        Assert.Throws<SerializerBoom>(() => rig.Cache.TryGetLocal<string>(Key, out _));
        Assert.Equal(2, rig.Cache.Statistics.SerializerFailures);
        Assert.Equal(2, rig.Collect().Longs["redisnearcache.serializer_failures"]);

        // The raw read never touches the serializer, so it must be unaffected.
        Assert.NotNull(await rig.Cache.GetBytesAsync(Key));
        Assert.Equal(2, rig.Cache.Statistics.SerializerFailures);
    }

    // --- L1 refusal and pre-arm plumbing --------------------------------------------------------------------

    /// <summary>
    /// The plumbing the refusal detection in <c>L1Cache.Set</c> is built on: the counter and the retained size limit.
    /// The detection itself is exercised by <see cref="L1CacheSizeAccountingTests"/>; what is asserted here is that
    /// the pieces it needs exist and behave, and that nothing about <c>Set</c> or <c>Clear</c> changed in adding them.
    /// </summary>
    [Fact]
    public void TheL1RefusalPlumbingCountsAndSetAndClearStillBehave()
    {
        var options = new RedisNearCacheOptions();
        using var l1 = new Caching.L1Cache(options);

        // The limit has to be retained, because "the value is bigger than the whole byte budget" is a legitimate
        // refusal that must NOT be counted, and without the limit that case cannot be told from the drift.
        Assert.Equal(options.L1SizeLimit, l1.SizeLimit);
        Assert.Equal(0, l1.StoreRefusals);

        l1.CountStoreRefusal();
        l1.CountStoreRefusal();
        Assert.Equal(2, l1.StoreRefusals);

        // Set still stores, and Clear still empties: the plumbing is additive. A second Clear of an empty cache is a
        // no-op rather than anything the refusal check could notice.
        l1.Set(Key, [1, 2, 3]);
        Assert.True(l1.TryGet(Key, out _));
        l1.Clear();
        Assert.False(l1.TryGet(Key, out _));
        l1.Clear();
        Assert.Equal(0, l1.Count);
        Assert.Equal(2, l1.StoreRefusals);
    }

    [Fact]
    public void ABytesBudgetIsTheRetainedLimit()
    {
        var options = new RedisNearCacheOptions { L1SizeLimitBytes = 4096 };
        using var l1 = new Caching.L1Cache(options);
        Assert.Equal(4096, l1.SizeLimit);
    }

    /// <summary>
    /// The pre-arm failure count is on the armer, not the statistics, because the armer is built before the facade.
    /// Broadcast mode never pre-arms a replica, so its answer is a constant 0 rather than a counter nobody touches.
    /// </summary>
    [Fact]
    public async Task PreArmFailuresIsReadableFromBothArmersAndStartsAtZero()
    {
        var mux = new FakeMultiplexer("rnc-obs-prearm-" + Guid.NewGuid().ToString("N"));
        mux.Add(7000, isReplica: false);
        var connection = FakeRedis.Connection(mux);
        await using var armer = new TrackingArmer(connection, NullLogger<TrackingArmer>.Instance, Timeout.InfiniteTimeSpan);
        Assert.Equal(0, armer.PreArmFailures);

        ITrackingArmer contract = armer;
        Assert.Equal(0, contract.PreArmFailures);
    }

    // --- health check ---------------------------------------------------------------------------------------

    [Fact]
    public async Task TheHealthCheckOfTheRealFacadeNamesTheLostEndpoints()
    {
        await using var rig = await Rig.StartAsync();
        var check = new RedisNearCacheHealthCheck(rig.Cache);
        var context = new HealthCheckContext
        {
            Registration = new HealthCheckRegistration("rnc", _ => throw new InvalidOperationException("not used"), HealthStatus.Unhealthy, tags: null),
        };

        var healthy = await check.CheckHealthAsync(context);
        Assert.Equal(HealthStatus.Healthy, healthy.Status);
        Assert.Equal(16, healthy.Data.Count);
        Assert.Equal(string.Empty, healthy.Data["lostEndpoints"]);
        Assert.Equal(0L, healthy.Data["lostEndpointCount"]);
        Assert.Equal(0d, healthy.Data["passThroughSeconds"]);

        rig.Armer.RaiseLost(A);
        await Task.Delay(30);

        var degraded = await check.CheckHealthAsync(context);
        Assert.Equal(HealthStatus.Degraded, degraded.Status);
        Assert.Equal(16, degraded.Data.Count);
        Assert.Equal(1L, degraded.Data["lostEndpointCount"]);
        Assert.Contains(A.ToString()!, (string)degraded.Data["lostEndpoints"], StringComparison.Ordinal);
        Assert.True((double)degraded.Data["passThroughSeconds"] > 0);
    }

    [Fact]
    public async Task ADisposedCacheReportsNoPassThroughDurationRatherThanOneThatKeepsGrowing()
    {
        var rig = await Rig.StartAsync();
        rig.Armer.RaiseLost(A);
        await Task.Delay(30);
        Assert.True(rig.Cache.Statistics.PassThroughSeconds > 0);

        await rig.DisposeAsync();

        // The statistics object outlives the cache; a duration that went on growing for the life of the process would
        // be the one number here that is actively wrong.
        Assert.Equal(0d, rig.Cache.Statistics.PassThroughSeconds);
        Assert.Equal(0, rig.Cache.Statistics.LostEndpointCount);
    }
}
