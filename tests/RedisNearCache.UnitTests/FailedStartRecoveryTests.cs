using System.Collections.Concurrent;
using System.Net;
using System.Reflection;
using Microsoft.Extensions.Logging.Abstractions;
using RedisNearCache.Internal;
using RedisNearCache.Tracking;
using StackExchange.Redis;
using Xunit;
using Facade = RedisNearCache.Caching.RedisNearCache;

namespace RedisNearCache.UnitTests;

/// <summary>
/// An application that starts while Redis is unreachable (<c>abortConnect=false</c>) used to keep a near cache that
/// never cached again: the subscription threw, so the armer was never started, hooked no event and ran no loop. And an
/// armer whose start did run but found no connected master threw before starting its reconcile sweep and its replica
/// pre-arm, for good (a start runs once). These tests pin the recovery of each layer.
/// </summary>
public class FailedStartRecoveryTests
{
    private const string Key = "k";
    private static readonly EndPoint A = new DnsEndPoint("a", 7001);

    private sealed class FlakyListener(int failures) : IInvalidationListener
    {
        private int _calls;
        public int Calls => Volatile.Read(ref _calls);
#pragma warning disable CS0067
        public event Action<string>? KeyInvalidated;
        public event Action? FlushAll;
#pragma warning restore CS0067
        public Task StartAsync(CancellationToken cancellationToken) =>
            Interlocked.Increment(ref _calls) <= failures
                ? Task.FromException(new RedisConnectionException(ConnectionFailureType.UnableToConnect, "no connection is available"))
                : Task.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class RecordingArmer : ITrackingArmer
    {
        private int _starts;
        public int Starts => Volatile.Read(ref _starts);
        public Exception? StartFailure { get; init; }
        /// <summary>Raised from inside <see cref="StartAsync"/>, before it fails: a recovery that beat the failure.</summary>
        public Action<RecordingArmer>? BeforeStartFails { get; init; }

        public event Action<TrackingArmedEvent>? Armed;
        public event Action<EndPoint>? TrackingLost;
#pragma warning disable CS0067
        public event Action<EndPoint>? EndpointRemoved;
#pragma warning restore CS0067
        public IReadOnlyDictionary<EndPoint, long> RedirectTargets { get; } = new Dictionary<EndPoint, long>();
        public IReadOnlyDictionary<EndPoint, long> ReplicaRedirectTargets { get; } = new Dictionary<EndPoint, long>();

        public Task StartAsync(CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _starts);
            if (StartFailure is null)
            {
                Armed?.Invoke(new TrackingArmedEvent(A, 1, ArmReason.Initial));
                return Task.CompletedTask;
            }

            BeforeStartFails?.Invoke(this);
            return Task.FromException(StartFailure);
        }

        public void RaiseArmed(ArmReason reason) => Armed?.Invoke(new TrackingArmedEvent(A, 2, reason));
        public void RaiseLost() => TrackingLost?.Invoke(A);
        public Task RearmAsync(EndPoint endPoint, ArmReason reason, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task RearmAllAsync(ArmReason reason, CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private static Facade Build(ITrackingArmer armer, IInvalidationListener listener, TimeSpan? retry = null)
    {
        var mux = new FakeMultiplexer("rnc-unit");
        mux.Add(7001, isReplica: false);
        var options = new RedisNearCacheOptions();
        options.TestHooks.StartRetryInterval = retry ?? TimeSpan.FromMilliseconds(20);
        return new Facade(FakeRedis.Connection(mux), armer, listener, Microsoft.Extensions.Options.Options.Create(options), NullLogger<Facade>.Instance);
    }

    private static async Task<bool> UntilAsync(Func<bool> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (condition()) return true;
            await Task.Delay(10);
        }

        return condition();
    }

    // --- facade -------------------------------------------------------------------------------------------

    [Fact]
    public async Task ASubscriptionThatFailsAtStartIsRetriedAndTheArmerIsStartedOnceItSucceeds()
    {
        var listener = new FlakyListener(failures: 3);
        var armer = new RecordingArmer();
        await using var cache = Build(armer, listener);

        await Assert.ThrowsAsync<RedisConnectionException>(() => cache.Ready);
        Assert.False(cache.IsCoherent);
        // Pass-through meanwhile: reads are served, nothing is stored.
        Assert.Equal("v1", await cache.GetAsync<string>(Key));
        Assert.False(cache.TryGetLocal<string>(Key, out string? _));

        Assert.True(await UntilAsync(() => cache.IsCoherent, TimeSpan.FromSeconds(10)),
            $"the cache never recovered from a failed subscription: listener starts={listener.Calls}, armer starts={armer.Starts}");
        Assert.Equal(4, listener.Calls);
        Assert.Equal(1, armer.Starts);
        Assert.True(cache.Ready.IsFaulted, "Ready reports the start the application saw; it does not change afterwards");

        Assert.Equal("v1", await cache.GetAsync<string>(Key));
        Assert.True(cache.TryGetLocal<string>(Key, out string? _), "the recovered cache does not cache");
        await cache.WaitForCoherenceAsync().WaitAsync(TimeSpan.FromSeconds(5));
    }

    [Fact]
    public async Task DisposeStopsTheRetryOfAFailedStart()
    {
        var listener = new FlakyListener(failures: int.MaxValue);
        var armer = new RecordingArmer();
        var cache = Build(armer, listener);
        await Assert.ThrowsAsync<RedisConnectionException>(() => cache.Ready);
        Assert.True(await UntilAsync(() => listener.Calls >= 3, TimeSpan.FromSeconds(10)));

        await cache.DisposeAsync();
        var callsAtDispose = listener.Calls;
        await Task.Delay(200);
        Assert.InRange(listener.Calls, callsAtDispose, callsAtDispose + 1); // at most the attempt already in flight
        Assert.Equal(0, armer.Starts);
    }

    [Fact]
    public async Task AnArmerWhoseStartFailedLeavesAPassThroughThatItsNextArmEnds()
    {
        var armer = new RecordingArmer { StartFailure = new RedisNearCacheTrackingException("No connected master to arm CLIENT TRACKING on (Initial).") };
        await using var cache = Build(armer, new SilentListener());

        await Assert.ThrowsAsync<RedisNearCacheTrackingException>(() => cache.Ready);
        Assert.False(cache.IsCoherent);
        Assert.Equal("v1", await cache.GetAsync<string>(Key));
        Assert.False(cache.TryGetLocal<string>(Key, out string? _));

        armer.RaiseLost();
        armer.RaiseArmed(ArmReason.TopologyChanged);
        Assert.True(cache.IsCoherent);
        Assert.Equal("v1", await cache.GetAsync<string>(Key));
        Assert.True(cache.TryGetLocal<string>(Key, out string? _));
        Assert.Equal(1, armer.Starts);
    }

    [Fact]
    public async Task AnArmThatSucceedsJustBeforeTheStartReportsItsFailureIsNotUndoneByIt()
    {
        // The armer's retry loop (or its reconcile) can arm the node between the start's last failed attempt and the
        // start's exception reaching the facade. The failure must not then mark the cache degraded: nothing would be
        // left to clear it.
        var armer = new RecordingArmer
        {
            StartFailure = new RedisNearCacheTrackingException("Could not arm CLIENT TRACKING on any of the 1 connected master(s)."),
            BeforeStartFails = a =>
            {
                a.RaiseLost();
                a.RaiseArmed(ArmReason.Recovered);
            },
        };
        await using var cache = Build(armer, new SilentListener());

        await Assert.ThrowsAsync<RedisNearCacheTrackingException>(() => cache.Ready);
        Assert.True(cache.IsCoherent, "a failed start overwrote a recovery that had already happened");
        Assert.Equal("v1", await cache.GetAsync<string>(Key));
        Assert.True(cache.TryGetLocal<string>(Key, out string? _));
    }

    // --- TrackingArmer ------------------------------------------------------------------------------------

    [Fact]
    public async Task AnArmerThatFoundNoConnectedMasterAtStartStillSweepsAndArmsItWhenItConnects()
    {
        var mux = new FakeMultiplexer("rnc-unit");
        var master = mux.Add(7000, isReplica: false);
        master.IsConnected = false;
        var replica = mux.Add(7001, isReplica: true);
        replica.IsConnected = false;

        var events = new ConcurrentQueue<string>();
        await using var armer = new TrackingArmer(FakeRedis.Connection(mux), NullLogger<TrackingArmer>.Instance, TimeSpan.FromMilliseconds(30));
        armer.TrackingLost += ep => events.Enqueue($"lost {ep}");
        armer.Armed += e => events.Enqueue($"armed {e.EndPoint} {e.Reason}");

        await Assert.ThrowsAsync<RedisNearCacheTrackingException>(() => armer.StartAsync(CancellationToken.None));
        Assert.Empty(armer.RedirectTargets);

        // Redis comes up. No ConnectionRestored or ConfigurationChanged is raised here on purpose: the sweep alone
        // must find the master, and it only runs if the failed start still started it.
        master.IsConnected = true;
        replica.IsConnected = true;

        Assert.True(await UntilAsync(() => armer.RedirectTargets.ContainsKey(master.EndPoint), TimeSpan.FromSeconds(10)),
            "the reconcile sweep never armed a master that connected after a failed start: " + string.Join(" | ", events));
        // Announced lost before it is armed (the reconcile and the arm each say so; the facade's lost set is a set).
        var seen = events.ToArray();
        Assert.Equal($"lost {master.EndPoint}", seen[0]);
        Assert.Equal($"armed {master.EndPoint} {ArmReason.TopologyChanged}", seen[^1]);
        Assert.Single(seen, e => e.StartsWith("armed", StringComparison.Ordinal));
        Assert.True(await UntilAsync(() => armer.ReplicaRedirectTargets.ContainsKey(replica.EndPoint), TimeSpan.FromSeconds(10)),
            "the replica pre-arm never ran after a failed start");
    }

    // --- InvalidationListener -----------------------------------------------------------------------------

    /// <summary>A multiplexer whose subscriber refuses the first <c>SubscribeAsync</c> calls, as one with no connection does.</summary>
    private sealed class SubscriberScript
    {
        private int _subscribeCalls;
        public int SubscribeCalls => Volatile.Read(ref _subscribeCalls);
        public int UnsubscribeCalls;
        public int Failures { get; init; }
        public List<Action<RedisChannel, RedisValue>> Handlers { get; } = new();

        public IConnectionMultiplexer Multiplexer()
        {
            var subscriber = FakeProxy.Create<ISubscriber>(HandleSubscriber);
            return FakeProxy.Create<IConnectionMultiplexer>((method, _) => method.Name switch
            {
                "GetSubscriber" => subscriber,
                "DisposeAsync" => ValueTask.CompletedTask,
                _ => throw new NotSupportedException(method.Name),
            });
        }

        private object? HandleSubscriber(MethodInfo method, object?[] args)
        {
            switch (method.Name)
            {
                case "SubscribeAsync":
                    if (Interlocked.Increment(ref _subscribeCalls) <= Failures)
                        return Task.FromException(new RedisConnectionException(ConnectionFailureType.UnableToConnect, "no connection is available"));
                    lock (Handlers) Handlers.Add((Action<RedisChannel, RedisValue>)args[1]!);
                    return Task.CompletedTask;
                case "Unsubscribe":
                    Interlocked.Increment(ref UnsubscribeCalls);
                    lock (Handlers) Handlers.Remove((Action<RedisChannel, RedisValue>)args[1]!);
                    return null;
                case "UnsubscribeAsync":
                    lock (Handlers) Handlers.Remove((Action<RedisChannel, RedisValue>)args[1]!);
                    return Task.CompletedTask;
                default:
                    throw new NotSupportedException($"ISubscriber.{method.Name}");
            }
        }
    }

    [Fact]
    public async Task AListenerWhoseSubscriptionFailedCanBeStartedAgainAndSubscribesExactlyOnce()
    {
        var script = new SubscriberScript { Failures = 1 };
        var ctor = typeof(RedisNearCacheConnection).GetConstructor(
            BindingFlags.Instance | BindingFlags.NonPublic, [typeof(IConnectionMultiplexer), typeof(string)])!;
        var connection = (RedisNearCacheConnection)ctor.Invoke([script.Multiplexer(), "rnc-unit"]);
        await using var listener = new InvalidationListener(connection, NullLogger<InvalidationListener>.Instance);
        var invalidated = new List<string>();
        listener.KeyInvalidated += invalidated.Add;

        await Assert.ThrowsAsync<RedisConnectionException>(() => listener.StartAsync(CancellationToken.None));
        Assert.Equal(1, script.UnsubscribeCalls); // whatever the failed attempt left registered is withdrawn

        await listener.StartAsync(CancellationToken.None);
        await listener.StartAsync(CancellationToken.None); // started: a further call is a no-op, as before
        Assert.Equal(2, script.SubscribeCalls);

        var handler = Assert.Single(script.Handlers);
        handler(RedisChannel.Literal("__redis__:invalidate"), "some-key");
        Assert.Equal(["some-key"], invalidated);
    }
}
