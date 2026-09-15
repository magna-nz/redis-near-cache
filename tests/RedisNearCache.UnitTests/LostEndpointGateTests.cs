using System.Net;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using RedisNearCache.Internal;
using Xunit;
using Facade = RedisNearCache.Caching.RedisNearCache;

namespace RedisNearCache.UnitTests;

/// <summary>
/// The facade's pass-through gate against a scripted armer: lifecycle events are raised directly, in orders the real
/// armer may or may not produce, and the gate must track the SET of lost endpoints rather than a count of events.
/// Duplicate losses, arms and removals of endpoints that were never lost, and removal racing re-arm are the orderings
/// an event counter gets wrong.
/// </summary>
public class LostEndpointGateTests
{
    private const string Key = "k";
    private static readonly EndPoint A = new DnsEndPoint("a", 7001);
    private static readonly EndPoint B = new DnsEndPoint("b", 7002);
    private static readonly EndPoint C = new DnsEndPoint("c", 7003);

    private sealed class ScriptedArmer : ITrackingArmer
    {
        public event Action<TrackingArmedEvent>? Armed;
        public event Action<EndPoint>? TrackingLost;
        public event Action<EndPoint>? EndpointRemoved;

        public IReadOnlyDictionary<EndPoint, long> RedirectTargets { get; } = new Dictionary<EndPoint, long>();
        public IReadOnlyDictionary<EndPoint, long> ReplicaRedirectTargets { get; } = new Dictionary<EndPoint, long>();

        public void RaiseArmed(EndPoint endPoint, ArmReason reason = ArmReason.InteractiveRestored) =>
            Armed?.Invoke(new TrackingArmedEvent(endPoint, 1, reason));
        public void RaiseLost(EndPoint endPoint) => TrackingLost?.Invoke(endPoint);
        public void RaiseRemoved(EndPoint endPoint) => EndpointRemoved?.Invoke(endPoint);

        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task RearmAsync(EndPoint endPoint, ArmReason reason, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task RearmAllAsync(ArmReason reason, CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class Rig : IAsyncDisposable
    {
        public required ScriptedArmer Armer { get; init; }
        public required Facade Cache { get; init; }
        public required RedisNearCacheOptions Options { get; init; }

        public static async Task<Rig> StartAsync()
        {
            var mux = new FakeMultiplexer("rnc-unit");
            mux.Add(7000, isReplica: false);
            var armer = new ScriptedArmer();
            var options = new RedisNearCacheOptions();
            var rig = new Rig
            {
                Armer = armer,
                Options = options,
                Cache = new Facade(FakeRedis.Connection(mux), armer, new SilentListener(), Microsoft.Extensions.Options.Options.Create(options), NullLogger<Facade>.Instance),
            };
            await rig.Cache.Ready;
            foreach (var endPoint in new[] { A, B, C }) armer.RaiseArmed(endPoint, ArmReason.Initial);
            Assert.True(rig.Cache.IsCoherent, "precondition: coherent once every endpoint is armed");
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

        /// <summary>Asserts the gate is closed: not coherent, and a read neither caches nor serves from L1.</summary>
        public async Task AssertPassThroughAsync(string because)
        {
            Assert.False(Cache.IsCoherent, because);
            Assert.False(await ReadCachesAsync(), because);
        }

        public async Task AssertCachingAsync(string because)
        {
            Assert.True(Cache.IsCoherent, because);
            Assert.True(await ReadCachesAsync(), because);
        }

        public ValueTask DisposeAsync() => Cache.DisposeAsync();
    }

    [Fact]
    public async Task DuplicateLossOfOneEndpointIsClearedByASingleArm()
    {
        await using var rig = await Rig.StartAsync();
        rig.Armer.RaiseLost(A);
        rig.Armer.RaiseLost(A);
        await rig.AssertPassThroughAsync("two losses of the same endpoint");

        rig.Armer.RaiseArmed(A);
        await rig.AssertCachingAsync("one arm answers every loss of that endpoint; the gate must not wait for a second");
    }

    [Fact]
    public async Task ArmOrRemovalOfAnEndpointThatWasNeverLostDoesNotOpenTheGate()
    {
        await using var rig = await Rig.StartAsync();
        rig.Armer.RaiseLost(A);
        rig.Armer.RaiseArmed(B, ArmReason.Recovered);
        rig.Armer.RaiseRemoved(C);
        await rig.AssertPassThroughAsync("A is still lost; B and C were never lost");

        rig.Armer.RaiseArmed(A);
        await rig.AssertCachingAsync("A re-armed");
    }

    [Fact]
    public async Task ArmsAndRemovalsOfHealthyEndpointsDoNotHideALaterLoss()
    {
        await using var rig = await Rig.StartAsync();
        // A counter decremented on every Armed/EndpointRemoved would sit below zero here and read zero after one loss.
        rig.Armer.RaiseArmed(B, ArmReason.Manual);
        rig.Armer.RaiseArmed(B, ArmReason.Promoted);
        rig.Armer.RaiseRemoved(C);
        await rig.AssertCachingAsync("nothing is lost");

        rig.Armer.RaiseLost(A);
        await rig.AssertPassThroughAsync("A is lost");
    }

    [Fact]
    public async Task RemovalAndReArmOfTheSameLostEndpointClearItOnce()
    {
        await using var rig = await Rig.StartAsync();
        rig.Armer.RaiseLost(A);
        rig.Armer.RaiseRemoved(A);
        rig.Armer.RaiseArmed(A, ArmReason.TopologyChanged);
        await rig.AssertCachingAsync("removed then re-armed");

        rig.Armer.RaiseLost(A);
        rig.Armer.RaiseArmed(A, ArmReason.TopologyChanged);
        rig.Armer.RaiseRemoved(A);
        await rig.AssertCachingAsync("re-armed then removed");

        rig.Armer.RaiseLost(A);
        await rig.AssertPassThroughAsync("a second clear of the same endpoint must not have left credit for a later loss");
    }

    [Fact]
    public async Task EveryLostEndpointMustBeClearedBeforeCachingResumes()
    {
        await using var rig = await Rig.StartAsync();
        rig.Armer.RaiseLost(A);
        rig.Armer.RaiseLost(B);
        rig.Armer.RaiseArmed(A);
        await rig.AssertPassThroughAsync("B is still lost");

        rig.Armer.RaiseRemoved(B);
        await rig.AssertCachingAsync("A armed and B removed");
    }

    [Fact]
    public async Task LossClosesTheGateBeforeTheFlushClearsL1()
    {
        await using var rig = await Rig.StartAsync();
        Assert.True(await rig.ReadCachesAsync(), "precondition: the key is cached");

        // The hook runs after the in-flight tracker is marked and BEFORE L1 is cleared, so the entry is still stored:
        // a read here must already refuse to serve it.
        bool? servedDuringFlush = null, coherentDuringFlush = null;
        rig.Options.TestHooks.InsideFlushHandler = () =>
        {
            servedDuringFlush = rig.Cache.TryGetLocal<string>(Key, out _);
            coherentDuringFlush = rig.Cache.IsCoherent;
        };
        rig.Armer.RaiseLost(A);

        Assert.False(servedDuringFlush ?? throw new InvalidOperationException("the loss did not flush"), "an entry was served while the flush for a loss was still running");
        Assert.False(coherentDuringFlush);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ArmOrRemovalOpensTheGateOnlyAfterTheFlush(bool removal)
    {
        await using var rig = await Rig.StartAsync();
        rig.Armer.RaiseLost(A);

        bool? coherentDuringFlush = null;
        rig.Options.TestHooks.InsideFlushHandler = () => coherentDuringFlush = rig.Cache.IsCoherent;
        if (removal) rig.Armer.RaiseRemoved(A);
        else rig.Armer.RaiseArmed(A);

        Assert.False(coherentDuringFlush ?? throw new InvalidOperationException("clearing the loss did not flush"), "the gate opened before the flush that must precede it");
        rig.Options.TestHooks.InsideFlushHandler = null;
        await rig.AssertCachingAsync("the loss was cleared");
    }

    [Fact]
    public async Task ConcurrentLifecycleEventsOnOtherEndpointsNeverOpenTheGateWhileOneIsLost()
    {
        await using var rig = await Rig.StartAsync();
        var pinned = new DnsEndPoint("pinned", 7999);
        rig.Armer.RaiseLost(pinned);

        // Each worker owns one endpoint and raises its events in some order, as the armer does per endpoint, while
        // every worker runs concurrently with the others. The pinned loss must hold the gate closed throughout.
        const int workers = 8, rounds = 2_000;
        using var stop = new CancellationTokenSource();
        var openedWhileLost = 0;
        var checker = Task.Run(() =>
        {
            while (!stop.IsCancellationRequested)
            {
                if (rig.Cache.IsCoherent || rig.Cache.TryGetLocal<string>(Key, out _)) Interlocked.Increment(ref openedWhileLost);
            }
        });

        var tasks = Enumerable.Range(0, workers).Select(w => Task.Run(() =>
        {
            var endPoint = new DnsEndPoint($"w{w}", 8000 + w);
            for (var i = 0; i < rounds; i++)
            {
                switch (i % 4)
                {
                    case 0: rig.Armer.RaiseLost(endPoint); rig.Armer.RaiseLost(endPoint); rig.Armer.RaiseArmed(endPoint); break;
                    case 1: rig.Armer.RaiseArmed(endPoint, ArmReason.Manual); break;
                    case 2: rig.Armer.RaiseLost(endPoint); rig.Armer.RaiseRemoved(endPoint); rig.Armer.RaiseArmed(endPoint); break;
                    default: rig.Armer.RaiseRemoved(endPoint); break;
                }
            }
        })).ToArray();
        try
        {
            await Task.WhenAll(tasks);
        }
        finally
        {
            // A worker that throws must fail the test, not leave the checker spinning forever.
            stop.Cancel();
            await checker;
        }

        Assert.Equal(0, openedWhileLost);
        await rig.AssertPassThroughAsync("the pinned endpoint is still lost after the storm");

        rig.Armer.RaiseArmed(pinned);
        await rig.AssertCachingAsync("every worker ended with its endpoint cleared and the pinned one is now armed");
    }
}
