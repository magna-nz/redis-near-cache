using System.Net;
using Microsoft.Extensions.Logging.Abstractions;
using RedisNearCache.Internal;
using Xunit;
using Facade = RedisNearCache.Caching.RedisNearCache;

namespace RedisNearCache.UnitTests;

/// <summary>
/// The masters of a deployment are armed concurrently at start, each raising its own <c>Armed(Initial)</c>, and an
/// initial arm announces no loss first. The first of those events used to settle startup: from then on reads skipped
/// the wait for <c>Ready</c> and were stored, including reads routed to a master whose <c>CLIENT TRACKING ON</c> had
/// not been sent yet. Such an entry is tracked by nobody and no flush follows (<c>Armed(Initial)</c> never flushes), so
/// it was served until its TTL or <c>L1MaxAge</c> whatever was written to the key. A scripted armer holds the second
/// master's arm open to land a read exactly there.
/// </summary>
public class InitialArmWindowTests
{
    private const string Key = "key-served-by-b";
    private static readonly EndPoint A = new DnsEndPoint("a", 7001);
    private static readonly EndPoint B = new DnsEndPoint("b", 7002);

    /// <summary>The armer's start over two masters: A is armed at once, B when the test says so (or fails).</summary>
    private sealed class TwoMasterArmer : ITrackingArmer
    {
        public event Action<TrackingArmedEvent>? Armed;
        public event Action<EndPoint>? TrackingLost;
#pragma warning disable CS0067 // the contract has it; this script never removes an endpoint
        public event Action<EndPoint>? EndpointRemoved;
#pragma warning restore CS0067

        public IReadOnlyDictionary<EndPoint, long> RedirectTargets { get; } = new Dictionary<EndPoint, long>();
        public IReadOnlyDictionary<EndPoint, long> ReplicaRedirectTargets { get; } = new Dictionary<EndPoint, long>();

        public TaskCompletionSource FirstArmed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Completes B's arm: true arms it, false makes it fail the way <c>RearmAllAsync</c> reports a partial failure.</summary>
        public TaskCompletionSource<bool> FinishB { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Raised between A's arm and B's, e.g. a connection event's re-arm of A overlapping the start.</summary>
        public Action<TwoMasterArmer>? WhileBIsArming { get; init; }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            Armed?.Invoke(new TrackingArmedEvent(A, 1, ArmReason.Initial));
            WhileBIsArming?.Invoke(this);
            FirstArmed.SetResult();
            if (await FinishB.Task.ConfigureAwait(false))
                Armed?.Invoke(new TrackingArmedEvent(B, 2, ArmReason.Initial));
            else
                TrackingLost?.Invoke(B); // MarkLostAndRetryLater, before the start returns
        }

        public void RaiseArmed(EndPoint endPoint, ArmReason reason) => Armed?.Invoke(new TrackingArmedEvent(endPoint, 3, reason));
        public void RaiseLost(EndPoint endPoint) => TrackingLost?.Invoke(endPoint);

        public Task RearmAsync(EndPoint endPoint, ArmReason reason, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task RearmAllAsync(ArmReason reason, CancellationToken cancellationToken) => Task.CompletedTask;

        /// <summary>
        /// Releases an arm still in flight, as the real armer's dispose cancels one. Without it a test whose
        /// assertion fails before it finishes B would hang here: the facade's dispose awaits <c>Ready</c>.
        /// </summary>
        public ValueTask DisposeAsync()
        {
            FinishB.TrySetResult(false);
            return ValueTask.CompletedTask;
        }
    }

    private static (Facade Cache, FakeMultiplexer Mux) Build(TwoMasterArmer armer)
    {
        var mux = new FakeMultiplexer("rnc-unit");
        mux.Add(7001, isReplica: false);
        mux.Add(7002, isReplica: false);
        var cache = new Facade(FakeRedis.Connection(mux), armer, new SilentListener(),
            Microsoft.Extensions.Options.Options.Create(new RedisNearCacheOptions()), NullLogger<Facade>.Instance);
        return (cache, mux);
    }

    [Fact]
    public async Task AReadAfterTheFirstInitialArmWaitsForTheWholeStartAndIsNotStoredBeforeIt()
    {
        var armer = new TwoMasterArmer();
        var (cache, mux) = Build(armer);
        await using var _ = cache;
        await armer.FirstArmed.Task;

        Assert.False(cache.Ready.IsCompleted, "precondition: B is still being armed");
        Assert.False(cache.IsCoherent, "one armed master of two is not a coherent cache");

        var read = cache.GetAsync<string>(Key).AsTask();
        var early = await Task.WhenAny(read, Task.Delay(300)) == read;
        Assert.False(early, "a read issued while a master is still being armed was served instead of waiting for Ready");
        Assert.Equal(0, mux.StringGetCalls);
        Assert.False(cache.TryGetLocal<string>(Key, out string? _));

        armer.FinishB.SetResult(true);
        await cache.Ready;
        Assert.Equal("v1", await read);
        Assert.True(cache.IsCoherent);
        // The read went to Redis only after every master was armed, so this entry is a tracked one.
        Assert.True(cache.TryGetLocal<string>(Key, out string? _), "the cache does not cache after a clean start");
        Assert.Equal(0, cache.Statistics.Flushes);
    }

    [Fact]
    public async Task AnotherArmOfAnArmedMasterDuringTheStartDoesNotEnableCachingEither()
    {
        // A's connection blips while B is still arming: its re-arm is lost-then-armed with a non-initial reason, which
        // settles startup (reads stop waiting for Ready) but must not let them be stored.
        var armer = new TwoMasterArmer
        {
            WhileBIsArming = a =>
            {
                a.RaiseLost(A);
                a.RaiseArmed(A, ArmReason.InteractiveRestored);
            },
        };
        var (cache, _) = Build(armer);
        await using var __ = cache;
        await armer.FirstArmed.Task;

        Assert.False(cache.IsCoherent);
        Assert.Equal("v1", await cache.GetAsync<string>(Key).AsTask().WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.False(cache.TryGetLocal<string>(Key, out string? _), "a read was stored while a master was still being armed");

        armer.FinishB.SetResult(true);
        await cache.Ready;
        Assert.True(cache.IsCoherent);
        Assert.Equal("v1", await cache.GetAsync<string>(Key));
        Assert.True(cache.TryGetLocal<string>(Key, out string? _));
    }

    [Fact]
    public async Task AMasterThatCouldNotBeArmedAtStartKeepsTheCacheInPassThroughUntilItIs()
    {
        var armer = new TwoMasterArmer();
        var (cache, _) = Build(armer);
        await using var __ = cache;
        await armer.FirstArmed.Task;

        armer.FinishB.SetResult(false);
        await cache.Ready; // a partial failure does not fault the start
        Assert.False(cache.IsCoherent);
        Assert.Equal("v1", await cache.GetAsync<string>(Key));
        Assert.False(cache.TryGetLocal<string>(Key, out string? _));

        armer.RaiseArmed(B, ArmReason.Recovered);
        Assert.True(cache.IsCoherent);
        Assert.Equal("v1", await cache.GetAsync<string>(Key));
        Assert.True(cache.TryGetLocal<string>(Key, out string? _));
    }

    [Fact]
    public async Task ACleanStartIsCoherentWithoutAnyReadHavingSettledIt()
    {
        var armer = new TwoMasterArmer();
        var (cache, _) = Build(armer);
        await using var __ = cache;
        armer.FinishB.SetResult(true);
        await cache.Ready;

        Assert.True(cache.IsCoherent);
        await cache.WaitForCoherenceAsync().WaitAsync(TimeSpan.FromSeconds(5));
    }
}
