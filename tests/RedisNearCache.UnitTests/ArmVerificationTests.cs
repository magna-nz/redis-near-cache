using System.Collections.Concurrent;
using Microsoft.Extensions.Logging.Abstractions;
using RedisNearCache.Internal;
using RedisNearCache.Tracking;
using StackExchange.Redis;
using Xunit;

namespace RedisNearCache.UnitTests;

/// <summary>
/// Everything else in the armer reacts to a connection event. This is the one check that assumes no event will come:
/// a subscriber connection can die silently - a NAT or load balancer dropping the one flow that never sends anything
/// - and then the server keeps redirecting every invalidation to a client that is gone while the facade serves L1 and
/// reports itself coherent. Measured at about 67 s of silent staleness before StackExchange.Redis's own keepalive
/// noticed, and unbounded when the reconnect never completes, because then no event is raised at all.
/// </summary>
public class ArmVerificationTests
{
    private static readonly TimeSpan Sweep = TimeSpan.FromMilliseconds(30);

    private sealed record Rig(FakeMultiplexer Mux, FakeServer Master, TrackingArmer Armer, ConcurrentQueue<string> Events) : IAsyncDisposable
    {
        public ValueTask DisposeAsync() => Armer.DisposeAsync();

        public string History => string.Join(" | ", Events.ToArray());
    }

    private static async Task<Rig> StartAsync()
    {
        var mux = new FakeMultiplexer("rnc-unit");
        var master = mux.Add(7000, isReplica: false);
        var events = new ConcurrentQueue<string>();
        var armer = new TrackingArmer(FakeRedis.Connection(mux), NullLogger<TrackingArmer>.Instance, Sweep);
        armer.TrackingLost += ep => events.Enqueue($"lost {ep}");
        armer.Armed += e => events.Enqueue($"armed {e.Reason} redirect={e.RedirectClientId}");
        armer.EndpointRemoved += ep => events.Enqueue($"removed {ep}");
        await armer.StartAsync(CancellationToken.None);
        Assert.Equal(master.SubscriberId, armer.RedirectTargets[master.EndPoint]);
        events.Clear(); // the initial arm is not what these tests are about
        return new Rig(mux, master, armer, events);
    }

    private static async Task<bool> UntilAsync(Func<bool> condition, TimeSpan? timeout = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(10));
        while (DateTime.UtcNow < deadline)
        {
            if (condition()) return true;
            await Task.Delay(10);
        }

        return condition();
    }

    /// <summary>
    /// The subscriber reconnected with a new client id and nothing said so. The server is redirecting to the old,
    /// dead one, so the sweep must announce the loss (the facade stops serving L1) and re-arm with the new id.
    /// </summary>
    [Fact]
    public async Task ASubscriberIdThatChangedWithoutAnEventIsFoundAndReArmed()
    {
        await using var rig = await StartAsync();
        var oldId = rig.Master.SubscriberId;
        rig.Master.SubscriberId = oldId + 41;

        var reArmed = await UntilAsync(() => rig.Armer.RedirectTargets.TryGetValue(rig.Master.EndPoint, out var id) && id == oldId + 41);
        Assert.True(reArmed, $"the sweep never re-armed the endpoint with the new subscriber id. events: {rig.History}");

        var history = rig.Events.ToArray();
        Assert.Contains($"lost {rig.Master.EndPoint}", history);
        Assert.Contains($"armed {ArmReason.VerificationFailed} redirect={oldId + 41}", history);
        // The loss must be announced BEFORE the arm, or the facade would keep caching across the gap.
        Assert.True(
            Array.FindIndex(history, e => e.StartsWith("lost", StringComparison.Ordinal))
            < Array.FindIndex(history, e => e.StartsWith("armed", StringComparison.Ordinal)),
            $"the re-arm was announced before the loss: {rig.History}");
    }

    /// <summary>The server dropped our subscriber connection altogether: it is no longer in CLIENT LIST.</summary>
    [Fact]
    public async Task ASubscriberConnectionMissingFromClientListIsFoundAndReArmed()
    {
        await using var rig = await StartAsync();
        rig.Master.SubscriberListed = false;

        var lost = await UntilAsync(() => rig.Events.Any(e => e.StartsWith("lost", StringComparison.Ordinal)));
        Assert.True(lost, $"a subscriber connection the server had dropped was never noticed. events: {rig.History}");
        // It cannot arm while the subscriber is missing, so the endpoint stays announced-lost (the facade is in
        // pass-through) and no Armed follows. The recorded redirect id is only dropped once the retry ladder gives up.
        Assert.DoesNotContain(rig.Events, e => e.StartsWith("armed", StringComparison.Ordinal));

        // Once the subscriber is back (a new id), the retry arms it and the facade is released.
        rig.Master.SubscriberListed = true;
        rig.Master.SubscriberId += 7;
        var reArmed = await UntilAsync(
            () => rig.Armer.RedirectTargets.TryGetValue(rig.Master.EndPoint, out var id) && id == rig.Master.SubscriberId,
            TimeSpan.FromSeconds(20));
        Assert.True(reArmed, $"the endpoint was never armed again once its subscriber was back. events: {rig.History}");
    }

    /// <summary>
    /// The subscriber id is unchanged but the server says tracking is off (it was turned off underneath us, or the
    /// redirect was broken server-side). CLIENT LIST alone cannot see this; TRACKINGINFO can.
    /// </summary>
    [Fact]
    public async Task TrackingInfoReportingTrackingOffIsFoundAndReArmed()
    {
        await using var rig = await StartAsync();
        rig.Master.TrackingInfoFlags = "off";

        var lost = await UntilAsync(() => rig.Events.Any(e => e.StartsWith("lost", StringComparison.Ordinal)));
        Assert.True(lost, $"tracking reported off by the server was never noticed. events: {rig.History}");

        // The re-arm issues CLIENT TRACKING ON again; the fake still reports "off", so it keeps retrying rather than
        // claiming success. What matters is that the facade was told, and stays in pass-through.
        rig.Master.TrackingInfoFlags = "on";
        var reArmed = await UntilAsync(
            () => rig.Armer.RedirectTargets.ContainsKey(rig.Master.EndPoint),
            TimeSpan.FromSeconds(20));
        Assert.True(reArmed, $"the endpoint was never armed again once tracking was back on. events: {rig.History}");
    }

    /// <summary>
    /// A redirect id that disagrees with CLIENT LIST only in TRACKINGINFO's answer: the server is redirecting
    /// somewhere else entirely.
    /// </summary>
    [Fact]
    public async Task ARedirectPointingSomewhereElseIsFoundAndReArmed()
    {
        await using var rig = await StartAsync();
        rig.Master.TrackingInfoRedirect = rig.Master.SubscriberId + 1000;

        var lost = await UntilAsync(() => rig.Events.Any(e => e.StartsWith("lost", StringComparison.Ordinal)));
        Assert.True(lost, $"a redirect pointing at another client was never noticed. events: {rig.History}");
    }

    /// <summary>
    /// The steady state, and the one that matters most: a healthy arm must not be disturbed. The sweep runs many
    /// times here (30 ms apart) and must raise nothing at all - every spurious re-arm costs an L1 flush and a
    /// pass-through window.
    /// </summary>
    [Fact]
    public async Task AHealthyArmIsLeftAloneBySweepAfterSweep()
    {
        await using var rig = await StartAsync();
        var armedId = rig.Armer.RedirectTargets[rig.Master.EndPoint];

        await Task.Delay(TimeSpan.FromMilliseconds(30 * 25));

        Assert.True(rig.Events.IsEmpty, $"the sweep disturbed a healthy arm: {rig.History}");
        Assert.Equal(armedId, rig.Armer.RedirectTargets[rig.Master.EndPoint]);
    }

    /// <summary>
    /// A server that cannot answer is not evidence of a broken arm: it must be left alone (and retried next sweep),
    /// because flushing on a blip would cost a pass-through window for nothing.
    /// </summary>
    [Fact]
    public async Task AServerThatCannotAnswerIsNotTreatedAsABrokenArm()
    {
        await using var rig = await StartAsync();
        rig.Master.CommandHook = _ => Task.FromException(new RedisTimeoutException("no answer", CommandStatus.WaitingInBacklog));

        await Task.Delay(TimeSpan.FromMilliseconds(30 * 15));

        Assert.True(rig.Events.IsEmpty, $"a failed verification read was treated as a broken arm: {rig.History}");
        Assert.True(rig.Armer.RedirectTargets.ContainsKey(rig.Master.EndPoint));
    }

    /// <summary>A disconnected master is the connection events' business, not the sweep's.</summary>
    [Fact]
    public async Task ADisconnectedMasterIsNotVerified()
    {
        await using var rig = await StartAsync();
        rig.Master.SubscriberListed = false;
        rig.Master.IsConnected = false;

        await Task.Delay(TimeSpan.FromMilliseconds(30 * 15));

        Assert.True(rig.Events.IsEmpty, $"the sweep acted on a disconnected master: {rig.History}");
    }
}
