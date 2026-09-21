using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Text;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using RedisNearCache.Caching;
using RedisNearCache.Internal;
using RedisNearCache.Tracking;
using StackExchange.Redis;
using Xunit;
using Facade = RedisNearCache.Caching.RedisNearCache;

#pragma warning disable CS0618 // the simple exception constructors are the clearest way to fake a server reply here

namespace RedisNearCache.UnitTests;

/// <summary>
/// Collects the spans of exactly one cache instance from the <see cref="RedisNearCacheStatistics.ActivitySourceName"/>
/// source, the <see cref="MeterProbe"/> of the tracing side. Two things are recorded, and the difference matters:
/// <list type="bullet">
/// <item><see cref="Finished"/> - spans that STOPPED, filtered on this instance's <c>rnc.client_name</c> tag, because
/// other test classes run in parallel in the same process, own caches of their own and will start spans on a source of
/// the same name while this listener is registered.</item>
/// <item><see cref="RequestedUnder"/> - every span the source was ASKED to create, recorded from the sampling
/// callback, i.e. from inside <c>StartActivity</c> before an <see cref="Activity"/> exists and before any tag can have
/// been set. A test that has to prove <c>StartActivity</c> was never called cannot use the tag filter (there is no tag
/// yet) and cannot use a global count (parallel classes pollute it), so it scopes the operation under
/// <see cref="StartScope"/> and asks what was requested under that span.</item>
/// </list>
/// </summary>
internal sealed class ActivityProbe : IDisposable
{
    /// <summary>A source of the test's own, for <see cref="StartScope"/>; never the library's.</summary>
    public const string ScopeSourceName = "RedisNearCache.UnitTests.ActivityProbe";

    private readonly string _clientName;
    private readonly ActivityListener _listener;
    private readonly ActivitySource _scopes = new(ScopeSourceName);
    private readonly ConcurrentQueue<Activity> _finished = new();
    private readonly ConcurrentQueue<(string Name, ActivitySpanId Parent)> _requested = new();

    public ActivityProbe(string clientName)
    {
        _clientName = clientName;
        _listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == RedisNearCacheStatistics.ActivitySourceName || source.Name == ScopeSourceName,
            // Everything sampled in, so a test that asserts NO span was created can never pass because sampling
            // happened to drop it.
            Sample = OnSample,
            ActivityStopped = OnStopped,
        };
        ActivitySource.AddActivityListener(_listener);
    }

    /// <summary>The spans of this instance that have stopped, optionally only those with one name.</summary>
    public IReadOnlyList<Activity> Finished(string? name = null) =>
        _finished.Where(a => name is null || a.OperationName == name).ToArray();

    /// <summary>The single span of this instance with that name; fails if there is not exactly one.</summary>
    public Activity Single(string name) => Assert.Single(Finished(name));

    /// <summary>How many library spans were requested with <paramref name="parent"/> as their parent.</summary>
    public int RequestedUnder(ActivitySpanId parent) => _requested.Count(r => r.Parent == parent);

    /// <summary>The names of the library spans requested under <paramref name="parent"/>, for an assertion message.</summary>
    public string NamesUnder(ActivitySpanId parent) => string.Join(", ", _requested.Where(r => r.Parent == parent).Select(r => r.Name));

    /// <summary>
    /// A span of the test's own to run an operation under, so that anything the library starts during it is a child of
    /// it and attributable to this test alone. Never null: this probe's listener samples its own source in.
    /// </summary>
    public Activity StartScope(string name = "test.scope") =>
        _scopes.StartActivity(name) ?? throw new InvalidOperationException("the probe's own source was not listened to");

    private ActivitySamplingResult OnSample(ref ActivityCreationOptions<ActivityContext> options)
    {
        if (options.Source.Name == RedisNearCacheStatistics.ActivitySourceName)
        {
            _requested.Enqueue((options.Name, options.Parent.SpanId));
        }

        return ActivitySamplingResult.AllDataAndRecorded;
    }

    private void OnStopped(Activity activity)
    {
        if (activity.Source.Name != RedisNearCacheStatistics.ActivitySourceName) return;
        if ((string?)activity.GetTagItem(RedisNearCacheTracing.ClientNameTag) != _clientName) return;
        _finished.Enqueue(activity);
    }

    public void Dispose()
    {
        _listener.Dispose();
        _scopes.Dispose();
    }
}


/// <summary>
/// The smallest RESP3 server <c>BroadcastTracker</c> will arm against: it answers <c>HELLO 3</c>, <c>CLIENT ID</c>,
/// <c>CLIENT TRACKING ... BCAST</c> and <c>CLIENT TRACKINGINFO</c>, and nothing else. Needed because a broadcast arm
/// opens a real TCP socket of its own (outside the multiplexer, so outside <see cref="FakeMultiplexer"/>) and speaks
/// RESP3 on it; <see cref="FakeServer"/> only models the <see cref="StackExchange.Redis.IServer"/> surface. It listens
/// on a loopback port the test then registers with the fake multiplexer, so the tracker's topology view and the socket
/// it opens agree.
/// </summary>
internal sealed class FakeResp3Server : IDisposable
{
    private readonly TcpListener _listener;
    private readonly CancellationTokenSource _stop = new();
    private readonly List<Task> _clients = [];

    /// <summary>Set to false to make CLIENT TRACKINGINFO report flags the arm cannot accept.</summary>
    public bool TrackingVerifies { get; set; } = true;

    public FakeResp3Server()
    {
        _listener = new TcpListener(IPAddress.Loopback, 0);
        _listener.Start();
        Port = ((IPEndPoint)_listener.LocalEndpoint).Port;
        // No await anywhere in the caller's flow, so this must never be able to fault: everything inside is caught.
        _ = Task.Run(AcceptAsync, CancellationToken.None);
    }

    public int Port { get; }

    private async Task AcceptAsync()
    {
        try
        {
            while (!_stop.IsCancellationRequested)
            {
                var client = await _listener.AcceptTcpClientAsync(_stop.Token).ConfigureAwait(false);
                lock (_clients) _clients.Add(Task.Run(() => ServeAsync(client), CancellationToken.None));
            }
        }
        catch (Exception)
        {
            // Stopped, or the listener went away: there is nothing to report to and nobody awaits this task.
        }
    }

    private async Task ServeAsync(TcpClient client)
    {
        using (client)
        {
            try
            {
                var stream = client.GetStream();
                var reader = new StreamReader(stream, Encoding.UTF8);
                while (!_stop.IsCancellationRequested)
                {
                    if (await ReadCommandAsync(reader).ConfigureAwait(false) is not { Count: > 0 } command) return;
                    var reply = Reply(command);
                    await stream.WriteAsync(Encoding.UTF8.GetBytes(reply), _stop.Token).ConfigureAwait(false);
                    await stream.FlushAsync(_stop.Token).ConfigureAwait(false);
                }
            }
            catch (Exception)
            {
                // A closed socket at shutdown. Nothing awaits this task either, so nothing may escape it.
            }
        }
    }

    /// <summary>One inline-free RESP array of bulk strings, which is all StackExchange.Redis-style clients send.</summary>
    private static async Task<List<string>?> ReadCommandAsync(StreamReader reader)
    {
        if (await reader.ReadLineAsync().ConfigureAwait(false) is not { Length: > 0 } header || header[0] != '*') return null;
        var count = int.Parse(header[1..], CultureInfo.InvariantCulture);
        var args = new List<string>(count);
        for (var i = 0; i < count; i++)
        {
            if (await reader.ReadLineAsync().ConfigureAwait(false) is not { Length: > 0 } length || length[0] != '$') return null;
            if (await reader.ReadLineAsync().ConfigureAwait(false) is not { } value) return null;
            args.Add(value);
        }

        return args;
    }

    private string Reply(List<string> command)
    {
        var verb = command[0].ToUpperInvariant();
        var sub = command.Count > 1 ? command[1].ToUpperInvariant() : string.Empty;
        return (verb, sub) switch
        {
            ("HELLO", _) => "%1\r\n$5\r\nproto\r\n:3\r\n",
            ("CLIENT", "ID") => ":17\r\n",
            ("CLIENT", "TRACKING") => "+OK\r\n",
            ("CLIENT", "TRACKINGINFO") => TrackingVerifies
                ? "%1\r\n$5\r\nflags\r\n~2\r\n$2\r\non\r\n$5\r\nbcast\r\n"
                : "%1\r\n$5\r\nflags\r\n~1\r\n$3\r\noff\r\n",
            ("PING", _) => "+PONG\r\n",
            _ => $"-ERR the fake RESP3 server does not model {verb} {sub}\r\n",
        };
    }

    public void Dispose()
    {
        _stop.Cancel();
        _listener.Stop();
        _stop.Dispose();
    }
}

/// <summary>
/// The two spans: <c>redisnearcache.read</c> around the Redis round trip of a MISS, and <c>redisnearcache.arm</c>
/// around arming one endpoint. Everything here is one test class on purpose: an <see cref="ActivityListener"/> is
/// process-wide, so a second class registering one in parallel would make
/// <see cref="NothingIsCreatedWhenNobodyIsListening"/> observe spans it is asserting cannot exist. xUnit runs the
/// tests of one class one at a time, which is what keeps that test honest.
/// </summary>
public class TracingTests
{
    private const string Key = "k";

    private static string Unique(string what) => $"rnc-trace-{what}-" + Guid.NewGuid().ToString("N");

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

    /// <summary>A listener whose <c>KeyInvalidated</c> the test raises, to land an invalidation inside a read.</summary>
    private sealed class ScriptedListener : IInvalidationListener
    {
        public event Action<string>? KeyInvalidated;
        public event Action? FlushAll { add { } remove { } }

        public void RaiseInvalidated(string key) => KeyInvalidated?.Invoke(key);

        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    /// <summary>An armer whose start fails, so the facade settles degraded: pass-through, nothing may be stored.</summary>
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

        /// <summary>Clears the degradation from inside a read, as a recovery that landed mid-flight would.</summary>
        public void RaiseArmed(EndPoint endPoint) => Armed?.Invoke(new TrackingArmedEvent(endPoint, 1, ArmReason.Recovered));

        public Task StartAsync(CancellationToken cancellationToken) =>
            Task.FromException(new RedisConnectionException(ConnectionFailureType.UnableToConnect, "no connection is available"));
        public Task RearmAsync(EndPoint endPoint, ArmReason reason, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task RearmAllAsync(ArmReason reason, CancellationToken cancellationToken) => Task.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    /// <summary>A started cache over the fake, with a real armer, as every other facade test builds one.</summary>
    private static async Task<Facade> StartAsync(FakeMultiplexer mux, RedisNearCacheOptions? options = null, IInvalidationListener? listener = null)
    {
        var connection = FakeRedis.Connection(mux);
        var armer = new TrackingArmer(connection, NullLogger<TrackingArmer>.Instance, Timeout.InfiniteTimeSpan);
        var cache = new Facade(connection, armer, listener ?? new SilentListener(),
            Options.Create(options ?? new RedisNearCacheOptions()), NullLogger<Facade>.Instance);
        await cache.Ready;
        return cache;
    }

    private static FakeMultiplexer Mux(string clientName)
    {
        var mux = new FakeMultiplexer(clientName);
        mux.Add(7000, isReplica: false);
        return mux;
    }

    private static string? NotStoredReason(Activity span) => (string?)span.GetTagItem(RedisNearCacheTracing.NotStoredReasonTag);

    // --- the read span --------------------------------------------------------------------------------------

    /// <summary>
    /// A miss: one span, named, kinded and tagged. The key is the FULL key as sent to Redis - high cardinality on a
    /// span is the point of a span - and <c>rnc.stored</c> says the reply went into L1.
    /// </summary>
    [Fact]
    public async Task AReadThatMissesCreatesExactlyOneReadSpan()
    {
        var clientName = Unique("miss");
        using var probe = new ActivityProbe(clientName);
        var mux = Mux(clientName);
        var options = new RedisNearCacheOptions { KeyNamespace = "app1:" };
        // The span must exist WHILE the read is in flight, not only afterwards: this runs inside the traced region.
        Activity? insideTheRead = null;
        options.TestHooks.AfterRedisReadBeforeStore = _ =>
        {
            insideTheRead = Activity.Current;
            return Task.CompletedTask;
        };
        await using var cache = await StartAsync(mux, options);

        Assert.Equal("v1", await cache.GetAsync<string>(Key));

        var span = probe.Single(RedisNearCacheTracing.ReadSpanName);
        Assert.Equal(RedisNearCacheTracing.ReadSpanName, span.OperationName);
        Assert.Equal(ActivityKind.Client, span.Kind);
        Assert.Equal("app1:" + Key, span.GetTagItem(RedisNearCacheTracing.KeyTag));
        Assert.Equal(clientName, span.GetTagItem(RedisNearCacheTracing.ClientNameTag));
        Assert.Equal(true, (bool?)span.GetTagItem(RedisNearCacheTracing.StoredTag));
        Assert.Null(span.GetTagItem(RedisNearCacheTracing.NotStoredReasonTag));
        // A read that worked is not an error, and there is no rnc.instance on an unnamed instance (the metrics' rule).
        Assert.Equal(ActivityStatusCode.Unset, span.Status);
        Assert.Null(span.GetTagItem(RedisNearCacheTracing.InstanceTag));
        Assert.Same(span, insideTheRead);
        // And the span is over by the time the caller has its value back.
        Assert.Null(Activity.Current);
    }

    /// <summary>A named instance carries the second identity tag, exactly as its measurements do.</summary>
    [Fact]
    public async Task ANamedInstanceCarriesTheInstanceTagOnItsSpans()
    {
        var clientName = Unique("named");
        using var probe = new ActivityProbe(clientName);
        var options = new RedisNearCacheOptions { InstanceName = "second" };
        await using var cache = await StartAsync(Mux(clientName), options);

        await cache.GetAsync<string>(Key);

        var span = probe.Single(RedisNearCacheTracing.ReadSpanName);
        Assert.Equal(clientName, span.GetTagItem(RedisNearCacheTracing.ClientNameTag));
        Assert.Equal("second", span.GetTagItem(RedisNearCacheTracing.InstanceTag));
    }

    /// <summary>
    /// THE test for the hot path. An L1 hit must not start a span - not "start one and dispose it early", not "start
    /// one that sampling drops": <c>StartActivity</c> must not be called at all, because it is several field reads and
    /// a branch even with no listener, and an allocation with one. The listener here samples everything in, and the hit
    /// runs under a span of the test's own, so anything the library asked for would be recorded as a child of it.
    /// </summary>
    [Fact]
    public async Task AnL1HitCreatesNoSpanAtAllEvenWithAListenerSamplingEverything()
    {
        var clientName = Unique("hit");
        using var probe = new ActivityProbe(clientName);
        await using var cache = await StartAsync(Mux(clientName));

        Assert.Equal("v1", await cache.GetAsync<string>(Key)); // the miss that populates L1
        Assert.Single(probe.Finished(RedisNearCacheTracing.ReadSpanName));

        using var scope = probe.StartScope();
        Assert.Equal("v1", await cache.GetAsync<string>(Key)); // the hit
        Assert.Equal("v1", await cache.GetAsync<string>(Key)); // and another, in case the first re-armed something
        Assert.True(cache.TryGetLocal<string>(Key, out _), "precondition: those reads were L1 hits, not misses");

        Assert.True(probe.RequestedUnder(scope.SpanId) == 0,
            $"an L1 hit asked the source for a span: {probe.NamesUnder(scope.SpanId)}");
        // Nothing stopped either, so the count of finished spans is still the one from the miss.
        Assert.Single(probe.Finished(RedisNearCacheTracing.ReadSpanName));
        Assert.Equal(2, cache.Statistics.Hits);
    }

    /// <summary>
    /// With no <see cref="ActivityListener"/> registered nothing is created at all: <c>StartActivity</c> returns null,
    /// so no <see cref="Activity"/> is allocated, no timestamp is taken and <see cref="Activity.Current"/> is never
    /// written - which the hook below observes from INSIDE the traced region, where a span would be current if one
    /// existed. This test registers no probe; see the class remarks for why that is not a race.
    /// </summary>
    [Fact]
    public async Task NothingIsCreatedWhenNobodyIsListening()
    {
        var clientName = Unique("silent");
        var options = new RedisNearCacheOptions();
        var currentInsideTheRead = new List<Activity?>();
        options.TestHooks.AfterRedisReadBeforeStore = _ =>
        {
            currentInsideTheRead.Add(Activity.Current);
            return Task.CompletedTask;
        };
        await using var cache = await StartAsync(Mux(clientName), options);

        Assert.Null(Activity.Current);
        Assert.Equal("v1", await cache.GetAsync<string>(Key));

        Assert.Null(Assert.Single(currentInsideTheRead));
        Assert.Null(Activity.Current);

        // And a listener registered afterwards sees nothing of that read either: there is nothing to replay.
        using var probe = new ActivityProbe(clientName);
        Assert.Empty(probe.Finished());
    }

    // --- what the read span says when nothing was stored ----------------------------------------------------

    /// <summary>
    /// The single most useful thing this span reports: a cache serving correctly and populating nothing. Each reason
    /// below is a different cause with the same symptom, which no counter tells apart per key.
    /// </summary>
    [Fact]
    public async Task AKeyThatDoesNotExistIsReportedAsNothingToStore()
    {
        var clientName = Unique("missing");
        using var probe = new ActivityProbe(clientName);
        var mux = Mux(clientName);
        mux.StoredValue = RedisValue.Null;
        await using var cache = await StartAsync(mux);

        Assert.Null(await cache.GetAsync<string>(Key));

        var span = probe.Single(RedisNearCacheTracing.ReadSpanName);
        Assert.Equal(false, (bool?)span.GetTagItem(RedisNearCacheTracing.StoredTag));
        Assert.Equal("key_missing", NotStoredReason(span));
    }

    [Fact]
    public async Task AnInvalidationThatLandedWhileTheReadWasInFlightIsReportedAsARaceDiscard()
    {
        var clientName = Unique("race");
        using var probe = new ActivityProbe(clientName);
        var listener = new ScriptedListener();
        var options = new RedisNearCacheOptions();
        options.TestHooks.AfterRedisReadBeforeStore = key =>
        {
            listener.RaiseInvalidated(key);
            return Task.CompletedTask;
        };
        await using var cache = await StartAsync(Mux(clientName), options, listener);

        Assert.Equal("v1", await cache.GetAsync<string>(Key)); // served, and rightly not stored

        var span = probe.Single(RedisNearCacheTracing.ReadSpanName);
        Assert.Equal(false, (bool?)span.GetTagItem(RedisNearCacheTracing.StoredTag));
        Assert.Equal("race_discard", NotStoredReason(span));
        Assert.Equal(1, cache.Statistics.RaceDiscards);
    }

    [Fact]
    public async Task ATtlThatCouldNotBeReadIsReportedAsSuch()
    {
        var clientName = Unique("ttl");
        using var probe = new ActivityProbe(clientName);
        var mux = Mux(clientName);
        await using var cache = await StartAsync(mux);

        mux.PttlFailure = new RedisServerException("NOPERM this user has no permissions to run the 'pttl' command");
        Assert.Equal("v1", await cache.GetAsync<string>(Key));

        var span = probe.Single(RedisNearCacheTracing.ReadSpanName);
        Assert.Equal(false, (bool?)span.GetTagItem(RedisNearCacheTracing.StoredTag));
        Assert.Equal("ttl_unknown", NotStoredReason(span));
    }

    [Fact]
    public async Task AKeyOutsideTheKeyPrefixesIsReportedAsSuch()
    {
        var clientName = Unique("prefix");
        using var probe = new ActivityProbe(clientName);
        var options = new RedisNearCacheOptions();
        options.KeyPrefixes.Add("app:");
        await using var cache = await StartAsync(Mux(clientName), options);

        Assert.Equal("v1", await cache.GetAsync<string>("outside-the-prefixes"));

        var span = probe.Single(RedisNearCacheTracing.ReadSpanName);
        Assert.Equal("outside-the-prefixes", span.GetTagItem(RedisNearCacheTracing.KeyTag));
        Assert.Equal(false, (bool?)span.GetTagItem(RedisNearCacheTracing.StoredTag));
        Assert.Equal("outside_key_prefixes", NotStoredReason(span));
    }

    /// <summary>
    /// The one an operator most wants named: the cache is in pass-through, so every read is going to Redis and
    /// nothing is being stored. It is a whole-instance condition, but the span is what shows it on the read in hand.
    /// </summary>
    [Fact]
    public async Task AReadInPassThroughIsReportedAsCachingDisabled()
    {
        var clientName = Unique("passthrough");
        using var probe = new ActivityProbe(clientName);
        var connection = FakeRedis.Connection(Mux(clientName));
        await using var cache = new Facade(connection, new FailingArmer(), new SilentListener(),
            Options.Create(new RedisNearCacheOptions()), NullLogger<Facade>.Instance);
        try { await cache.Ready; } catch { /* the armer's start failed on purpose; the cache stays in pass-through */ }

        Assert.False(cache.IsCoherent, "precondition: the cache is degraded");
        Assert.Equal("v1", await cache.GetAsync<string>(Key));

        var span = probe.Single(RedisNearCacheTracing.ReadSpanName);
        Assert.Equal(false, (bool?)span.GetTagItem(RedisNearCacheTracing.StoredTag));
        Assert.Equal("caching_disabled", NotStoredReason(span));
    }

    /// <summary>
    /// The sixth reason, and the subtlest: the read went out in pass-through, so it carries no TTL to cap the entry
    /// by, and caching came back before the reply landed. Storing it for the full <c>L1MaxAge</c> would ignore
    /// <c>RespectServerTtl</c>, so the facade leaves it to the next read - silently, until now.
    /// </summary>
    [Fact]
    public async Task AReadThatStartedInPassThroughAndEndedCoherentSaysSo()
    {
        var clientName = Unique("resumed");
        using var probe = new ActivityProbe(clientName);
        var armer = new FailingArmer();
        var options = new RedisNearCacheOptions();
        options.TestHooks.AfterRedisReadBeforeStore = _ =>
        {
            armer.RaiseArmed(new IPEndPoint(IPAddress.Loopback, 7000));
            return Task.CompletedTask;
        };
        var connection = FakeRedis.Connection(Mux(clientName));
        await using var cache = new Facade(connection, armer, new SilentListener(),
            Options.Create(options), NullLogger<Facade>.Instance);
        try { await cache.Ready; } catch { /* as above */ }

        Assert.Equal("v1", await cache.GetAsync<string>(Key));

        Assert.True(cache.IsCoherent, "precondition: the arm raised mid-read cleared the degradation");
        var span = probe.Single(RedisNearCacheTracing.ReadSpanName);
        Assert.Equal(false, (bool?)span.GetTagItem(RedisNearCacheTracing.StoredTag));
        Assert.Equal("caching_resumed_mid_read", NotStoredReason(span));
    }

    /// <summary>
    /// A throw inside the traced region is recorded as an error on the span and reaches the caller exactly as it was:
    /// the same exception instance, not wrapped, not swallowed, and not counted a second time.
    /// </summary>
    [Fact]
    public async Task AThrowInsideTheTracedRegionIsRecordedAndStillReachesTheCaller()
    {
        var clientName = Unique("throw");
        using var probe = new ActivityProbe(clientName);
        var thrown = new InvalidOperationException("the read blew up");
        var options = new RedisNearCacheOptions();
        options.TestHooks.AfterRedisReadBeforeStore = _ => throw thrown;
        await using var cache = await StartAsync(Mux(clientName), options);

        var caught = await Assert.ThrowsAsync<InvalidOperationException>(async () => await cache.GetAsync<string>(Key));

        Assert.Same(thrown, caught);
        var span = probe.Single(RedisNearCacheTracing.ReadSpanName);
        Assert.Equal(ActivityStatusCode.Error, span.Status);
        Assert.Equal(thrown.Message, span.StatusDescription);
        // The failure is not stored and is not a serializer failure either: nothing is double-counted by the span.
        Assert.Null(span.GetTagItem(RedisNearCacheTracing.StoredTag));
        Assert.Equal(0, cache.Statistics.SerializerFailures);
    }

    // --- the arm span ---------------------------------------------------------------------------------------

    /// <summary>The initial arm is traced too: the first arm is the one a cache stuck in pass-through never finishes.</summary>
    [Fact]
    public async Task TheInitialArmCreatesOneSpan()
    {
        var clientName = Unique("arm-initial");
        using var probe = new ActivityProbe(clientName);
        var mux = new FakeMultiplexer(clientName);
        var master = mux.Add(7000, isReplica: false);
        await using var cache = await StartAsync(mux);

        var span = probe.Single(RedisNearCacheTracing.ArmSpanName);
        Assert.Equal(ActivityKind.Client, span.Kind);
        Assert.Equal(master.EndPoint.ToString(), span.GetTagItem(RedisNearCacheTracing.EndpointTag));
        Assert.Equal(nameof(ArmReason.Initial), span.GetTagItem(RedisNearCacheTracing.ArmReasonTag));
        Assert.Equal(master.SubscriberId, (long?)span.GetTagItem(RedisNearCacheTracing.RedirectClientIdTag));
        Assert.Equal(clientName, span.GetTagItem(RedisNearCacheTracing.ClientNameTag));
        Assert.Equal(ActivityStatusCode.Unset, span.Status);
        Assert.True(cache.IsCoherent);
    }

    /// <summary>
    /// A real re-arm, driven the way production drives one: the subscriber connection came back with a new client id
    /// and nothing said so, and the reconcile sweep finds it (as <c>ArmVerificationTests</c> covers). The assertion
    /// waits on the <c>Armed</c> EVENT, never on the redirect map, which the armer writes first.
    /// </summary>
    [Fact]
    public async Task ARearmFoundBySweepCreatesASpanCarryingTheEndpointAndTheReason()
    {
        var clientName = Unique("arm-rearm");
        using var probe = new ActivityProbe(clientName);
        var mux = new FakeMultiplexer(clientName);
        var master = mux.Add(7000, isReplica: false);
        var connection = FakeRedis.Connection(mux);
        var armer = new TrackingArmer(connection, NullLogger<TrackingArmer>.Instance, TimeSpan.FromMilliseconds(30));
        var armed = new ConcurrentQueue<ArmReason>();
        armer.Armed += e => armed.Enqueue(e.Reason);
        await using var cache = new Facade(connection, armer, new SilentListener(),
            Options.Create(new RedisNearCacheOptions()), NullLogger<Facade>.Instance);
        await cache.Ready;
        Assert.Contains(ArmReason.Initial, armed);

        var newId = master.SubscriberId + 41;
        master.SubscriberId = newId;

        Assert.True(await UntilAsync(() => armed.Contains(ArmReason.VerificationFailed)),
            $"the sweep never re-armed the endpoint. arms: {string.Join(", ", armed)}");

        // The first re-arm span, not the only one: the sweep goes on running, so its count is nobody's assertion.
        var rearms = probe.Finished(RedisNearCacheTracing.ArmSpanName)
            .Where(a => (string?)a.GetTagItem(RedisNearCacheTracing.ArmReasonTag) == nameof(ArmReason.VerificationFailed)).ToArray();
        Assert.NotEmpty(rearms);
        var span = rearms[0];
        Assert.Equal(master.EndPoint.ToString(), span.GetTagItem(RedisNearCacheTracing.EndpointTag));
        Assert.Equal(newId, (long?)span.GetTagItem(RedisNearCacheTracing.RedirectClientIdTag));
        Assert.Equal(clientName, span.GetTagItem(RedisNearCacheTracing.ClientNameTag));
    }

    /// <summary>An arm the server would not confirm is an error on the span, not a silent success.</summary>
    [Fact]
    public async Task AnArmTheServerDoesNotConfirmIsAnErrorOnTheSpan()
    {
        var clientName = Unique("arm-unverified");
        using var probe = new ActivityProbe(clientName);
        var mux = new FakeMultiplexer(clientName);
        var master = mux.Add(7000, isReplica: false);
        // CLIENT TRACKINGINFO reports a redirect somewhere else entirely, so the arm is never confirmed.
        master.TrackingInfoRedirect = master.SubscriberId + 1000;
        var connection = FakeRedis.Connection(mux);
        var armer = new TrackingArmer(connection, NullLogger<TrackingArmer>.Instance, Timeout.InfiniteTimeSpan);
        // Ready is deliberately not awaited: nothing will ever arm here, so the start works through its whole retry
        // ladder first. The first attempt's span is there within microseconds, and the dispose below observes Ready.
        await using var cache = new Facade(connection, armer, new SilentListener(),
            Options.Create(new RedisNearCacheOptions()), NullLogger<Facade>.Instance);

        Assert.True(await UntilAsync(() => probe.Finished(RedisNearCacheTracing.ArmSpanName).Count > 0),
            "the arm attempt was not traced at all");
        var span = probe.Finished(RedisNearCacheTracing.ArmSpanName)[0];
        Assert.Equal(ActivityStatusCode.Error, span.Status);
        Assert.Equal(RedisNearCacheTracing.ArmNotVerified, span.StatusDescription);
    }

    // --- lifetime ------------------------------------------------------------------------------------------

    /// <summary>
    /// The source is disposed with the cache, exactly as the meter is, and a read afterwards does not resurrect it:
    /// the read is refused before it reaches the source, and the source would answer null anyway.
    /// </summary>
    [Fact]
    public async Task TheActivitySourceIsDisposedWithTheCache()
    {
        var clientName = Unique("dispose");
        using var probe = new ActivityProbe(clientName);
        var cache = await StartAsync(Mux(clientName));
        await cache.GetAsync<string>(Key);
        // The facade's own tracing, reached the way FakeRedis reaches the connection's private constructor. Asserting
        // on the object itself is what makes this a test of the facade's dispose rather than of ThrowIfDisposed: a
        // read after dispose is refused before it reaches the source, so it would prove nothing on its own.
        var tracing = (RedisNearCacheTracing)typeof(Facade)
            .GetField("_tracing", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(cache)!;
        using (var live = tracing.StartRead("before-dispose")) Assert.NotNull(live);
        var spansBefore = probe.Finished().Count;

        await cache.DisposeAsync();

        Assert.Null(tracing.StartRead("after-dispose"));
        using var scope = probe.StartScope();
        await Assert.ThrowsAsync<ObjectDisposedException>(async () => await cache.GetAsync<string>(Key));
        Assert.Equal(0, probe.RequestedUnder(scope.SpanId));
        Assert.Equal(spansBefore, probe.Finished().Count);
    }

    /// <summary>
    /// And the type itself honours Dispose: a disposed <see cref="ActivitySource"/> starts nothing, whatever is
    /// listening. This is the half of the test above that does not depend on the read being refused first.
    /// </summary>
    [Fact]
    public void ADisposedTracingStartsNothing()
    {
        var clientName = Unique("dispose-type");
        using var probe = new ActivityProbe(clientName);
        var tracing = new RedisNearCacheTracing(clientName);

        using (var live = tracing.StartRead(Key))
        {
            Assert.NotNull(live);
        }

        tracing.Dispose();

        Assert.Null(tracing.StartRead(Key));
        Assert.Null(tracing.StartArm(new IPEndPoint(IPAddress.Loopback, 7000), ArmReason.Manual, 1));
        Assert.Single(probe.Finished(RedisNearCacheTracing.ReadSpanName));
    }

    /// <summary>
    /// The names and attribute keys, as literals in one place, for the reason <c>MetricsTests</c> keeps the instrument
    /// names as literals: they are the identity every dashboard, alert and trace query is written against, and every
    /// other assertion here goes through the constants, so nothing else would notice a rename.
    /// </summary>
    [Fact]
    public void TheSpanNamesAndAttributeKeysAreTheOnesQueriesAreWrittenAgainst()
    {
        Assert.Equal("redisnearcache.read", RedisNearCacheTracing.ReadSpanName);
        Assert.Equal("redisnearcache.arm", RedisNearCacheTracing.ArmSpanName);
        Assert.Equal("rnc.client_name", RedisNearCacheTracing.ClientNameTag);
        Assert.Equal("rnc.instance", RedisNearCacheTracing.InstanceTag);
        Assert.Equal("rnc.key", RedisNearCacheTracing.KeyTag);
        Assert.Equal("rnc.stored", RedisNearCacheTracing.StoredTag);
        Assert.Equal("rnc.not_stored_reason", RedisNearCacheTracing.NotStoredReasonTag);
        Assert.Equal("rnc.endpoint", RedisNearCacheTracing.EndpointTag);
        Assert.Equal("rnc.arm_reason", RedisNearCacheTracing.ArmReasonTag);
        Assert.Equal("rnc.redirect_client_id", RedisNearCacheTracing.RedirectClientIdTag);
        // The two identity tags are the metrics' own constants, not a second spelling of them.
        Assert.Equal(RedisNearCacheMetrics.ClientNameTag, RedisNearCacheTracing.ClientNameTag);
        Assert.Equal(RedisNearCacheMetrics.InstanceTag, RedisNearCacheTracing.InstanceTag);
        // Every reason the read span can give, spelled the way it is emitted.
        Assert.Equal(
            ["key_missing", "race_discard", "ttl_unknown", "caching_disabled", "outside_key_prefixes", "caching_resumed_mid_read"],
            Enum.GetValues<ReadNotStored>().Select(RedisNearCacheTracing.Name).ToArray());
        // And the arm reason is the ArmReason member name, the same value the rearms metric's reason tag carries.
        Assert.Equal(nameof(ArmReason.VerificationFailed), RedisNearCacheTracing.Name(ArmReason.VerificationFailed));
    }

    /// <summary>
    /// The source name is the meter name, and publicly discoverable: a caller needs it for OpenTelemetry's
    /// <c>AddSource</c>, and one scope covering both signals is why it is the same string.
    /// </summary>
    [Fact]
    public void TheActivitySourceNameIsThePublicConstantAndMatchesTheMeterName()
    {
        Assert.Equal(RedisNearCacheStatistics.MeterName, RedisNearCacheStatistics.ActivitySourceName);
        Assert.Equal(RedisNearCacheStatistics.ActivitySourceName, RedisNearCacheTracing.ActivitySourceName);
        Assert.Equal("RedisNearCache", RedisNearCacheStatistics.ActivitySourceName);
    }

    // --- the arm span in Broadcast mode ---------------------------------------------------------------------

    /// <summary>
    /// BCAST arms per endpoint just as REDIRECT does - a handshake, <c>CLIENT TRACKING ON BCAST</c>, the same
    /// <c>CLIENT TRACKINGINFO</c> verification, and a retry ladder REDIRECT has no equivalent of - so it gets the same
    /// span, under the same name and attribute keys: a trace query must not have to know which mode a deployment uses.
    /// <c>rnc.redirect_client_id</c> is the one difference, absent because a BCAST socket tracks for itself.
    /// </summary>
    [Fact]
    public async Task ABroadcastArmCreatesTheSameArmSpan()
    {
        var clientName = Unique("arm-bcast");
        using var probe = new ActivityProbe(clientName);
        using var server = new FakeResp3Server();
        var mux = new FakeMultiplexer(clientName);
        var master = mux.Add(server.Port, isReplica: false);
        var connection = FakeRedis.Connection(mux);
        var options = new RedisNearCacheOptions
        {
            ConnectionString = $"127.0.0.1:{server.Port}",
            TrackingMode = TrackingMode.Broadcast,
        };
        // A prefix, so the arm is not the "whole keyspace" one that logs a warning of its own.
        options.KeyPrefixes.Add("k");
        // One object as both armer and listener, exactly as the DI registration wires Broadcast mode.
        var tracker = new RedisNearCache.Tracking.Broadcast.BroadcastTracker(connection, options, NullLogger<RedisNearCache.Tracking.Broadcast.BroadcastTracker>.Instance);
        await using var cache = new Facade(connection, tracker, tracker, Options.Create(options), NullLogger<Facade>.Instance);
        await cache.Ready;

        var span = probe.Single(RedisNearCacheTracing.ArmSpanName);
        Assert.Equal(ActivityKind.Client, span.Kind);
        Assert.Equal(master.EndPoint.ToString(), span.GetTagItem(RedisNearCacheTracing.EndpointTag));
        Assert.Equal(nameof(ArmReason.Initial), span.GetTagItem(RedisNearCacheTracing.ArmReasonTag));
        Assert.Equal(clientName, span.GetTagItem(RedisNearCacheTracing.ClientNameTag));
        Assert.Equal(ActivityStatusCode.Unset, span.Status);
        // No redirect target exists in BCAST, so the attribute is absent rather than a placeholder.
        Assert.Null(span.GetTagItem(RedisNearCacheTracing.RedirectClientIdTag));
        Assert.True(cache.IsCoherent);
    }

    /// <summary>
    /// And a BCAST arm the server will not confirm is the same error on the span as an unconfirmed REDIRECT arm, with
    /// the same status description, so one alert covers both modes.
    /// </summary>
    [Fact]
    public async Task ABroadcastArmTheServerDoesNotConfirmIsAnErrorOnTheSpan()
    {
        var clientName = Unique("arm-bcast-unverified");
        using var probe = new ActivityProbe(clientName);
        using var server = new FakeResp3Server { TrackingVerifies = false };
        var mux = new FakeMultiplexer(clientName);
        mux.Add(server.Port, isReplica: false);
        var connection = FakeRedis.Connection(mux);
        var options = new RedisNearCacheOptions
        {
            ConnectionString = $"127.0.0.1:{server.Port}",
            TrackingMode = TrackingMode.Broadcast,
        };
        // A prefix, so the arm is not the "whole keyspace" one that logs a warning of its own.
        options.KeyPrefixes.Add("k");
        var tracker = new RedisNearCache.Tracking.Broadcast.BroadcastTracker(connection, options, NullLogger<RedisNearCache.Tracking.Broadcast.BroadcastTracker>.Instance);
        // Ready is deliberately not awaited: nothing will ever arm here, so the start works through its retry ladder
        // first. The dispose below observes Ready.
        await using var cache = new Facade(connection, tracker, tracker, Options.Create(options), NullLogger<Facade>.Instance);

        Assert.True(await UntilAsync(() => probe.Finished(RedisNearCacheTracing.ArmSpanName).Count > 0), "the BCAST arm attempt was not traced at all");
        var span = probe.Finished(RedisNearCacheTracing.ArmSpanName)[0];
        Assert.Equal(ActivityStatusCode.Error, span.Status);
        Assert.Equal(RedisNearCacheTracing.ArmNotVerified, span.StatusDescription);
    }
}
