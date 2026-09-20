using System.Globalization;
using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.DependencyInjection;
using RedisNearCache.Internal;
using RedisNearCache.Tests.Chaos;
using RedisNearCache.Tests.EdgeCases;
using Xunit;
using Xunit.Abstractions;

namespace RedisNearCache.Tests.Resilience;

/// <summary>
/// The start sequence, against a real server. Two holes lived here:
/// <list type="bullet">
/// <item>A cache built while Redis was unreachable (<c>abortConnect=false</c>, the documented way to let an
/// application start before its Redis) never cached again for the life of the process. The subscription threw, so
/// the armer was never started: it hooked no connection event and ran no sweep, and a start runs once.</item>
/// <item>On a deployment with several masters, the masters are armed concurrently and each raises its own
/// <c>Armed(Initial)</c>. The first of them used to settle startup, so reads stopped waiting for <c>Ready</c> and
/// were stored - including reads routed to a master whose <c>CLIENT TRACKING ON</c> had not been sent yet. Nothing
/// tracks such an entry and no flush follows, so it was served until its TTL whatever was written to the key.</item>
/// </list>
/// Neither is provable against fakes alone: the first is about StackExchange.Redis's own reconnect behaviour, the
/// second about a real cluster's per-node arming.
/// </summary>
public class StartupCoherenceTests
{
    private readonly ITestOutputHelper _out;

    public StartupCoherenceTests(ITestOutputHelper output) => _out = output;

    /// <summary>
    /// Forwards a local port to another, and can be started late: the cache is pointed at the near side while
    /// nothing listens there (connection refused, exactly as a Redis that is not up yet), and the forwarder is
    /// started afterwards. Used instead of stopping the shared container, which other suites are reading.
    /// </summary>
    private sealed class LateForwarder : IDisposable
    {
        private readonly TcpListener _listener;
        private readonly string _targetHost;
        private readonly int _targetPort;
        private readonly CancellationTokenSource _stop = new();

        private LateForwarder(TcpListener listener, string targetHost, int targetPort)
        {
            _listener = listener;
            _targetHost = targetHost;
            _targetPort = targetPort;
        }

        public int Port { get; private set; }

        /// <summary>Reserves a free port without listening on it, so a connect to it is refused.</summary>
        public static (LateForwarder Forwarder, int Port) Reserve(string targetHost, int targetPort)
        {
            var probe = new TcpListener(IPAddress.Loopback, 0);
            probe.Start();
            var port = ((IPEndPoint)probe.LocalEndpoint).Port;
            probe.Stop();
            var forwarder = new LateForwarder(new TcpListener(IPAddress.Loopback, port), targetHost, targetPort) { Port = port };
            return (forwarder, port);
        }

        public void Start()
        {
            _listener.Start();
            _ = Task.Run(async () =>
            {
                try
                {
                    while (!_stop.IsCancellationRequested)
                    {
                        var client = await _listener.AcceptTcpClientAsync(_stop.Token).ConfigureAwait(false);
                        _ = Task.Run(() => PumpAsync(client), CancellationToken.None);
                    }
                }
                catch (Exception)
                {
                    // Stopped, or the listener went away. Nothing here may escape: this task is never awaited, and a
                    // fault on it would be an unobserved task exception charged to an unrelated test.
                }
            }, CancellationToken.None);
        }

        private async Task PumpAsync(TcpClient client)
        {
            using var _ = client;
            using var upstream = new TcpClient();
            try
            {
                await upstream.ConnectAsync(_targetHost, _targetPort, _stop.Token).ConfigureAwait(false);
                var a = client.GetStream();
                var b = upstream.GetStream();
                // First direction to end finishes the connection, but NEITHER task may fault: WhenAny leaves the
                // loser running, and when Dispose cancels it the socket read throws with nobody watching. An
                // unobserved task exception is rethrown by the finalizer inside whichever test the GC interrupts,
                // which is how this cost an unrelated test (EdgeCases.DisposeRaceTests, which hooks
                // TaskScheduler.UnobservedTaskException) a red build on one image only. CopyAsync swallows its own
                // ending, so the loser completes quietly whenever it does.
                await Task.WhenAny(CopyAsync(a, b), CopyAsync(b, a)).ConfigureAwait(false);
            }
            catch (Exception) { /* either side went away */ }
        }

        /// <summary>One direction, to completion. Its own end of the connection going away is the normal way to stop.</summary>
        private async Task CopyAsync(Stream from, Stream to)
        {
            try
            {
                await from.CopyToAsync(to, _stop.Token).ConfigureAwait(false);
            }
            catch (Exception)
            {
                // Cancelled, reset or closed: expected, and never a fault anyone needs to see.
            }
        }

        public void Dispose()
        {
            _stop.Cancel();
            try { _listener.Stop(); } catch (SocketException) { /* already stopped */ }
            _stop.Dispose();
        }
    }

    /// <summary>
    /// Redis is unreachable when the cache is built and comes up afterwards. The cache must serve reads
    /// throughout (pass-through, nothing cached, <c>IsCoherent</c> false), then arm itself once the server is
    /// there - and really arm it: a foreign write has to evict what it cached.
    /// </summary>
    [Fact]
    public async Task ACacheBuiltWhileRedisIsUnreachableArmsItselfOnceRedisIsThere()
    {
        var (forwarder, port) = LateForwarder.Reserve("127.0.0.1", 6379);
        using var forwarderLifetime = forwarder;
        var key = TestHelpers.Key("startup-unreachable");
        var connectionString = string.Create(CultureInfo.InvariantCulture,
            $"127.0.0.1:{port},abortConnect=false,connectRetry=1,connectTimeout=1000");

        var handle = await EdgeCaseSupport.BuildAsync(
            connectionString,
            o => o.TestHooks.StartRetryInterval = TimeSpan.FromMilliseconds(250),
            awaitReady: false);
        try
        {
            var cache = handle.Cache;
            await Assert.ThrowsAnyAsync<Exception>(() => cache.Ready);
            Assert.False(cache.IsCoherent, "a cache whose start failed must not report itself coherent");
            _out.WriteLine($"start failed as expected; stats={cache.Statistics}");

            // Redis appears.
            RedisCli.Standalone("SET", key, "v1");
            forwarder.Start();

            var coherent = await Poll.UntilAsync(() => cache.IsCoherent, TimeSpan.FromSeconds(60), TimeSpan.FromMilliseconds(100));
            Assert.True(coherent,
                $"the cache never armed itself after Redis appeared: stats={cache.Statistics}, armed=[{string.Join(",", handle.Armer.RedirectTargets.Keys)}]");
            _out.WriteLine($"coherent again; armed=[{string.Join(",", handle.Armer.RedirectTargets.Keys)}]");

            // Ready keeps the outcome the application saw; the cache does not.
            Assert.True(cache.Ready.IsFaulted);

            Assert.True(await TestHelpers.ReadUntilCachedAsync(cache, key, "v1", TimeSpan.FromSeconds(30)),
                "the recovered cache never cached the key.");

            // Tracking was genuinely armed at the server, not merely assumed: a foreign write must evict.
            RedisCli.Standalone("SET", key, "v2");
            var evicted = await Poll.UntilAsync(() => !cache.TryGetLocal<string>(key, out string? _), TimeSpan.FromSeconds(10));
            Assert.True(evicted, "invalidations are not arriving: the recovered cache is not really tracked.");
            Assert.Equal("v2", await cache.GetAsync<string>(key));
        }
        finally
        {
            await handle.DisposeAsync();
            try { RedisCli.Standalone("DEL", key); } catch (InvalidOperationException) { /* best effort */ }
        }
    }

    /// <summary>
    /// A cluster master that is connected but cannot answer while the others are being armed (here
    /// <c>CLIENT PAUSE ... ALL</c>, as a failover window or a blocking save would). The private multiplexer is
    /// connected to every master BEFORE the pause, so all three are in the initial arm and the paused one's
    /// <c>CLIENT TRACKING ON</c> is the only one still outstanding: exactly the window in which a read routed to it
    /// used to be stored with nothing tracking it, and no flush to follow.
    /// </summary>
    [Fact]
    public async Task AReadDuringTheStartIsNeverStoredUntrackedWhileOneMasterIsStillBeingArmed()
    {
        var services = new ServiceCollection();
        services.AddRedisNearCache(ClusterCacheFixture.ConnectionString);
        var provider = services.BuildServiceProvider();
        string? key = null;
        var slowPort = 0;
        try
        {
            // Resolving the connection connects the private multiplexer to every master, without arming anything:
            // the facade (which starts the armer in its constructor) is only resolved after the pause is in place.
            var connection = provider.GetRequiredService<RedisNearCacheConnection>();
            var masters = connection.ConnectedMasters().ToArray();
            Assert.True(masters.Length >= 2, $"this test needs a multi-master cluster; found {masters.Length}");
            var slowMaster = masters[^1];
            slowPort = ResilienceSupport.PortOf(slowMaster.EndPoint!);
            key = TestHelpers.KeyForEndPoint(connection.Multiplexer, masters[0], slowMaster.EndPoint!);
            RedisCli.Cluster(slowPort, "SET", key, "v1");
            _out.WriteLine($"key {key} belongs to {slowMaster.EndPoint}, which is connected and about to be paused");

            // Long enough that the other masters finish arming while this one cannot answer CLIENT LIST at all, and
            // short enough that it expires on its own (CLIENT UNPAUSE is itself postponed; see ClientPauseTests).
            RedisCli.Cluster(slowPort, "CLIENT", "PAUSE", "3000", "ALL");

            var cache = provider.GetRequiredService<IRedisNearCache>();
            var armer = provider.GetRequiredService<ITrackingArmer>();

            // The window: some master armed, the paused one not, the start not finished.
            var windowObserved = await Poll.UntilAsync(
                () => armer.RedirectTargets.Count > 0 && !armer.RedirectTargets.ContainsKey(slowMaster.EndPoint!) && !cache.Ready.IsCompleted,
                TimeSpan.FromSeconds(3), TimeSpan.FromMilliseconds(5));
            _out.WriteLine($"window observed={windowObserved}: armed=[{string.Join(",", armer.RedirectTargets.Keys)}], IsCoherent={cache.IsCoherent}");
            if (windowObserved)
            {
                Assert.False(cache.IsCoherent,
                    $"the cache reported itself coherent while {slowMaster.EndPoint} was still being armed " +
                    $"(armed=[{string.Join(",", armer.RedirectTargets.Keys)}])");
            }

            // A read of the paused master's key, issued inside that window. It cannot complete until the pause ends.
            var read = ChaosSupport.WithReconnectRetryAsync(async () => await cache.GetAsync<string>(key), TimeSpan.FromSeconds(30));
            await cache.Ready;
            var value = await read;
            _out.WriteLine($"read during the start returned '{value}'; cached={cache.TryGetLocal<string>(key, out string? _)}; stats={cache.Statistics}");
            Assert.Equal("v1", value);

            // The decisive assertion: whatever L1 holds now is tracked by the server, so a write from outside evicts
            // it. Before the fix the entry stored inside the window survived this write and was served indefinitely.
            RedisCli.Cluster(slowPort, "SET", key, "v2");
            var readKey = key;
            var fresh = await Poll.UntilAsync(
                async () =>
                {
                    if (cache.TryGetLocal<string>(readKey, out string? local) && local == "v1") return false;
                    return await cache.GetAsync<string>(readKey) == "v2";
                },
                TimeSpan.FromSeconds(15), TimeSpan.FromMilliseconds(100));
            Assert.True(fresh,
                "a value stored while a master was still being armed survived a foreign write: L1 holds " +
                $"'{(cache.TryGetLocal<string>(key, out string? stale) ? stale : "(nothing)")}' while Redis holds 'v2'; stats={cache.Statistics}");

            Assert.True(await ChaosSupport.QuiesceAsync(cache, timeout: TimeSpan.FromSeconds(30)), "the cache never settled after the start.");
        }
        finally
        {
            await provider.DisposeAsync();
            if (key is not null)
            {
                try { RedisCli.Cluster(slowPort, "DEL", key); } catch (InvalidOperationException) { /* best effort */ }
            }
        }
    }
}
