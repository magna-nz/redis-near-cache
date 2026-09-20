using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Text;
using RedisNearCache.Tests.Chaos;
using RedisNearCache.Tests.EdgeCases;
using Xunit;
using Xunit.Abstractions;

namespace RedisNearCache.Tests.Resilience;

/// <summary>
/// The failure mode nothing reported: the subscriber connection - the redirect target for every invalidation, and the
/// one connection that never sends anything - goes silent without being closed, as a NAT or load balancer dropping an
/// idle flow does. The server keeps redirecting invalidations to a client it believes is there, the reading
/// (interactive) connection stays perfectly healthy, and the facade serves L1 and reports itself coherent.
/// <para>
/// Measured before the fix: about 67 s of silent stale reads while StackExchange.Redis's 60 s keepalive got round to
/// noticing, and <b>unbounded</b> when its reconnect could not complete either - 466 of 466 reads stale over 260 s,
/// with 29 reconnect attempts and not one <c>ConnectionFailed</c>/<c>Restored</c> event raised, so nothing in the
/// library ever learned of it. Two changes close it: the private multiplexer's keepalive is 10 s rather than 60 s,
/// and the armer's 5 s sweep now re-checks that the server is still redirecting to the client id of our subscriber
/// connection as <c>CLIENT LIST</c> reports it now.
/// </para>
/// </summary>
public class SubscriberSilentlyDeadTests
{
    private readonly ITestOutputHelper _out;

    public SubscriberSilentlyDeadTests(ITestOutputHelper output) => _out = output;

    /// <summary>
    /// Forwards a local port to Redis, and can silently swallow everything on one connection - in both directions,
    /// leaving the socket open, so neither end is told.
    /// <para>
    /// The subscription bridge is recognised by what the SERVER sends back on it: only that connection ever carries
    /// <c>__redis__:invalidate</c> (its subscribe confirmation, then the invalidation pushes). Sniffing the client's
    /// own <c>SUBSCRIBE</c> is not enough, because StackExchange.Redis also subscribes on the interactive connection
    /// when the subscriber bridge cannot confirm, and swallowing reads is a different failure from this one; the
    /// server-visible source port is no good either, since Docker's published port rewrites it.
    /// </para>
    /// </summary>
    private sealed class SubscriberBlackholeProxy : IDisposable
    {
        private readonly TcpListener _listener;
        private readonly CancellationTokenSource _stop = new();
        private int _subscriberConnections;
        private int _connections;
        private int _subscriptionBridge;

        public SubscriberBlackholeProxy()
        {
            _listener = new TcpListener(IPAddress.Loopback, 0);
            _listener.Start();
            Port = ((IPEndPoint)_listener.LocalEndpoint).Port;
            _ = Task.Run(AcceptAsync, CancellationToken.None);
        }

        public int Port { get; }

        /// <summary>The one connection to swallow, by this proxy's own ordinal.</summary>
        public int? BlackholedConnection { get; set; }

        /// <summary>Ordinal of the first connection the server used to deliver <c>__redis__:invalidate</c>.</summary>
        public int? SubscriptionBridge => Volatile.Read(ref _subscriptionBridge) is var id && id > 0 ? id : null;

        /// <summary>How many distinct connections the server has used as a subscription bridge (reconnects included).</summary>
        public int SubscriberConnections => Volatile.Read(ref _subscriberConnections);

        private async Task AcceptAsync()
        {
            try
            {
                while (!_stop.IsCancellationRequested)
                {
                    var client = await _listener.AcceptTcpClientAsync(_stop.Token).ConfigureAwait(false);
                    var id = Interlocked.Increment(ref _connections);
                    _ = Task.Run(() => PumpAsync(client, id), CancellationToken.None);
                }
            }
            catch (Exception) { /* stopped */ }
        }

        private async Task PumpAsync(TcpClient client, int id)
        {
            var isSubscriptionBridge = false;
            using var upstream = new TcpClient();
            try
            {
                await upstream.ConnectAsync("127.0.0.1", 6379, _stop.Token).ConfigureAwait(false);
                var clientStream = client.GetStream();
                var serverStream = upstream.GetStream();

                // Each pump swallows its own ending. WhenAny below leaves the loser running - which is deliberate,
                // a blackholed flow must stay open - so it must never be able to fault: an unobserved task exception
                // is rethrown by the finalizer inside whichever test the GC happens to interrupt.
                async Task Pump(NetworkStream from, NetworkStream to, bool fromClient)
                {
                    try
                    {
                        await PumpCore(from, to, fromClient).ConfigureAwait(false);
                    }
                    catch (Exception)
                    {
                        // Cancelled, reset or closed: expected here.
                    }
                }

                async Task PumpCore(NetworkStream from, NetworkStream to, bool fromClient)
                {
                    var buffer = new byte[64 * 1024];
                    while (true)
                    {
                        var read = await from.ReadAsync(buffer, _stop.Token).ConfigureAwait(false);
                        if (read <= 0) return;
                        if (!fromClient && !isSubscriptionBridge
                            && Encoding.ASCII.GetString(buffer, 0, read).Contains("__redis__:invalidate", StringComparison.Ordinal))
                        {
                            isSubscriptionBridge = true;
                            Interlocked.Increment(ref _subscriberConnections);
                            Interlocked.CompareExchange(ref _subscriptionBridge, id, 0);
                        }

                        if (BlackholedConnection == id) continue; // swallowed; neither side is told
                        await to.WriteAsync(buffer.AsMemory(0, read), _stop.Token).ConfigureAwait(false);
                    }
                }

                await Task.WhenAny(Pump(clientStream, serverStream, true), Pump(serverStream, clientStream, false)).ConfigureAwait(false);
            }
            catch (Exception) { /* either side went away */ }
            finally
            {
                // A blackholed connection is left half-open on purpose: closing it would be an event.
                if (BlackholedConnection != id) { try { client.Close(); } catch (Exception) { /* already gone */ } }
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
    /// The same cut-off, but the subscriber is allowed to reconnect - a NAT dropping one idle flow rather than a
    /// permanently broken path. StackExchange.Redis notices within its (now 10 s) keepalive and reconnects, which the
    /// armer turns into a re-arm and a flush. This is the common case, and it must be quick.
    /// </summary>
    [Fact]
    public async Task ASubscriberThatDiesSilentlyIsNoticedAndStopsServingStaleValues()
    {
        using var proxy = new SubscriberBlackholeProxy();
        var key = TestHelpers.Key("silent-subscriber");
        var connectionString = string.Create(CultureInfo.InvariantCulture, $"127.0.0.1:{proxy.Port}");
        var handle = await EdgeCaseSupport.BuildAsync(connectionString);
        try
        {
            var cache = handle.Cache;
            RedisCli.Standalone("SET", key, "v1");
            Assert.True(await TestHelpers.ReadUntilCachedAsync(cache, key, "v1"), "the key was not cached before the subscriber was cut off.");

            // The subscription bridge has identified itself by now: the server sent it the subscribe confirmation
            // for __redis__:invalidate, and the read above proved tracking is live on it.
            var bridge = proxy.SubscriptionBridge;
            Assert.NotNull(bridge);
            _out.WriteLine($"armed: {string.Join(",", handle.Armer.RedirectTargets.Select(kv => $"{kv.Key}=>{kv.Value}"))}; subscription bridge is proxy connection #{bridge}");

            // That one flow goes silent, and a foreign write produces an invalidation that can never arrive.
            proxy.BlackholedConnection = bridge;
            RedisCli.Standalone("SET", key, "v2");
            _out.WriteLine("subscriber flow blackholed (socket left open); Redis holds v2 while L1 holds v1");

            // Nothing may keep serving the pre-write value. Recovery is either a re-arm (flush, then v2) or the
            // endpoint being given up on (pass-through, also v2). Before the fix the cache served v1 for about 67 s,
            // waiting on StackExchange.Redis's 60 s keepalive; the budget here is deliberately below that.
            var stale = 0;
            var fresh = await Poll.UntilAsync(
                async () =>
                {
                    var value = await ChaosSupport.WithReconnectRetryAsync(async () => await cache.GetAsync<string>(key), TimeSpan.FromSeconds(20));
                    if (value == "v2") return true;
                    stale++;
                    return false;
                },
                TimeSpan.FromSeconds(45), TimeSpan.FromMilliseconds(500));

            _out.WriteLine($"stale reads before recovery: {stale}; subscriber connections: {proxy.SubscriberConnections}; stats={cache.Statistics}");
            Assert.True(fresh,
                $"the cache served the pre-write value for 45 s: {stale} stale reads, " +
                $"{proxy.SubscriberConnections} subscriber connections, IsCoherent={cache.IsCoherent}, stats={cache.Statistics}");
            Assert.False(cache.TryGetLocal<string>(key, out string? local) && local == "v1", "L1 still holds the pre-write value after recovery.");
            Assert.True(cache.Statistics.Flushes > 0, $"nothing flushed L1, so nothing noticed the dead subscriber: stats={cache.Statistics}");

            // Tracking is genuinely live again (the subscriber reconnected through the proxy): a foreign write evicts.
            Assert.True(await TestHelpers.ReadUntilCachedAsync(cache, key, "v2", TimeSpan.FromSeconds(30)), "the key was not cached again after recovery.");
            RedisCli.Standalone("SET", key, "v3");
            var evicted = await Poll.UntilAsync(() => !cache.TryGetLocal<string>(key, out string? _), TimeSpan.FromSeconds(15));
            Assert.True(evicted, "invalidations are still not arriving after the subscriber reconnected.");
            Assert.Equal("v3", await ChaosSupport.WithReconnectRetryAsync(async () => await cache.GetAsync<string>(key)));
        }
        finally
        {
            await handle.DisposeAsync();
            try { RedisCli.Standalone("DEL", key); } catch (InvalidOperationException) { /* best effort */ }
        }
    }

}
