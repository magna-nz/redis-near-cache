using Xunit;
using Xunit.Abstractions;

namespace RedisNearCache.Tests.Sentinel;

/// <summary>
/// The cache configured with sentinel endpoints and <c>serviceName=mymaster</c> instead of a data server. Two
/// things have to survive that indirection: the options clone in <c>RedisNearCacheConnection.BuildConfiguration</c>
/// must keep the service name (otherwise the private multiplexer would try to use the sentinels as data servers),
/// and the armer must find and arm the master Sentinel reports - and only it, not its replicas.
/// </summary>
public class SentinelTrackingTests
{
    private readonly ITestOutputHelper _out;

    public SentinelTrackingTests(ITestOutputHelper output) => _out = output;

    [Fact]
    public async Task ConnectingThroughSentinelArmsTheReportedMaster()
    {
        var master = await SentinelSupport.EnsureHealthyAsync();
        _out.WriteLine("before: " + await SentinelSupport.DescribeAsync());

        var p = await SentinelSupport.BuildAsync();
        try
        {
            _out.WriteLine($"private multiplexer servers: {SentinelSupport.DescribeMultiplexer(p.Multiplexer)}");

            // Exactly the master is armed, with the redirect id of our subscriber connection on that node.
            var armed = SentinelSupport.ArmedPorts(p.Armer);
            _out.WriteLine($"armed ports: {string.Join(",", armed)}; client name {p.Connection.ClientName}");
            Assert.Equal([master], armed);
            var redirect = SentinelSupport.RedirectFor(p.Armer, master);
            Assert.NotNull(redirect);

            // Independent confirmation from redis-cli on the master: our subscriber connection (flag P) is the redirect
            // target, and one of our connections carries the tracking flag (t).
            var lines = await SentinelSupport.ClientLinesAsync(master, p.Connection.ClientName);
            foreach (var line in lines) _out.WriteLine($"{master}: {line}");
            var subscriberIds = lines
                .Where(l => SentinelSupport.Field(l, "flags")?.Contains('P', StringComparison.Ordinal) == true)
                .Select(l => long.Parse(SentinelSupport.Field(l, "id")!, System.Globalization.CultureInfo.InvariantCulture))
                .ToArray();
            Assert.Contains(redirect!.Value, subscriberIds);
            var tracked = lines.Where(l => SentinelSupport.Field(l, "flags")?.Contains('t', StringComparison.Ordinal) == true).ToArray();
            Assert.NotEmpty(tracked);
            // Redis 7+ and Valkey also print the redirect id in CLIENT LIST (redir=); Redis 6.2 does not.
            foreach (var line in tracked)
            {
                if (SentinelSupport.Field(line, "redir") is { } redir) Assert.Equal(redirect.Value.ToString(System.Globalization.CultureInfo.InvariantCulture), redir);
            }

            // CLIENT TRACKINGINFO only describes the connection it is sent on, so redis-cli cannot ask it about ours;
            // it is read over the private multiplexer's interactive connection to the master instead.
            var server = p.Multiplexer.GetServers().Single(s => Resilience.ResilienceSupport.PortOf(s.EndPoint!) == master);
            var info = (StackExchange.Redis.RedisResult[])(await server.ExecuteAsync("CLIENT", "TRACKINGINFO"))!;
            var trackingInfo = string.Join(" ", info.Select(i => i.Resp2Type == StackExchange.Redis.ResultType.Array
                ? "[" + string.Join(",", ((StackExchange.Redis.RedisResult[])i!).Select(x => x.ToString())) + "]"
                : i.ToString()));
            _out.WriteLine($"CLIENT TRACKINGINFO on {master}: {trackingInfo}");
            Assert.Contains("redirect " + redirect.Value, trackingInfo, StringComparison.Ordinal);
            Assert.Contains("[on", trackingInfo, StringComparison.Ordinal);

            // Replicas are pre-armed (so a promotion needs no re-arm), but they are never armed masters: they appear in
            // ReplicaRedirectTargets only, and the redirect there is that replica's own subscriber connection.
            foreach (var replica in SentinelSupport.DataPorts.Where(port => port != master))
            {
                Assert.True(p.Multiplexer.GetServers().Any(s => s.EndPoint is not null && Resilience.ResilienceSupport.PortOf(s.EndPoint) == replica),
                    $"the private multiplexer does not list replica {replica}: {SentinelSupport.DescribeMultiplexer(p.Multiplexer)}");
                Assert.True(await Poll.UntilAsync(
                        () => p.Armer.ReplicaRedirectTargets.Keys.Any(ep => Resilience.ResilienceSupport.PortOf(ep) == replica),
                        TimeSpan.FromSeconds(15), TimeSpan.FromMilliseconds(200)),
                    $"replica {replica} was not pre-armed: {string.Join(",", p.Armer.ReplicaRedirectTargets.Select(kv => $"{kv.Key}=>{kv.Value}"))}");
                var replicaRedirect = p.Armer.ReplicaRedirectTargets.Single(kv => Resilience.ResilienceSupport.PortOf(kv.Key) == replica).Value;
                var replicaLines = await SentinelSupport.ClientLinesAsync(replica, p.Connection.ClientName);
                var replicaSubscribers = replicaLines
                    .Where(l => SentinelSupport.Field(l, "flags")?.Contains('P', StringComparison.Ordinal) == true)
                    .Select(l => long.Parse(SentinelSupport.Field(l, "id")!, System.Globalization.CultureInfo.InvariantCulture));
                Assert.Contains(replicaRedirect, replicaSubscribers);
                Assert.Contains(replicaLines, l => SentinelSupport.Field(l, "flags")?.Contains('t', StringComparison.Ordinal) == true);
            }
            Assert.Equal([master], SentinelSupport.ArmedPorts(p.Armer));
        }
        catch
        {
            p.Dump(_out);
            throw;
        }
        finally
        {
            await p.DisposeAsync();
        }
    }

    [Fact]
    public async Task ReadIsServedLocallyAndForeignWriteOnMasterInvalidates()
    {
        var master = await SentinelSupport.EnsureHealthyAsync();
        var key = TestHelpers.Key("sentinel-basic");

        var p = await SentinelSupport.BuildAsync();
        try
        {
            var cache = p.Cache;
            await SentinelSupport.CliAsync(master, "SET", key, "v1");

            Assert.Equal("v1", await cache.GetAsync<string>(key));
            Assert.True(cache.TryGetLocal<string>(key, out var local) && local == "v1", "the first read did not populate L1.");

            var hitsBefore = cache.Statistics.Hits;
            var missesBefore = cache.Statistics.Misses;
            Assert.Equal("v1", await cache.GetAsync<string>(key));
            Assert.Equal(hitsBefore + 1, cache.Statistics.Hits);
            Assert.Equal(missesBefore, cache.Statistics.Misses);

            var invalidationsBefore = cache.Statistics.Invalidations;
            await SentinelSupport.CliAsync(master, "SET", key, "v2");
            var evicted = await Poll.UntilAsync(() => !cache.TryGetLocal<string>(key, out _), TimeSpan.FromSeconds(10));
            Assert.True(evicted, $"a foreign write on master {master} did not evict the key. stats={cache.Statistics}");
            Assert.True(cache.Statistics.Invalidations > invalidationsBefore);
            Assert.Equal("v2", await cache.GetAsync<string>(key));
            _out.WriteLine($"stats: {cache.Statistics}");
        }
        catch
        {
            p.Dump(_out);
            throw;
        }
        finally
        {
            await p.DisposeAsync();
            await SentinelSupport.TryCliAsync(master, "DEL", key);
        }
    }
}
