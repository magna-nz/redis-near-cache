using System.Diagnostics;
using RedisNearCache.Tests.Chaos;
using RedisNearCache.Tests.EdgeCases;
using Xunit;
using Xunit.Abstractions;

namespace RedisNearCache.Tests.Resilience;

/// <summary>
/// The whole server goes away and comes back empty. This is harsher than killing a connection: the container
/// restart resets the client-id counter (so the recorded redirect id is meaningless), drops every key (the
/// container runs with <c>--save ""</c> and <c>--appendonly no</c>) and gives the replica a master to resync
/// against. The danger is that L1 still holds pre-restart values while Redis now holds different ones, and no
/// invalidation will ever arrive for them because the server forgot it was tracking anything: that is silent,
/// unbounded staleness. The test reproduces exactly that setup - the keys are re-seeded from outside with NEW
/// values before the cache reads again - and asserts no read ever returns a pre-restart value.
/// </summary>
public class ContainerRestartTests
{
    private const int KeyCount = 20;

    private readonly ITestOutputHelper _out;

    public ContainerRestartTests(ITestOutputHelper output) => _out = output;

    [Fact]
    public async Task ContainerRestartRearmsAndNoStale()
    {
        var keys = StressHarness.KeyPool("container-restart", KeyCount);
        var handle = await EdgeCaseSupport.BuildAsync(StandaloneCacheFixture.ConnectionString);
        ForeignClient? truth = null;
        try
        {
            var cache = handle.Cache;

            // Seed from outside and cache all 20 keys.
            foreach (var key in keys) RedisCli.Standalone("SET", key, "v1");
            foreach (var key in keys)
            {
                Assert.Equal("v1", await cache.GetAsync<string>(key));
                Assert.True(cache.TryGetLocal<string>(key, out _), $"{key} was not cached before the restart.");
            }

            var rearmsBefore = cache.Statistics.Rearms;
            _out.WriteLine($"before restart: {cache.Statistics}");

            var sw = Stopwatch.StartNew();
            ResilienceSupport.RestartContainer(ResilienceSupport.StandaloneContainer);

            var up = await Poll.UntilAsync(
                () => ResilienceSupport.PingOk(ResilienceSupport.StandaloneContainer),
                TimeSpan.FromSeconds(60), TimeSpan.FromMilliseconds(100));
            Assert.True(up, "the standalone container never answered PING again after the restart.");
            _out.WriteLine($"container answered PING again after {sw.ElapsedMilliseconds} ms");

            // The restart lost every key. Re-seed with DIFFERENT values from outside, before the cache reads
            // again: from here on, any read that returns "v1" came from a stale L1 entry and nowhere else.
            foreach (var key in keys) RedisCli.Standalone("SET", key, "v2");

            // Read continuously across the reconnect window, recording (never asserting inside the loop, so
            // the readers keep running) any read that produced a pre-restart value.
            var violations = new System.Collections.Concurrent.ConcurrentBag<string>();
            using var stop = new CancellationTokenSource();
            var reader = Task.Run(async () =>
            {
                while (!stop.IsCancellationRequested)
                {
                    foreach (var key in keys)
                    {
                        try
                        {
                            var value = await cache.GetAsync<string>(key);
                            if (value == "v1") violations.Add($"{key} read 'v1' after the restart and re-seed");
                            if (cache.TryGetLocal<string>(key, out string? local) && local == "v1")
                                violations.Add($"{key} held 'v1' in L1 after the restart and re-seed");
                        }
                        catch (Exception ex) when (ex is StackExchange.Redis.RedisConnectionException
                                                      or StackExchange.Redis.RedisTimeoutException
                                                      or StackExchange.Redis.RedisServerException)
                        {
                            // The server is still coming back; not the thing under test.
                        }
                    }

                    await Task.Yield();
                }
            });

            var rearmed = await Poll.UntilAsync(
                () => handle.Multiplexer.IsConnected && cache.Statistics.Rearms > rearmsBefore,
                TimeSpan.FromSeconds(60), TimeSpan.FromMilliseconds(100));

            await stop.CancelAsync();
            await reader;

            _out.WriteLine($"after restart: {cache.Statistics} (rearms before={rearmsBefore})");
            Assert.True(rearmed,
                $"the cache did not reconnect and re-arm after the container restart. connected={handle.Multiplexer.IsConnected}, stats={cache.Statistics}");

            Assert.True(await ChaosSupport.QuiesceAsync(cache), "the cache never settled after the restart.");

            // Every key: either evicted, or exactly what Redis holds now. Nothing stale, ever.
            truth = await ForeignClient.ConnectAsync(StandaloneCacheFixture.ConnectionString);
            var stale = await ChaosSupport.FindStaleAsync(cache, truth, keys);
            Assert.True(stale.Count == 0, "L1 kept pre-restart values:\n" + string.Join("\n", stale));
            Assert.True(violations.IsEmpty, "reads returned pre-restart values:\n" + string.Join("\n", violations));

            // Tracking really was re-armed at the restarted server: a foreign write still evicts.
            var probe = keys[0];
            Assert.True(await TestHelpers.ReadUntilCachedAsync(cache, probe, "v2", TimeSpan.FromSeconds(30)),
                $"{probe} was not cached again after the restart.");
            RedisCli.Standalone("SET", probe, "v3");
            var evicted = await Poll.UntilAsync(() => !cache.TryGetLocal<string>(probe, out _), TimeSpan.FromSeconds(10));
            Assert.True(evicted, "invalidations are being lost after the container restart: tracking was not re-armed.");
            Assert.Equal("v3", await cache.GetAsync<string>(probe));

            // The replica container is not restarted, but its master was: it must notice and resync. A replica
            // stuck on master_link_status:down would serve indefinitely stale data to anything reading it.
            var linkUp = await Poll.UntilAsync(
                () => EdgeCaseSupport.Replica("INFO", "replication").Contains("master_link_status:up", StringComparison.Ordinal),
                TimeSpan.FromSeconds(60), TimeSpan.FromMilliseconds(250));
            Assert.True(linkUp,
                "the replica never re-established its link to the restarted master:\n" + EdgeCaseSupport.Replica("INFO", "replication"));
        }
        finally
        {
            if (truth is not null) await truth.DisposeAsync();
            await handle.DisposeAsync();
            foreach (var key in keys)
            {
                try { RedisCli.Standalone("DEL", key); } catch (InvalidOperationException) { /* server may be gone */ }
            }
        }
    }
}
