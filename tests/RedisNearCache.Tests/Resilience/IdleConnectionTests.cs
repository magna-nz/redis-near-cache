using Microsoft.Extensions.DependencyInjection;
using RedisNearCache.Tests.EdgeCases;
using StackExchange.Redis;
using Xunit;
using Xunit.Abstractions;

namespace RedisNearCache.Tests.Resilience;

/// <summary>
/// A cache that nobody reads for a while. Tracking state lives on the server connection, so anything that
/// silently drops or recycles that connection during an idle period takes tracking with it and the next write
/// produces no invalidation - the entry read an hour ago is still served. StackExchange.Redis defends against
/// that with a 60 s keep-alive; this test idles past it (65 s) with no traffic at all and then proves the
/// connection is still the same tracked one: no connection event was raised, no re-arm happened, the redirect
/// client id is unchanged, the L1 entry from before the idle is still there, the server still reports
/// <c>CLIENT TRACKINGINFO</c> flags=on with that redirect id, and a foreign write still evicts.
/// </summary>
/// <remarks>
/// Tagged <c>Slow</c> because of the 65 s idle, which is the subject of the test rather than a poll, but
/// deliberately NOT excluded from CI: the failure it guards against is invisible in every fast test.
/// </remarks>
[Trait("Category", "Slow")]
public class IdleConnectionTests
{
    private static readonly TimeSpan IdlePeriod = TimeSpan.FromSeconds(65);

    private readonly ITestOutputHelper _out;

    public IdleConnectionTests(ITestOutputHelper output) => _out = output;

    [Fact]
    public async Task IdleConnectionKeepsTracking()
    {
        var handle = await EdgeCaseSupport.BuildAsync(StandaloneCacheFixture.ConnectionString);
        var key = TestHelpers.Key("idle");
        try
        {
            var cache = handle.Cache;
            var endpoint = handle.Multiplexer.GetEndPoints()[0];

            RedisCli.Standalone("SET", key, "v1");
            Assert.True(await TestHelpers.ReadUntilCachedAsync(cache, key, "v1"), "the key was not cached before the idle period.");

            var rearmsBefore = cache.Statistics.Rearms;
            var redirectBefore = handle.Armer.RedirectTargets[endpoint];
            _out.WriteLine($"before {IdlePeriod.TotalSeconds:0} s idle: redirect={redirectBefore} {cache.Statistics}");

            // Record what happens during the idle rather than only reading the counters afterwards: the
            // counters cannot tell a dropped connection apart from someone else's FLUSHDB on the shared
            // database, and only the first of those is a failure of this test.
            var events = new System.Collections.Concurrent.ConcurrentQueue<string>();
            var connectionEvents = new System.Collections.Concurrent.ConcurrentQueue<string>();
            var externalFlushes = 0;
            var clock = System.Diagnostics.Stopwatch.StartNew();
            void Record(string what) => events.Enqueue($"+{clock.Elapsed.TotalSeconds:0.0}s {what}");
            void RecordConnection(string what)
            {
                Record(what);
                connectionEvents.Enqueue(what);
            }

            var listener = handle.Provider.GetRequiredService<Internal.IInvalidationListener>();
            listener.FlushAll += () =>
            {
                Interlocked.Increment(ref externalFlushes);
                Record("listener FlushAll (null invalidation: someone ran FLUSHDB/FLUSHALL)");
            };
            listener.KeyInvalidated += k => Record($"listener KeyInvalidated {k}");
            handle.Armer.TrackingLost += ep => RecordConnection($"armer TrackingLost {ep}");
            handle.Armer.Armed += e => RecordConnection($"armer Armed {e.EndPoint} redirect={e.RedirectClientId} ({e.Reason})");
            handle.Armer.EndpointRemoved += ep => RecordConnection($"armer EndpointRemoved {ep}");
            handle.Multiplexer.ConnectionFailed += (_, e) => RecordConnection($"mux ConnectionFailed {e.EndPoint} {e.ConnectionType} {e.FailureType}");
            handle.Multiplexer.ConnectionRestored += (_, e) => RecordConnection($"mux ConnectionRestored {e.EndPoint} {e.ConnectionType}");

            // The idle is the test. Nothing is read, written or polled for the whole period, so the only thing
            // keeping the connection alive is the client's keep-alive.
            await Task.Delay(IdlePeriod);

            _out.WriteLine($"during idle: {(events.IsEmpty ? "<nothing>" : string.Join(" | ", events))}");
            _out.WriteLine($"after idle: {cache.Statistics}, redirect targets={string.Join(",", handle.Armer.RedirectTargets.Select(kv => $"{kv.Key}=>{kv.Value}"))}");

            // The heart of it: neither connection went away and came back. A reconnect would have produced a
            // connection event, a re-arm and a new redirect client id.
            Assert.True(connectionEvents.IsEmpty,
                "the connection did not survive the idle period; the keep-alive did not hold it open: " + string.Join(" | ", connectionEvents));
            Assert.Equal(rearmsBefore, cache.Statistics.Rearms);
            Assert.Equal(redirectBefore, handle.Armer.RedirectTargets[endpoint]);

            // The entry survived the idle (L1MaxAge defaults to 5 minutes) and is still served locally. Skipped
            // only if something outside this test flushed the shared database, which legitimately drops it.
            if (Volatile.Read(ref externalFlushes) == 0)
            {
                Assert.True(cache.TryGetLocal<string>(key, out var local), "the L1 entry was dropped during the idle period.");
                Assert.Equal("v1", local);
                var hitsBefore = cache.Statistics.Hits;
                Assert.Equal("v1", await cache.GetAsync<string>(key));
                Assert.Equal(hitsBefore + 1, cache.Statistics.Hits);
            }
            else
            {
                _out.WriteLine("an external FLUSHDB landed during the idle period; skipping the L1-survived check and re-seeding.");
                RedisCli.Standalone("SET", key, "v1");
                Assert.True(await TestHelpers.ReadUntilCachedAsync(cache, key, "v1"), "the key could not be cached again after an external flush.");
            }

            // The server still considers the connection tracked, with the redirect we armed.
            var info = await handle.Multiplexer.GetServer(endpoint).ExecuteAsync("CLIENT", "TRACKINGINFO");
            var (flags, redirect) = ParseTrackingInfo(info);
            _out.WriteLine($"CLIENT TRACKINGINFO after idle: flags=[{string.Join(" ", flags)}] redirect={redirect}");
            Assert.Contains("on", flags);
            Assert.Equal(redirectBefore, redirect);

            // And the end-to-end proof: a foreign write still evicts after an idle period.
            RedisCli.Standalone("SET", key, "v2");
            var evicted = await Poll.UntilAsync(() => !cache.TryGetLocal<string>(key, out _), TimeSpan.FromSeconds(5));
            Assert.True(evicted, "invalidations stopped arriving after the connection sat idle past the keep-alive.");
            Assert.Equal("v2", await cache.GetAsync<string>(key));
        }
        finally
        {
            await handle.DisposeAsync();
            RedisCli.Standalone("DEL", key);
        }
    }

    /// <summary>Reads flags and redirect out of a RESP2 <c>CLIENT TRACKINGINFO</c> reply (a flat key/value array).</summary>
    private static (IReadOnlyList<string> Flags, long Redirect) ParseTrackingInfo(RedisResult info)
    {
        var flags = new List<string>();
        long redirect = -1;
        var items = (RedisResult[]?)info ?? [];
        for (var i = 0; i + 1 < items.Length; i += 2)
        {
            var name = items[i].ToString();
            if (string.Equals(name, "flags", StringComparison.OrdinalIgnoreCase))
            {
                foreach (var f in (RedisResult[]?)items[i + 1] ?? []) flags.Add(f.ToString()!);
            }
            else if (string.Equals(name, "redirect", StringComparison.OrdinalIgnoreCase))
            {
                redirect = (long)items[i + 1];
            }
        }

        return (flags, redirect);
    }
}
