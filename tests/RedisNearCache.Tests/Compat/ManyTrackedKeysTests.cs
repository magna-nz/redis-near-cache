using System.Diagnostics;
using RedisNearCache.Tests.Chaos;
using RedisNearCache.Tests.EdgeCases;
using StackExchange.Redis;
using Xunit;
using Xunit.Abstractions;

namespace RedisNearCache.Tests.Compat;

/// <summary>
/// Scale of the server-side tracking table: one connection reading 100,000 distinct keys must leave 100,000
/// keys in the server's tracking table (<c>INFO stats</c> / <c>tracking_total_keys</c>), and every one of them
/// must still be individually invalidated afterwards. This is the test that would catch the server silently
/// capping what it tracks (<c>tracking-table-max-keys</c> defaults to 1,000,000, so 100,000 is comfortably
/// inside it) or the library losing invalidations once L1 is large.
/// </summary>
public class ManyTrackedKeysTests
{
    private const int KeyCount = 100_000;
    private const int SeedBatch = 1_000;
    private const int ReadConcurrency = 64;
    private const int SampleSize = 50;

    private readonly ITestOutputHelper _out;

    public ManyTrackedKeysTests(ITestOutputHelper output) => _out = output;

    [Fact]
    public async Task HundredThousandKeysAreTrackedAndIndividuallyInvalidated()
    {
        var prefix = TestHelpers.Key("many");
        var keys = Enumerable.Range(0, KeyCount).Select(i => $"{prefix}:{i}").ToArray();

        var handle = await EdgeCaseSupport.BuildAsync(
            StandaloneCacheFixture.ConnectionString,
            o => o.L1SizeLimit = 200_000);
        await using var foreign = await ForeignClient.ConnectAsync(StandaloneCacheFixture.ConnectionString);
        var stopwatch = Stopwatch.StartNew();
        try
        {
            var cache = handle.Cache;

            // Seeding goes through a plain foreign multiplexer in MSET batches: 100,000 individual round
            // trips through the cache would dominate the test's runtime and prove nothing extra.
            for (var start = 0; start < KeyCount; start += SeedBatch)
            {
                var pairs = new KeyValuePair<RedisKey, RedisValue>[Math.Min(SeedBatch, KeyCount - start)];
                for (var i = 0; i < pairs.Length; i++)
                {
                    pairs[i] = new KeyValuePair<RedisKey, RedisValue>(keys[start + i], $"v-{start + i}");
                }

                await foreign.Db.StringSetAsync(pairs);
            }

            _out.WriteLine($"seeded {KeyCount} keys in {stopwatch.ElapsedMilliseconds} ms");

            var trackingBefore = CompatSupport.TrackingTotalKeys();

            // Bounded concurrency: 64 reads in flight over the one private multiplexer.
            var next = -1;
            var readers = Enumerable.Range(0, ReadConcurrency).Select(async _ =>
            {
                while (true)
                {
                    var index = Interlocked.Increment(ref next);
                    if (index >= KeyCount) return;
                    var value = await cache.GetAsync<string>(keys[index]);
                    Assert.Equal($"v-{index}", value);
                }
            }).ToArray();
            await Task.WhenAll(readers);

            _out.WriteLine($"read {KeyCount} keys through the cache after {stopwatch.ElapsedMilliseconds} ms total");

            var trackingAfter = CompatSupport.TrackingTotalKeys();
            _out.WriteLine($"tracking_total_keys: {trackingBefore} before, {trackingAfter} after");
            Assert.True(
                trackingAfter >= KeyCount,
                $"the server reports tracking_total_keys={trackingAfter} after {KeyCount} distinct reads; it is not tracking everything this connection read.");

            // A random sample rather than all 100,000: the claim is that a key deep in a large tracking table
            // is invalidated as reliably as a key in a small one.
            var random = new Random(20260914);
            var sample = Enumerable.Range(0, SampleSize).Select(_ => keys[random.Next(KeyCount)]).Distinct().ToArray();
            foreach (var key in sample)
            {
                Assert.True(cache.TryGetLocal<string>(key, out _), $"sampled key {key} was not in L1 before the write.");
            }

            var writes = sample.Select(k => foreign.Db.StringSetAsync(k, "rewritten")).ToArray();
            await Task.WhenAll(writes);

            var allEvicted = await Poll.UntilAsync(
                () => sample.All(k => !cache.TryGetLocal<string>(k, out _)),
                TimeSpan.FromSeconds(10));
            var survivors = sample.Where(k => cache.TryGetLocal<string>(k, out _)).ToArray();
            Assert.True(
                allEvicted,
                $"{survivors.Length} of {sample.Length} written keys were not evicted: {string.Join(", ", survivors.Take(5))}");

            foreach (var key in sample)
            {
                Assert.Equal("rewritten", await cache.GetAsync<string>(key));
            }

            _out.WriteLine($"total elapsed {stopwatch.Elapsed.TotalSeconds:0.0} s");
        }
        finally
        {
            // UNLINK in batches so the database is left as it was found; 100,000 leftover keys would change
            // the memory baseline for every test that runs after this one.
            for (var start = 0; start < KeyCount; start += SeedBatch)
            {
                var batch = new RedisKey[Math.Min(SeedBatch, KeyCount - start)];
                for (var i = 0; i < batch.Length; i++) batch[i] = keys[start + i];
                await foreign.Db.KeyDeleteAsync(batch, CommandFlags.FireAndForget);
            }

            await handle.DisposeAsync();
        }
    }
}
