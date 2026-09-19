using Microsoft.Extensions.DependencyInjection;
using Xunit;
using Xunit.Abstractions;

namespace RedisNearCache.Tests.Chaos;

/// <summary>
/// Prefix opt-in under load. Keys outside <see cref="RedisNearCacheOptions.KeyPrefixes"/> are still read
/// through the tracked connection — so the server does track them and does push invalidations for them — but
/// they must never reach L1. The failure mode this hunts for is a concurrency path that stores a value
/// without consulting the prefix check, which would silently start caching keys the caller opted out of.
/// </summary>
/// <remarks>
/// The prefix half of this test has never failed: no <c>b:</c> key has ever reached L1.
///
/// The other half is a regression test for the same race as <see cref="StressNoStaleAfterQuiescenceTests"/>
/// and <see cref="InvalidationStormHotKeyTests"/> — a stale value surviving in L1 because
/// <c>OnKeyInvalidated</c> evicted L1 before marking the in-flight tracker — and has nothing to do with
/// prefixes; opting in simply means these keys are eligible for L1 and therefore eligible to go stale.
/// </remarks>
public class PrefixOptInStressTests : IAsyncLifetime
{
    private readonly ITestOutputHelper _out;
    private ServiceProvider _provider = null!;
    private IRedisNearCache _cache = null!;
    private ForeignClient _foreign = null!;

    public PrefixOptInStressTests(ITestOutputHelper output) => _out = output;

    public async Task InitializeAsync()
    {
        var services = new ServiceCollection();
        services.AddRedisNearCache(StandaloneCacheFixture.ConnectionString, o => o.KeyPrefixes.Add("a:"));
        _provider = services.BuildServiceProvider();
        _cache = _provider.GetRequiredService<IRedisNearCache>();
        await _cache.Ready;
        _foreign = await ForeignClient.ConnectAsync(StandaloneCacheFixture.ConnectionString);
    }

    public async Task DisposeAsync()
    {
        await _foreign.DisposeAsync();
        await _provider.DisposeAsync();
    }

    [Fact]
    public async Task PrefixOptInStress()
    {
        var run = TestHelpers.Key("prefix-stress");
        var aKeys = Enumerable.Range(0, 10).Select(i => $"a:{run}:{i}").ToArray();
        var bKeys = Enumerable.Range(0, 10).Select(i => $"b:{run}:{i}").ToArray();
        var allKeys = aKeys.Concat(bKeys).ToArray();

        foreach (var key in allKeys) await _foreign.Db.StringSetAsync(key, "0");

        var outcome = await StressHarness.RunAsync(
            _cache,
            allKeys,
            readerCount: 16,
            duration: TimeSpan.FromSeconds(3),
            writeAsync: async (key, value) => await _foreign.Db.StringSetAsync(key, value),
            // Checked on the reading thread, immediately after each read, so a b: key that reaches L1 even
            // momentarily is caught rather than being missed by an end-of-test snapshot.
            inspectRead: (o, key, _) =>
            {
                if (!key.StartsWith("b:", StringComparison.Ordinal)) return;
                if (_cache.TryGetLocal<string>(key, out var local)) o.Violations.Add($"{key} was in L1 as '{local}'");
            });

        Assert.True(outcome.ReaderFailures.IsEmpty,
            "readers threw: " + string.Join(" | ", outcome.ReaderFailures.Select(e => e.ToString()).Take(3)));
        Assert.True(outcome.Reads > 500, $"only {outcome.Reads} reads; the window was too quiet.");

        Assert.True(outcome.Violations.IsEmpty,
            "keys outside KeyPrefixes reached L1: " + string.Join(" | ", outcome.Violations.Take(5)));

        var settled = await ChaosSupport.QuiesceAsync(_cache, TimeSpan.FromSeconds(1));
        Assert.True(settled, "the system never quiesced.");

        foreach (var key in bKeys)
        {
            Assert.False(_cache.TryGetLocal<string>(key, out _), $"{key} is outside KeyPrefixes but ended up in L1.");
        }

        foreach (var (key, expected) in outcome.FinalValues)
        {
            var actual = await _foreign.Db.StringGetAsync(key);
            Assert.Equal(expected, actual.ToString());
        }

        var stale = await ChaosSupport.FindStaleAsync(_cache, _foreign, aKeys);
        _out.WriteLine($"reads={outcome.Reads} writes={outcome.Writes} stats={_cache.Statistics}");
        Assert.True(stale.Count == 0,
            "an opted-in (a:) key was left stale in L1: " + string.Join(" | ", stale) +
            ". Same cause as StressNoStaleAfterQuiescence; see the remarks on this class.");
    }
}
