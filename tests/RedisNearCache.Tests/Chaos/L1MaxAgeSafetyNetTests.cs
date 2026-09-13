using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Xunit;
using Xunit.Abstractions;

namespace RedisNearCache.Tests.Chaos;

/// <summary>
/// <see cref="RedisNearCacheOptions.L1MaxAge"/> is the last line of defence: if every other mechanism in this
/// directory failed and an invalidation was lost, an entry still has to disappear on its own. Nothing writes
/// the key here, so only the age can drop it.
/// Needs its own provider because the age has to be configured at registration time.
/// </summary>
public class L1MaxAgeSafetyNetTests : IAsyncLifetime
{
    private static readonly TimeSpan MaxAge = TimeSpan.FromMilliseconds(300);

    private readonly ITestOutputHelper _out;
    private ServiceProvider _provider = null!;
    private IRedisNearCache _cache = null!;

    public L1MaxAgeSafetyNetTests(ITestOutputHelper output) => _out = output;

    public async Task InitializeAsync()
    {
        var services = new ServiceCollection();
        services.AddRedisNearCache(StandaloneCacheFixture.ConnectionString, o => o.L1MaxAge = MaxAge);
        _provider = services.BuildServiceProvider();
        _cache = _provider.GetRequiredService<IRedisNearCache>();
        await _cache.Ready;
    }

    public async Task DisposeAsync() => await _provider.DisposeAsync();

    [Fact]
    public async Task L1MaxAgeSafetyNet()
    {
        var key = TestHelpers.Key("maxage");
        await _cache.SetAsync(key, "v1");
        Assert.Equal("v1", await _cache.GetAsync<string>(key));
        Assert.True(_cache.TryGetLocal<string>(key, out _), "the key was not cached at all, so the expiry proves nothing.");

        var sw = Stopwatch.StartNew();
        var expired = await Poll.UntilAsync(
            () => !_cache.TryGetLocal<string>(key, out _),
            TimeSpan.FromMilliseconds(1_500), TimeSpan.FromMilliseconds(20));
        sw.Stop();

        _out.WriteLine($"L1MaxAge={MaxAge.TotalMilliseconds} ms; entry disappeared after {sw.ElapsedMilliseconds} ms without any write");
        Assert.True(expired, $"the L1 entry survived {sw.ElapsedMilliseconds} ms with L1MaxAge={MaxAge.TotalMilliseconds} ms and no invalidation.");

        // And the expiry is an eviction, not a poisoning: the next read still returns the right value.
        Assert.Equal("v1", await _cache.GetAsync<string>(key));
    }
}
