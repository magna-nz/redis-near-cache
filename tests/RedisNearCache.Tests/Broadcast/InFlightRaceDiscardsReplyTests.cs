using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace RedisNearCache.Tests.Broadcast;

/// <summary>
/// Needs access to the internal <see cref="RedisNearCacheOptions.TestHooks"/> hook, captured from the configure
/// callback at registration time, so it builds its own provider instead of using
/// <see cref="BroadcastStandaloneCacheFixture"/>.
/// </summary>
public class InFlightRaceDiscardsReplyTests : IAsyncLifetime
{
    private ServiceProvider _provider = null!;
    private IRedisNearCache _cache = null!;
    private RedisNearCacheOptions _options = null!;

    public async Task InitializeAsync()
    {
        var services = new ServiceCollection();
        services.AddRedisNearCache(BroadcastStandaloneCacheFixture.ConnectionString, o =>
        {
            o.TrackingMode = TrackingMode.Broadcast;
            o.KeyPrefixes.Add(BroadcastKey.Prefix);
            _options = o;
        });
        _provider = services.BuildServiceProvider();
        _cache = _provider.GetRequiredService<IRedisNearCache>();
        await _cache.Ready;
    }

    public async Task DisposeAsync() => await _provider.DisposeAsync();

    [Fact]
    public async Task InFlightRaceDiscardsReply()
    {
        var key = BroadcastKey.New("race");
        await _cache.SetAsync(key, "v1");

        _options.TestHooks.AfterRedisReadBeforeStore = async k =>
        {
            var before = _cache.Statistics.Invalidations;
            RedisCli.Standalone("SET", k, "v2");
            await Poll.UntilAsync(() => _cache.Statistics.Invalidations > before);
        };

        // The Redis reply for this read ("v1") arrives, but the hook above invalidates the key while the read is
        // in flight; the reply must be discarded rather than stored.
        _ = await _cache.GetAsync<string>(key);

        Assert.False(_cache.TryGetLocal<string>(key, out _), "a reply for a key invalidated in flight must not populate L1.");
        Assert.Equal(1, _cache.Statistics.RaceDiscards);

        _options.TestHooks.AfterRedisReadBeforeStore = null;
        var after = await _cache.GetAsync<string>(key);
        Assert.Equal("v2", after);
    }
}
