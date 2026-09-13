using Microsoft.Extensions.DependencyInjection;
using RedisNearCache.Internal;
using Xunit;

namespace RedisNearCache.Tests;

/// <summary>
/// Regression tests for the "store after clear" window: a read whose reply is stored while a whole-cache
/// flush (FLUSHDB) or a re-arm (interactive reconnect) is being handled must not leave a stale entry in L1.
/// The handlers must mark the in-flight tracker BEFORE clearing L1, so the read's post-store re-check sees
/// the mark. The InsideFlushHandler hook releases a parked reader exactly between those two steps.
/// Uses FLUSHDB, so it runs in the "flush" collection, alone.
/// </summary>
[Collection("flush")]
public class InFlightDuringFlushOrRearmTests : IAsyncLifetime
{
    private ServiceProvider _provider = null!;
    private IRedisNearCache _cache = null!;
    private RedisNearCacheOptions _options = null!;
    private RedisNearCacheConnection _connection = null!;

    public async Task InitializeAsync()
    {
        var services = new ServiceCollection();
        services.AddRedisNearCache(StandaloneCacheFixture.ConnectionString, o => _options = o);
        _provider = services.BuildServiceProvider();
        _cache = _provider.GetRequiredService<IRedisNearCache>();
        _connection = _provider.GetRequiredService<RedisNearCacheConnection>();
        await _cache.Ready;
    }

    public async Task DisposeAsync()
    {
        _options.TestHooks.AfterRedisReadBeforeStore = null;
        _options.TestHooks.InsideFlushHandler = null;
        await _provider.DisposeAsync();
    }

    /// <summary>Parks one read after its reply arrived; returns the task and a gate that lets it store.</summary>
    private (Task<string?> read, TaskCompletionSource parked, TaskCompletionSource gate) ParkRead(string key)
    {
        var parked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _options.TestHooks.AfterRedisReadBeforeStore = async _ =>
        {
            parked.TrySetResult();
            await gate.Task;
        };
        // Release the parked reader from inside the flush handler, then give it time to store and re-check.
        _options.TestHooks.InsideFlushHandler = () =>
        {
            gate.TrySetResult();
            Thread.Sleep(100);
        };
        return (_cache.GetAsync<string>(key).AsTask(), parked, gate);
    }

    [Fact]
    public async Task FlushDbDuringInFlightReadDoesNotLeaveStaleEntry()
    {
        var key = TestHelpers.Key("flushrace");
        RedisCli.Standalone("SET", key, "v1");
        var flushesBefore = _cache.Statistics.Flushes;

        var (read, parked, _) = ParkRead(key);
        await parked.Task.WaitAsync(TimeSpan.FromSeconds(5));

        RedisCli.Standalone("FLUSHDB");
        var value = await read.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.True(await Poll.UntilAsync(() => _cache.Statistics.Flushes > flushesBefore), "flush was not observed");

        Assert.Equal("v1", value); // the read itself was valid when issued
        Assert.False(_cache.TryGetLocal<string>(key, out _), "a reply stored inside the flush handler must not survive in L1");
        Assert.True(_cache.Statistics.RaceDiscards >= 1);
    }

    [Fact]
    public async Task RearmDuringInFlightReadDoesNotLeaveStaleEntry()
    {
        var key = TestHelpers.Key("rearmrace");
        RedisCli.Standalone("SET", key, "v1");
        var db = _connection.Multiplexer.GetDatabase();
        var rearmsBefore = _cache.Statistics.Rearms;

        var (read, parked, _) = ParkRead(key);
        await parked.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // Kill our own interactive connection: the server drops tracking, we reconnect and re-arm, and the
        // Armed handler releases the parked reader in the middle of its flush.
        var interactiveId = (long)await db.ExecuteAsync("CLIENT", "ID");
        RedisCli.Standalone("CLIENT", "KILL", "ID", interactiveId.ToString());
        _ = await read.WaitAsync(TimeSpan.FromSeconds(15));
        Assert.True(await Poll.UntilAsync(() => _cache.Statistics.Rearms > rearmsBefore, TimeSpan.FromSeconds(10)), "re-arm was not observed");

        Assert.False(_cache.TryGetLocal<string>(key, out _), "a reply stored inside the re-arm flush must not survive in L1");
        _options.TestHooks.AfterRedisReadBeforeStore = null;
        _options.TestHooks.InsideFlushHandler = null;
        RedisCli.Standalone("SET", key, "v2");
        Assert.True(await Poll.UntilAsync(() => !_cache.TryGetLocal<string>(key, out _)));
        Assert.Equal("v2", await _cache.GetAsync<string>(key));
    }
}
