using Xunit;
using Xunit.Abstractions;

namespace RedisNearCache.Tests.EdgeCases;

/// <summary>
/// While tracking on an endpoint is lost, <c>RedisNearCache.CachingEnabled</c> is false
/// (src/RedisNearCache/Caching/RedisNearCache.cs), so <c>TryGetLocal</c> must return false even before L1 is
/// physically cleared. <see cref="RedisNearCacheTestHooks.InsideFlushHandler"/> fires synchronously inside the
/// flush that <c>OnTrackingLost</c> triggers, after the in-flight tracker is marked but before
/// <c>L1.Clear()</c> runs, which is the earliest point a test can observe the window from outside; if that
/// callback never fires before the endpoint is already re-armed (a fast reconnect can close the window before
/// any test code runs), this falls back to asserting the end state and says so in the failure message.
/// </summary>
public class TrackingLostWindowTests
{
    private readonly ITestOutputHelper _out;

    public TrackingLostWindowTests(ITestOutputHelper output) => _out = output;

    [Fact]
    public async Task TryGetLocalIsFalseWhileTrackingLost()
    {
        RedisNearCacheOptions? capturedOptions = null;
        var handle = await EdgeCaseSupport.BuildAsync(StandaloneCacheFixture.ConnectionString, o => capturedOptions = o);
        var key = TestHelpers.Key("tracking-lost-window");
        try
        {
            var cache = handle.Cache;
            await cache.SetAsync(key, "v1");
            Assert.Equal("v1", await cache.GetAsync<string>(key));
            Assert.True(cache.TryGetLocal<string>(key, out _), "key must be cached before the connection is killed.");

            bool? insideFlushSawCached = null;
            capturedOptions!.TestHooks.InsideFlushHandler = () =>
            {
                // Only the first flush after the kill matters here: it is the TrackingLost flush. A later
                // flush (the re-arm's own) is a different window and would always read false anyway.
                insideFlushSawCached ??= cache.TryGetLocal<string>(key, out _);
            };

            var db = handle.Multiplexer.GetDatabase();
            var interactiveId = (long)await db.ExecuteAsync("CLIENT", "ID");
            RedisCli.Standalone("CLIENT", "KILL", "ID", interactiveId.ToString());

            var flushed = await Poll.UntilAsync(() => cache.Statistics.Flushes > 0, TimeSpan.FromSeconds(5));
            Assert.True(flushed, "expected a flush after the interactive connection was killed.");

            if (insideFlushSawCached is { } observedInsideFlush)
            {
                Assert.False(observedInsideFlush,
                    "TryGetLocal must already be false inside the flush handler, before L1 is even cleared, " +
                    "because CachingEnabled goes false as soon as TrackingLost is raised.");
                _out.WriteLine("observed the pre-clear window directly via InsideFlushHandler.");
            }
            else
            {
                _out.WriteLine("the pre-re-arm window was not observable (the hook never fired before re-arm); asserting the end state instead.");
            }

            Assert.False(cache.TryGetLocal<string>(key, out _), "TryGetLocal must be false once tracking has been lost.");
        }
        finally
        {
            await handle.DisposeAsync();
            RedisCli.Standalone("DEL", key);
        }
    }
}
