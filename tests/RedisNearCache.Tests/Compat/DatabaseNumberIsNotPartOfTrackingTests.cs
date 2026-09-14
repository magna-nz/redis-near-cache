using RedisNearCache.Tests.EdgeCases;
using Xunit;
using Xunit.Abstractions;

namespace RedisNearCache.Tests.Compat;

/// <summary>
/// <b>Caveat, not a bug.</b> Redis's client-side caching invalidation table is keyed by key NAME alone; the
/// database number is not part of it. A client tracking <c>k</c> in db 2 is therefore invalidated when some
/// other client writes <c>k</c> in db 0, even though that write cannot possibly have changed what db 2 holds.
///
/// <para>
/// The consequence for RedisNearCache is a false eviction, never a stale read: the local copy is dropped and
/// the next read re-fetches from the configured database and gets the right value. Applications that share
/// key names across numbered databases (a pattern Redis itself discourages, and which does not exist in
/// cluster mode, where only db 0 exists) will see extra misses in proportion to how often the other database
/// is written. The fix is namespacing by key prefix rather than by database number.
/// </para>
/// </summary>
public class DatabaseNumberIsNotPartOfTrackingTests
{
    private readonly ITestOutputHelper _out;

    public DatabaseNumberIsNotPartOfTrackingTests(ITestOutputHelper output) => _out = output;

    [Fact]
    public async Task WriteInAnotherDatabaseEvictsTheSameKeyName()
    {
        var key = TestHelpers.Key("db-scope");
        var handle = await EdgeCaseSupport.BuildAsync($"{StandaloneCacheFixture.ConnectionString},defaultDatabase=2");
        try
        {
            var cache = handle.Cache;

            // Everything this cache does goes to db 2, because the private multiplexer was built from a
            // connection string with defaultDatabase=2.
            await cache.SetAsync(key, "value-in-db-2");
            Assert.True(
                await TestHelpers.ReadUntilCachedAsync(cache, key, "value-in-db-2"),
                "the db 2 key was not cached locally before the db 0 write.");
            Assert.Equal("value-in-db-2", RedisCli.Standalone("-n", "2", "GET", key));
            Assert.Equal(string.Empty, RedisCli.Standalone("-n", "0", "GET", key));

            // Same key NAME, different database. Nothing about db 2 changed.
            RedisCli.Standalone("-n", "0", "SET", key, "value-in-db-0");

            var evicted = await Poll.UntilAsync(() => !cache.TryGetLocal<string>(key, out _), TimeSpan.FromSeconds(3));
            _out.WriteLine(evicted
                ? "db 0 write evicted the db 2 entry: the tracking table ignores the database number."
                : "db 0 write did NOT evict the db 2 entry: this server scopes tracking per database.");
            Assert.True(
                evicted,
                "a write to the same key name in db 0 did not evict the db 2 copy; the documented caveat no longer holds for this server.");

            // The cache is still correct: it re-reads its own database, not the one that was written.
            Assert.Equal("value-in-db-2", await cache.GetAsync<string>(key));
        }
        finally
        {
            RedisCli.Standalone("-n", "0", "DEL", key);
            RedisCli.Standalone("-n", "2", "DEL", key);
            await handle.DisposeAsync();
        }
    }
}
