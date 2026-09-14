using RedisNearCache.Tests.Chaos;
using Xunit;
using Xunit.Abstractions;

namespace RedisNearCache.Tests.Compat;

/// <summary>
/// One theory case per kind of server-side mutation, proving that the near cache reacts to the whole command
/// surface rather than just <c>SET</c>/<c>DEL</c>: expiry, rename, copy, move, scripts, functions, restore,
/// flush and swapdb all reach L1 through the same <c>__redis__:invalidate</c> channel.
///
/// <para>
/// Three cases assert a NEGATIVE or otherwise surprising result and are documented inline where they are
/// implemented: <c>SET ... NX</c> against an existing key does NOT invalidate (Redis only signals a key as
/// modified when the write actually happened); <c>SET</c> with the value the key already holds DOES
/// invalidate (Redis never compares the old value); and <c>SWAPDB</c> invalidates NOTHING, leaving the near
/// cache serving the database that was swapped away (see <see cref="SwapDbAsync"/> - the one real staleness
/// hazard in this file, and one the library cannot see). All three were verified empirically against the
/// server this suite runs on, not assumed.
/// </para>
/// </summary>
// FLUSHALL and SWAPDB affect the whole server, not just this test's keys; they share the named collection
// with the existing FLUSHDB test so that re-enabling parallelization later cannot run them next to anything.
[Collection("flush")]
public class MutationKindsInvalidateTests : IClassFixture<StandaloneCacheFixture>
{
    private static readonly TimeSpan EvictionDeadline = TimeSpan.FromSeconds(3);

    private readonly StandaloneCacheFixture _fx;
    private readonly ITestOutputHelper _out;

    public MutationKindsInvalidateTests(StandaloneCacheFixture fx, ITestOutputHelper output)
    {
        _fx = fx;
        _out = output;
    }

    [Theory]
    [InlineData("SET")]
    [InlineData("DEL")]
    [InlineData("EXPIRE")]
    [InlineData("PEXPIRE")]
    [InlineData("RENAME")]
    [InlineData("COPY")]
    [InlineData("MOVE")]
    [InlineData("GETDEL")]
    [InlineData("SETNX_ON_EXISTING")]
    [InlineData("MSET")]
    [InlineData("APPEND")]
    [InlineData("INCR")]
    [InlineData("EVAL")]
    [InlineData("FCALL")]
    [InlineData("RESTORE")]
    [InlineData("SET_SAME_VALUE")]
    [InlineData("FLUSHALL")]
    [InlineData("SWAPDB")]
    public async Task MutationInvalidatesTrackedKey(string kind)
    {
        switch (kind)
        {
            case "SET": await SetAsync(); break;
            case "DEL": await DelAsync(); break;
            case "EXPIRE": await ExpireAsync(); break;
            case "PEXPIRE": await PexpireAsync(); break;
            case "RENAME": await RenameAsync(); break;
            case "COPY": await CopyAsync(); break;
            case "MOVE": await MoveAsync(); break;
            case "GETDEL": await GetDelAsync(); break;
            case "SETNX_ON_EXISTING": await SetNxOnExistingAsync(); break;
            case "MSET": await MsetAsync(); break;
            case "APPEND": await AppendAsync(); break;
            case "INCR": await IncrAsync(); break;
            case "EVAL": await EvalAsync(); break;
            case "FCALL": await FcallAsync(); break;
            case "RESTORE": await RestoreAsync(); break;
            case "SET_SAME_VALUE": await SetSameValueAsync(); break;
            case "FLUSHALL": await FlushAllAsync(); break;
            case "SWAPDB": await SwapDbAsync(); break;
            default: throw new ArgumentOutOfRangeException(nameof(kind), kind, "unknown mutation kind");
        }
    }

    private async Task SetAsync()
    {
        var key = await TrackedKeyAsync("set");
        try
        {
            RedisCli.Standalone("SET", key, "v2");
            await AssertEvictedAsync(key, "SET overwrote the value");
            Assert.Equal("v2", await _fx.Cache.GetAsync<string>(key));
        }
        finally
        {
            RedisCli.Standalone("DEL", key);
        }
    }

    private async Task DelAsync()
    {
        var key = await TrackedKeyAsync("del");
        RedisCli.Standalone("DEL", key);
        await AssertEvictedAsync(key, "DEL removed the key");
        Assert.Null(await _fx.Cache.GetAsync<string>(key));
    }

    private async Task ExpireAsync()
    {
        var key = await TrackedKeyAsync("expire");
        try
        {
            // EXPIRE is itself a modification, so Redis invalidates at the moment the TTL is set as well as
            // when the key finally expires. Either is enough for the eviction assertion; the second assertion
            // below is what proves the key really went away.
            RedisCli.Standalone("EXPIRE", key, "1");
            await AssertEvictedAsync(key, "EXPIRE set a TTL on the key");

            // Each poll is a docker exec (~80 ms), so it steps at 100 ms rather than the 20 ms default.
            var gone = await Poll.UntilAsync(
                () => RedisCli.Standalone("EXISTS", key) == "0",
                TimeSpan.FromSeconds(5),
                TimeSpan.FromMilliseconds(100));
            Assert.True(gone, "the key did not expire server-side within the deadline.");
            Assert.Null(await _fx.Cache.GetAsync<string>(key));
        }
        finally
        {
            RedisCli.Standalone("DEL", key);
        }
    }

    private async Task PexpireAsync()
    {
        var key = await TrackedKeyAsync("pexpire");
        try
        {
            RedisCli.Standalone("PEXPIRE", key, "100");
            await AssertEvictedAsync(key, "PEXPIRE set a 100 ms TTL on the key");

            var gone = await Poll.UntilAsync(
                () => RedisCli.Standalone("EXISTS", key) == "0",
                TimeSpan.FromSeconds(3),
                TimeSpan.FromMilliseconds(100));
            Assert.True(gone, "the key did not expire server-side within the deadline.");
        }
        finally
        {
            RedisCli.Standalone("DEL", key);
        }
    }

    private async Task RenameAsync()
    {
        var source = await TrackedKeyAsync("rename-src");
        var destination = await TrackedKeyAsync("rename-dst", "other");
        try
        {
            // Both keys are tracked: the source disappears and the destination is overwritten, so the server
            // must invalidate both, not only the key named as the RENAME target.
            RedisCli.Standalone("RENAME", source, destination);
            await AssertEvictedAsync(source, "RENAME removed the source key");
            await AssertEvictedAsync(destination, "RENAME overwrote the destination key");
            Assert.Null(await _fx.Cache.GetAsync<string>(source));
            Assert.Equal("v1", await _fx.Cache.GetAsync<string>(destination));
        }
        finally
        {
            RedisCli.Standalone("DEL", source, destination);
        }
    }

    private async Task CopyAsync()
    {
        if (!CompatSupport.RequireServer(new Version(6, 2), "COPY", _out)) return;

        var source = TestHelpers.Key("copy-src");
        var destination = await TrackedKeyAsync("copy-dst", "destination-value");
        try
        {
            RedisCli.Standalone("SET", source, "source-value");
            RedisCli.Standalone("COPY", source, destination, "REPLACE");

            await AssertEvictedAsync(destination, "COPY overwrote the destination key");
            Assert.Equal("source-value", await _fx.Cache.GetAsync<string>(destination));
        }
        finally
        {
            RedisCli.Standalone("DEL", source, destination);
        }
    }

    private async Task MoveAsync()
    {
        var key = await TrackedKeyAsync("move");
        try
        {
            // MOVE deletes the key from db 0. Note that tracking is by key NAME only (see
            // DatabaseNumberIsNotPartOfTrackingTests), so the key being re-created in db 1 is irrelevant here.
            RedisCli.Standalone("MOVE", key, "1");
            await AssertEvictedAsync(key, "MOVE removed the key from db 0");
            Assert.Null(await _fx.Cache.GetAsync<string>(key));
        }
        finally
        {
            RedisCli.Standalone("-n", "1", "DEL", key);
            RedisCli.Standalone("DEL", key);
        }
    }

    private async Task GetDelAsync()
    {
        if (!CompatSupport.RequireServer(new Version(6, 2), "GETDEL", _out)) return;

        var key = await TrackedKeyAsync("getdel");
        Assert.Equal("v1", RedisCli.Standalone("GETDEL", key));
        await AssertEvictedAsync(key, "GETDEL removed the key");
        Assert.Null(await _fx.Cache.GetAsync<string>(key));
    }

    private async Task SetNxOnExistingAsync()
    {
        var key = await TrackedKeyAsync("setnx");
        try
        {
            // SET ... NX against a key that already exists is a no-op: Redis never calls signalModifiedKey,
            // so no invalidation is sent and the L1 copy legitimately survives. This is the useful half of
            // the invariant - the server invalidates on actual modification, not on the attempt - and it is
            // asserted rather than assumed because it is the behaviour applications depend on for
            // "set-if-absent" locks not to churn every reader's near cache.
            Assert.Equal(string.Empty, RedisCli.Standalone("SET", key, "v2", "NX"));

            await ChaosSupport.QuiesceAsync(_fx.Cache, TimeSpan.FromMilliseconds(500), TimeSpan.FromSeconds(5));
            Assert.True(
                _fx.Cache.TryGetLocal<string>(key, out var local),
                "SET ... NX on an existing key evicted the L1 copy; this server invalidates on attempted, not actual, modification.");
            Assert.Equal("v1", local);
            Assert.Equal("v1", RedisCli.Standalone("GET", key));
        }
        finally
        {
            RedisCli.Standalone("DEL", key);
        }
    }

    private async Task MsetAsync()
    {
        var key = await TrackedKeyAsync("mset");
        try
        {
            RedisCli.Standalone("MSET", key, "v2");
            await AssertEvictedAsync(key, "MSET overwrote the key");
            Assert.Equal("v2", await _fx.Cache.GetAsync<string>(key));
        }
        finally
        {
            RedisCli.Standalone("DEL", key);
        }
    }

    private async Task AppendAsync()
    {
        var key = await TrackedKeyAsync("append");
        try
        {
            RedisCli.Standalone("APPEND", key, "-tail");
            await AssertEvictedAsync(key, "APPEND extended the value");
            Assert.Equal("v1-tail", await _fx.Cache.GetAsync<string>(key));
        }
        finally
        {
            RedisCli.Standalone("DEL", key);
        }
    }

    private async Task IncrAsync()
    {
        var key = await TrackedKeyAsync("incr", "5");
        try
        {
            RedisCli.Standalone("INCR", key);
            await AssertEvictedAsync(key, "INCR changed the numeric value");
            Assert.Equal("6", await _fx.Cache.GetAsync<string>(key));
        }
        finally
        {
            RedisCli.Standalone("DEL", key);
        }
    }

    private async Task EvalAsync()
    {
        var key = await TrackedKeyAsync("eval");
        try
        {
            RedisCli.Standalone("EVAL", "redis.call('SET', KEYS[1], ARGV[1]) return 1", "1", key, "from-lua");
            await AssertEvictedAsync(key, "a Lua script SET the key");
            Assert.Equal("from-lua", await _fx.Cache.GetAsync<string>(key));
        }
        finally
        {
            RedisCli.Standalone("DEL", key);
        }
    }

    private async Task FcallAsync()
    {
        if (!CompatSupport.RequireServer(new Version(7, 0), "FUNCTION LOAD / FCALL", _out)) return;

        var library = $"rnccompat{Guid.NewGuid():N}";
        var function = $"{library}_set";
        var key = await TrackedKeyAsync("fcall");
        try
        {
            RedisCli.Standalone(
                "FUNCTION",
                "LOAD",
                $"#!lua name={library}\nredis.register_function('{function}', function(keys, args) return redis.call('SET', keys[1], args[1]) end)");

            RedisCli.Standalone("FCALL", function, "1", key, "from-function");
            await AssertEvictedAsync(key, "a Redis function SET the key");
            Assert.Equal("from-function", await _fx.Cache.GetAsync<string>(key));
        }
        finally
        {
            RedisCli.Standalone("DEL", key);
            try { RedisCli.Standalone("FUNCTION", "DELETE", library); }
            catch (InvalidOperationException) { /* never loaded; nothing to restore */ }
        }
    }

    private async Task RestoreAsync()
    {
        var donor = TestHelpers.Key("restore-donor");
        var key = await TrackedKeyAsync("restore");
        // DUMP payloads are binary and would not survive a shell round-trip, so this one mutation is made
        // through a plain foreign multiplexer instead of redis-cli. It is still a foreign client: its own
        // connection, its own client id, untracked by the server.
        await using var foreign = await ForeignClient.ConnectAsync(StandaloneCacheFixture.ConnectionString);
        try
        {
            await foreign.Db.StringSetAsync(donor, "donated");
            var payload = await foreign.Db.KeyDumpAsync(donor);
            Assert.NotNull(payload);

            await foreign.Db.ExecuteAsync("RESTORE", key, 0, payload!, "REPLACE");
            await AssertEvictedAsync(key, "RESTORE ... REPLACE overwrote the key");
            Assert.Equal("donated", await _fx.Cache.GetAsync<string>(key));
        }
        finally
        {
            RedisCli.Standalone("DEL", donor, key);
        }
    }

    private async Task SetSameValueAsync()
    {
        var key = await TrackedKeyAsync("set-same");
        try
        {
            // Writing the value the key already holds still invalidates: Redis signals the key as modified
            // without comparing the old value to the new one. Verified empirically - a cache cannot rely on
            // "idempotent" writes being free for its readers.
            RedisCli.Standalone("SET", key, "v1");
            await AssertEvictedAsync(
                key,
                "SET with the identical value did not invalidate; this server compares values before signalling");
            Assert.Equal("v1", await _fx.Cache.GetAsync<string>(key));
        }
        finally
        {
            RedisCli.Standalone("DEL", key);
        }
    }

    private async Task FlushAllAsync()
    {
        var key = await TrackedKeyAsync("flushall");
        var flushesBefore = _fx.Cache.Statistics.Flushes;

        RedisCli.Standalone("FLUSHALL");

        await AssertEvictedAsync(key, "FLUSHALL wiped the server");
        var flushed = await Poll.UntilAsync(
            () => _fx.Cache.Statistics.Flushes > flushesBefore,
            EvictionDeadline);
        Assert.True(flushed, "FLUSHALL did not produce a whole-cache flush (null invalidation).");
    }

    /// <summary>
    /// <b>SWAPDB does not invalidate anything</b> (verified against redis 7.4: no per-key invalidation and no
    /// null/flush message reached the cache within 3 s of <c>SWAPDB 0 1</c>). Swapping the databases replaces
    /// the contents of every tracked key name at once, but Redis's swapdb path does not touch the tracking
    /// table the way FLUSHALL/FLUSHDB do, so a near cache goes on serving values from the database that was
    /// swapped away. This is a genuine staleness hazard in the server, not in RedisNearCache: the library is
    /// never told, and only <see cref="RedisNearCacheOptions.L1MaxAge"/> (5 minutes by default) eventually
    /// corrects it. Applications that use SWAPDB must flush their near caches themselves.
    ///
    /// <para>
    /// The test asserts what the server actually does and reports which branch it took, so a future server
    /// that does start invalidating on SWAPDB passes too and says so in the log rather than failing.
    /// </para>
    /// </summary>
    private async Task SwapDbAsync()
    {
        var key = await TrackedKeyAsync("swapdb");
        var flushesBefore = _fx.Cache.Statistics.Flushes;
        var invalidationsBefore = _fx.Cache.Statistics.Invalidations;
        var swapped = false;
        try
        {
            // The reasoning below depends on db 1 being empty, so that after the swap the key does not exist
            // on the server at all. Every test in this project that touches db 1 restores it; check anyway,
            // because a leftover would turn a real failure into a confusing one.
            Assert.Equal("0", RedisCli.Standalone("-n", "1", "DBSIZE"));

            RedisCli.Standalone("SWAPDB", "0", "1");
            swapped = true;

            var evicted = await Poll.UntilAsync(() => !_fx.Cache.TryGetLocal<string>(key, out _), EvictionDeadline);
            var flushed = _fx.Cache.Statistics.Flushes > flushesBefore;
            var invalidated = _fx.Cache.Statistics.Invalidations > invalidationsBefore;
            _out.WriteLine($"SWAPDB 0 1: evicted={evicted} flush={flushed} perKeyInvalidation={invalidated}");

            if (evicted)
            {
                // A server that does signal SWAPDB. Nothing more to check: the cache reacted.
                Assert.True(flushed || invalidated, "the entry left L1 but no invalidation or flush was counted.");
                return;
            }

            // The documented hazard, asserted so it cannot change silently: the key is gone from the server
            // yet L1 still serves the pre-swap value.
            Assert.Equal("0", RedisCli.Standalone("EXISTS", key));
            Assert.True(_fx.Cache.TryGetLocal<string>(key, out var stale));
            Assert.Equal("v1", stale);
            Assert.Equal("v1", await _fx.Cache.GetAsync<string>(key));
            _out.WriteLine(
                "SWAPDB produced no invalidation: L1 served 'v1' for a key the server no longer holds. " +
                "Applications using SWAPDB must invalidate their near caches themselves.");
        }
        finally
        {
            // Swap back, then make sure db 1 is left as this test found it (empty) and no stale entry is
            // handed on to the next test in this class.
            if (swapped) RedisCli.Standalone("SWAPDB", "0", "1");
            _fx.Cache.EvictLocal(key);
            RedisCli.Standalone("DEL", key);
            RedisCli.Standalone("-n", "1", "DEL", key);
        }
    }

    /// <summary>Seeds a key and reads it through the cache until the value is genuinely held in L1 and tracked.</summary>
    private async Task<string> TrackedKeyAsync(string suffix, string value = "v1")
    {
        var key = TestHelpers.Key($"mut-{suffix}");
        await _fx.Cache.SetAsync(key, value);
        Assert.True(
            await TestHelpers.ReadUntilCachedAsync(_fx.Cache, key, value),
            $"the key {key} was not cached locally before the mutation, so the test would prove nothing.");
        return key;
    }

    private async Task AssertEvictedAsync(string key, string because)
    {
        var evicted = await Poll.UntilAsync(() => !_fx.Cache.TryGetLocal<string>(key, out _), EvictionDeadline);
        Assert.True(evicted, $"the L1 copy of {key} survived {EvictionDeadline.TotalSeconds:0} s after {because}.");
    }
}
