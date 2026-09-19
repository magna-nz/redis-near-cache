using StackExchange.Redis;
using Xunit;

namespace RedisNearCache.Tests.EdgeCases;

/// <summary>
/// The conditional-write overloads (<c>SetAsync</c>/<c>SetBytesAsync</c> taking a <see cref="When"/>) against a
/// real standalone Redis: NX/XX outcomes, <c>keepTtl</c>, expiry, the argument guard, and that L1 stays coherent
/// whatever the outcome of the write.
/// </summary>
public class ConditionalWriteTests : IClassFixture<StandaloneCacheFixture>
{
    private readonly StandaloneCacheFixture _fx;

    public ConditionalWriteTests(StandaloneCacheFixture fx) => _fx = fx;

    [Fact]
    public async Task NotExistsOnAnAbsentKeySucceeds()
    {
        var key = TestHelpers.Key("cond-notexists-absent");
        try
        {
            Assert.True(await _fx.Cache.SetAsync(key, "v1", When.NotExists));
            Assert.Equal("v1", RedisCli.Standalone("GET", key));
        }
        finally
        {
            RedisCli.Standalone("DEL", key);
        }
    }

    [Fact]
    public async Task NotExistsOnAnExistingKeyFailsAndLeavesTheOldValue()
    {
        var key = TestHelpers.Key("cond-notexists-existing");
        try
        {
            RedisCli.Standalone("SET", key, "v1");

            Assert.False(await _fx.Cache.SetAsync(key, "v2", When.NotExists));

            Assert.Equal("v1", RedisCli.Standalone("GET", key));
        }
        finally
        {
            RedisCli.Standalone("DEL", key);
        }
    }

    [Fact]
    public async Task ExistsOnAnAbsentKeyFailsAndTheKeyStaysAbsent()
    {
        var key = TestHelpers.Key("cond-exists-absent");
        try
        {
            Assert.False(await _fx.Cache.SetAsync(key, "v1", When.Exists));

            Assert.Equal("0", RedisCli.Standalone("EXISTS", key));
        }
        finally
        {
            RedisCli.Standalone("DEL", key);
        }
    }

    [Fact]
    public async Task ExistsOnAnExistingKeySucceedsAndReplacesTheValue()
    {
        var key = TestHelpers.Key("cond-exists-existing");
        try
        {
            RedisCli.Standalone("SET", key, "v1");

            Assert.True(await _fx.Cache.SetAsync(key, "v2", When.Exists));

            Assert.Equal("v2", RedisCli.Standalone("GET", key));
        }
        finally
        {
            RedisCli.Standalone("DEL", key);
        }
    }

    [Fact]
    public async Task KeepTtlKeepsTheExistingTtlWhereasTheOldOverloadClearsIt()
    {
        var key = TestHelpers.Key("cond-keepttl");
        try
        {
            RedisCli.Standalone("SET", key, "v1", "PX", "60000");

            Assert.True(await _fx.Cache.SetAsync(key, "v2", When.Always, keepTtl: true));
            var pttlAfterKeepTtl = long.Parse(RedisCli.Standalone("PTTL", key));
            Assert.True(pttlAfterKeepTtl > 0, "keepTtl must preserve the key's existing TTL.");
            Assert.Equal("v2", RedisCli.Standalone("GET", key));

            // Contrast: the OLD overload on the same key clears the TTL.
            await _fx.Cache.SetAsync(key, "v3");
            Assert.Equal("-1", RedisCli.Standalone("PTTL", key));
        }
        finally
        {
            RedisCli.Standalone("DEL", key);
        }
    }

    [Fact]
    public async Task KeepTtlWithExistsAlsoKeepsTheTtl()
    {
        var key = TestHelpers.Key("cond-keepttl-exists");
        try
        {
            RedisCli.Standalone("SET", key, "v1", "PX", "60000");

            Assert.True(await _fx.Cache.SetAsync(key, "v2", When.Exists, keepTtl: true));

            var pttl = long.Parse(RedisCli.Standalone("PTTL", key));
            Assert.True(pttl > 0, "When.Exists combined with keepTtl must still preserve the existing TTL.");
            Assert.Equal("v2", RedisCli.Standalone("GET", key));
        }
        finally
        {
            RedisCli.Standalone("DEL", key);
        }
    }

    [Fact]
    public async Task ExpiryWithAConditionSetsTheTtl()
    {
        var key = TestHelpers.Key("cond-expiry");
        try
        {
            Assert.True(await _fx.Cache.SetAsync(key, "v1", When.NotExists, TimeSpan.FromSeconds(30)));

            var pttl = long.Parse(RedisCli.Standalone("PTTL", key));
            Assert.InRange(pttl, 1, 30_000);
        }
        finally
        {
            RedisCli.Standalone("DEL", key);
        }
    }

    [Fact]
    public async Task L1RemainsCoherentAcrossSuccessfulFailedAndForeignWrites()
    {
        var key = TestHelpers.Key("cond-l1");
        try
        {
            await _fx.Cache.SetAsync(key, "v1");
            Assert.True(await TestHelpers.ReadUntilCachedAsync(_fx.Cache, key, "v1"), "key was not cached after the initial write.");

            // A conditional write that SUCCEEDS (the key exists, When.Exists) evicts L1, and the next read sees the
            // new value and caches it again.
            Assert.True(await _fx.Cache.SetAsync(key, "v2", When.Exists));
            Assert.False(_fx.Cache.TryGetLocal<string>(key, out _), "a successful conditional write must evict L1.");
            Assert.Equal("v2", await _fx.Cache.GetAsync<string>(key));
            Assert.True(_fx.Cache.TryGetLocal<string>(key, out var recached) && recached == "v2",
                "the read right after a successful conditional write must re-cache the new value.");

            // A conditional write that returns FALSE (the key exists, When.NotExists) still evicts L1, but Redis
            // keeps "v2": the next read must never surface a value Redis does not hold.
            Assert.False(await _fx.Cache.SetAsync(key, "v3", When.NotExists));
            Assert.False(_fx.Cache.TryGetLocal<string>(key, out _), "a conditional write that did not happen must still evict L1.");
            Assert.Equal("v2", await _fx.Cache.GetAsync<string>(key));
            Assert.Equal("v2", RedisCli.Standalone("GET", key));

            // A foreign write still invalidates as usual after a conditional write.
            RedisCli.Standalone("SET", key, "v4");
            var evicted = await Poll.UntilAsync(() => !_fx.Cache.TryGetLocal<string>(key, out _));
            Assert.True(evicted, "L1 entry was not evicted after a foreign write following a conditional write.");
            Assert.Equal("v4", await _fx.Cache.GetAsync<string>(key));
        }
        finally
        {
            RedisCli.Standalone("DEL", key);
        }
    }

    [Fact]
    public async Task SetBytesVariantsOfNotExistsExistsAndKeepTtl()
    {
        var key = TestHelpers.Key("cond-bytes");
        try
        {
            Assert.True(await _fx.Cache.SetBytesAsync(key, new byte[] { 1 }, When.NotExists));
            Assert.False(await _fx.Cache.SetBytesAsync(key, new byte[] { 2 }, When.NotExists));
            Assert.Equal(new byte[] { 1 }, await _fx.Cache.GetBytesAsync(key));

            Assert.True(await _fx.Cache.SetBytesAsync(key, new byte[] { 3 }, When.Exists));
            Assert.Equal(new byte[] { 3 }, await _fx.Cache.GetBytesAsync(key));

            RedisCli.Standalone("PEXPIRE", key, "60000");
            Assert.True(await _fx.Cache.SetBytesAsync(key, new byte[] { 4 }, When.Always, keepTtl: true));
            var pttl = long.Parse(RedisCli.Standalone("PTTL", key));
            Assert.True(pttl > 0, "keepTtl must preserve the TTL for SetBytesAsync too.");
        }
        finally
        {
            RedisCli.Standalone("DEL", key);
        }
    }

    [Fact]
    public async Task KeepTtlWithExpiryThrowsArgumentExceptionAgainstTheRealCache()
    {
        var key = TestHelpers.Key("cond-argex");
        try
        {
            var ex = await Assert.ThrowsAsync<ArgumentException>(
                () => _fx.Cache.SetAsync(key, "v1", When.Always, TimeSpan.FromSeconds(1), keepTtl: true).AsTask());

            Assert.Equal("keepTtl", ex.ParamName);
            Assert.Equal("0", RedisCli.Standalone("EXISTS", key));
        }
        finally
        {
            RedisCli.Standalone("DEL", key);
        }
    }
}
