using RedisNearCache.Tests.EdgeCases;
using StackExchange.Redis;
using Xunit;

namespace RedisNearCache.Tests.Namespacing;

/// <summary>
/// <see cref="RedisNearCacheOptions.KeyNamespace"/> against the real standalone container in the default
/// <see cref="TrackingMode.Redirect"/> mode: the value really is at <c>namespace + key</c> in Redis, the caller
/// really does use its own key everywhere, and the server's invalidations - which name the FULL key - really do
/// reach the right L1 entry, while an invalidation for the bare key or another namespace's key does not.
/// </summary>
/// <remarks>
/// Each test makes its own namespace (a GUID), owns its own provider, and deletes the full keys it created.
/// Statistics are compared as deltas on a cache the test owns.
/// </remarks>
public class KeyNamespaceRedirectTests
{
    private static string NewNamespace() => $"ns-{Guid.NewGuid():N}:";

    [Fact]
    public async Task TheValueLandsAtTheNamespacedKeyAndNotAtTheBareOne()
    {
        var ns = NewNamespace();
        var key = TestHelpers.Key("ns-write");
        var full = ns + key;
        var handle = await EdgeCaseSupport.BuildAsync(StandaloneCacheFixture.ConnectionString, o => o.KeyNamespace = ns);
        try
        {
            await handle.Cache.SetAsync(key, "v1");

            Assert.Equal("v1", RedisCli.Standalone("GET", full));
            Assert.Equal(string.Empty, RedisCli.Standalone("GET", key));
            Assert.Equal("1", RedisCli.Standalone("EXISTS", full));
            Assert.Equal("0", RedisCli.Standalone("EXISTS", key));

            // And the caller reads it back with its own key.
            Assert.True(await TestHelpers.ReadUntilCachedAsync(handle.Cache, key, "v1"),
                "the caller's own key did not read back the value it had just written.");
        }
        finally
        {
            await handle.DisposeAsync();
            RedisCli.Standalone("DEL", full, key);
        }
    }

    [Fact]
    public async Task AForeignWriteToTheNamespacedKeyInvalidates()
    {
        var ns = NewNamespace();
        var key = TestHelpers.Key("ns-foreign");
        var full = ns + key;
        var handle = await EdgeCaseSupport.BuildAsync(StandaloneCacheFixture.ConnectionString, o => o.KeyNamespace = ns);
        try
        {
            var cache = handle.Cache;
            RedisCli.Standalone("SET", full, "v1");
            Assert.True(await TestHelpers.ReadUntilCachedAsync(cache, key, "v1"), "the key was never cached.");

            RedisCli.Standalone("SET", full, "v2");

            var evicted = await Poll.UntilAsync(() => !cache.TryGetLocal<string>(key, out _), TimeSpan.FromSeconds(5));
            Assert.True(evicted, "a foreign write to the full key did not evict the local copy.");
            Assert.Equal("v2", await cache.GetAsync<string>(key));
        }
        finally
        {
            await handle.DisposeAsync();
            RedisCli.Standalone("DEL", full);
        }
    }

    /// <summary>
    /// A write to the BARE key is somebody else's key: it must not touch this cache. The positive control at the end
    /// is what makes that mean something - it shows invalidations really were flowing to this cache throughout the
    /// window in which the bare-key write produced none.
    /// </summary>
    [Fact]
    public async Task AForeignWriteToTheBareKeyDoesNotInvalidate()
    {
        var ns = NewNamespace();
        var guarded = TestHelpers.Key("ns-bare-guarded");
        var control = TestHelpers.Key("ns-bare-control");
        var guardedFull = ns + guarded;
        var controlFull = ns + control;
        var handle = await EdgeCaseSupport.BuildAsync(StandaloneCacheFixture.ConnectionString, o => o.KeyNamespace = ns);
        try
        {
            var cache = handle.Cache;
            RedisCli.Standalone("SET", guardedFull, "v1");
            RedisCli.Standalone("SET", controlFull, "c1");
            Assert.True(await TestHelpers.ReadUntilCachedAsync(cache, guarded, "v1"), "the guarded key was never cached.");
            Assert.True(await TestHelpers.ReadUntilCachedAsync(cache, control, "c1"), "the control key was never cached.");

            var invalidationsBefore = cache.Statistics.Invalidations;

            // The bare key: a real key in Redis, simply not this cache's.
            RedisCli.Standalone("SET", guarded, "bare");

            var evicted = await Poll.UntilAsync(() => !cache.TryGetLocal<string>(guarded, out _), TimeSpan.FromSeconds(2));
            Assert.False(evicted, "a write to the BARE key evicted the namespaced entry.");
            Assert.Equal(invalidationsBefore, cache.Statistics.Invalidations);

            // Positive control: the same kind of write, to a key this cache DOES hold, is seen at once.
            RedisCli.Standalone("SET", controlFull, "c2");
            Assert.True(await Poll.UntilAsync(() => !cache.TryGetLocal<string>(control, out _), TimeSpan.FromSeconds(5)),
                "the control key was not evicted, so the assertion above proves nothing about tracking being live.");
            Assert.True(cache.Statistics.Invalidations > invalidationsBefore);

            // And the guarded key is still there, unchanged, after an invalidation round trip has demonstrably run.
            Assert.True(cache.TryGetLocal<string>(guarded, out var stillLocal),
                "the guarded entry was dropped somewhere along the way.");
            Assert.Equal("v1", stillLocal);
        }
        finally
        {
            await handle.DisposeAsync();
            RedisCli.Standalone("DEL", guardedFull, controlFull, guarded);
        }
    }

    /// <summary>The same caller key under a DIFFERENT namespace is a different Redis key and invalidates nothing here.</summary>
    [Fact]
    public async Task AForeignWriteUnderAnotherNamespaceDoesNotInvalidate()
    {
        var ns = NewNamespace();
        var other = NewNamespace();
        var key = TestHelpers.Key("ns-other");
        var control = TestHelpers.Key("ns-other-control");
        var full = ns + key;
        var otherFull = other + key;
        var controlFull = ns + control;
        var handle = await EdgeCaseSupport.BuildAsync(StandaloneCacheFixture.ConnectionString, o => o.KeyNamespace = ns);
        try
        {
            var cache = handle.Cache;
            RedisCli.Standalone("SET", full, "v1");
            RedisCli.Standalone("SET", controlFull, "c1");
            Assert.True(await TestHelpers.ReadUntilCachedAsync(cache, key, "v1"));
            Assert.True(await TestHelpers.ReadUntilCachedAsync(cache, control, "c1"));
            var invalidationsBefore = cache.Statistics.Invalidations;

            RedisCli.Standalone("SET", otherFull, "somebody-elses");

            Assert.False(await Poll.UntilAsync(() => !cache.TryGetLocal<string>(key, out _), TimeSpan.FromSeconds(2)),
                "a write under another namespace evicted this cache's entry.");
            Assert.Equal(invalidationsBefore, cache.Statistics.Invalidations);

            RedisCli.Standalone("SET", controlFull, "c2");
            Assert.True(await Poll.UntilAsync(() => !cache.TryGetLocal<string>(control, out _), TimeSpan.FromSeconds(5)),
                "the control key was not evicted, so the assertion above proves nothing.");
        }
        finally
        {
            await handle.DisposeAsync();
            RedisCli.Standalone("DEL", full, otherFull, controlFull);
        }
    }

    [Fact]
    public async Task RemoveAsyncDeletesTheNamespacedKey()
    {
        var ns = NewNamespace();
        var key = TestHelpers.Key("ns-remove");
        var full = ns + key;
        var handle = await EdgeCaseSupport.BuildAsync(StandaloneCacheFixture.ConnectionString, o => o.KeyNamespace = ns);
        try
        {
            // The bare key exists too and must survive: a delete of the wrong key would show up here.
            RedisCli.Standalone("SET", key, "bare");
            await handle.Cache.SetAsync(key, "v1");
            Assert.Equal("1", RedisCli.Standalone("EXISTS", full));

            Assert.True(await handle.Cache.RemoveAsync(key));

            Assert.Equal("0", RedisCli.Standalone("EXISTS", full));
            Assert.Equal("bare", RedisCli.Standalone("GET", key));
            Assert.Null(await handle.Cache.GetAsync<string>(key));
        }
        finally
        {
            await handle.DisposeAsync();
            RedisCli.Standalone("DEL", full, key);
        }
    }

    [Fact]
    public async Task GetManyAnswersWithTheCallersKeys()
    {
        var ns = NewNamespace();
        var present = TestHelpers.Key("ns-many-present");
        var alsoPresent = TestHelpers.Key("ns-many-also");
        var missing = TestHelpers.Key("ns-many-missing");
        var handle = await EdgeCaseSupport.BuildAsync(StandaloneCacheFixture.ConnectionString, o => o.KeyNamespace = ns);
        try
        {
            var cache = handle.Cache;
            RedisCli.Standalone("SET", ns + present, "v1");
            RedisCli.Standalone("SET", ns + alsoPresent, "v2");
            // A decoy at the BARE key of the missing one: a cache that dropped the namespace would find it.
            RedisCli.Standalone("SET", missing, "decoy");

            var result = await cache.GetManyAsync<string>([present, alsoPresent, missing]);

            Assert.Equal(3, result.Count);
            Assert.Equal("v1", result[present]);
            Assert.Equal("v2", result[alsoPresent]);
            Assert.Null(result[missing]);
            Assert.True(result.ContainsKey(missing), "a key that does not exist must still be present, with a null value.");

            // The caller's keys, not the full ones.
            Assert.DoesNotContain(result.Keys, k => k.StartsWith(ns, StringComparison.Ordinal));

            var bytes = await cache.GetManyBytesAsync([present, missing]);
            Assert.Equal("v1"u8.ToArray(), bytes[present]);
            Assert.Null(bytes[missing]);
        }
        finally
        {
            await handle.DisposeAsync();
            RedisCli.Standalone("DEL", ns + present, ns + alsoPresent, missing);
        }
    }

    [Fact]
    public async Task AConditionalWriteRespectsTheNamespacedKey()
    {
        var ns = NewNamespace();
        var key = TestHelpers.Key("ns-conditional");
        var full = ns + key;
        var handle = await EdgeCaseSupport.BuildAsync(StandaloneCacheFixture.ConnectionString, o => o.KeyNamespace = ns);
        try
        {
            var cache = handle.Cache;
            // The BARE key already exists. A cache that wrote without the namespace would see "exists" and refuse.
            RedisCli.Standalone("SET", key, "bare");

            Assert.True(await cache.SetAsync(key, "v1", When.NotExists),
                "NX on a key that only exists WITHOUT the namespace must succeed.");
            Assert.Equal("v1", RedisCli.Standalone("GET", full));
            Assert.Equal("bare", RedisCli.Standalone("GET", key));

            Assert.False(await cache.SetAsync(key, "v2", When.NotExists), "NX on the now-existing namespaced key must fail.");
            Assert.Equal("v1", RedisCli.Standalone("GET", full));

            Assert.True(await cache.SetAsync(key, "v3", When.Exists), "XX on the existing namespaced key must succeed.");
            Assert.Equal("v3", RedisCli.Standalone("GET", full));
        }
        finally
        {
            await handle.DisposeAsync();
            RedisCli.Standalone("DEL", full, key);
        }
    }

    /// <summary>
    /// Two caches over ONE Redis, with different namespaces and the SAME caller key: the values do not meet, and a
    /// write through one does not evict the other's copy (its key is a different key). The positive control is the
    /// second cache being evicted by a write to its OWN full key right afterwards.
    /// </summary>
    [Fact]
    public async Task TwoNamespacesOverOneRedisDoNotCollide()
    {
        var nsA = NewNamespace();
        var nsB = NewNamespace();
        var key = TestHelpers.Key("ns-two");
        var a = await EdgeCaseSupport.BuildAsync(StandaloneCacheFixture.ConnectionString, o => o.KeyNamespace = nsA);
        EdgeCaseProvider? b = null;
        try
        {
            b = await EdgeCaseSupport.BuildAsync(StandaloneCacheFixture.ConnectionString, o => o.KeyNamespace = nsB);

            await a.Cache.SetAsync(key, "from-a");
            await b.Cache.SetAsync(key, "from-b");

            Assert.Equal("from-a", RedisCli.Standalone("GET", nsA + key));
            Assert.Equal("from-b", RedisCli.Standalone("GET", nsB + key));
            Assert.True(await TestHelpers.ReadUntilCachedAsync(a.Cache, key, "from-a"), "a's copy was never cached.");
            Assert.True(await TestHelpers.ReadUntilCachedAsync(b.Cache, key, "from-b"), "b's copy was never cached.");

            var bInvalidationsBefore = b.Cache.Statistics.Invalidations;

            await a.Cache.SetAsync(key, "from-a-again");

            Assert.False(await Poll.UntilAsync(() => !b.Cache.TryGetLocal<string>(key, out _), TimeSpan.FromSeconds(2)),
                "a write through the cache in namespace A evicted the entry of the cache in namespace B.");
            Assert.Equal(bInvalidationsBefore, b.Cache.Statistics.Invalidations);
            Assert.Equal("from-b", await b.Cache.GetAsync<string>(key));

            // Positive control: B's own key, written by a foreign client, does evict it.
            RedisCli.Standalone("SET", nsB + key, "from-somebody-else");
            Assert.True(await Poll.UntilAsync(() => !b.Cache.TryGetLocal<string>(key, out _), TimeSpan.FromSeconds(5)),
                "B was not evicted by a write to its own key, so the assertion above proves nothing.");
        }
        finally
        {
            await a.DisposeAsync();
            if (b is not null) await b.DisposeAsync();
            RedisCli.Standalone("DEL", nsA + key, nsB + key);
        }
    }

    /// <summary>
    /// The regression case, stated once explicitly: a provider built exactly as every existing application builds
    /// one puts the value at the BARE key, as it always did.
    /// </summary>
    [Fact]
    public async Task WithoutANamespaceTheValueStillLandsAtTheBareKey()
    {
        var key = TestHelpers.Key("no-ns");
        var handle = await EdgeCaseSupport.BuildAsync(StandaloneCacheFixture.ConnectionString);
        try
        {
            await handle.Cache.SetAsync(key, "v1");

            Assert.Equal("v1", RedisCli.Standalone("GET", key));
            Assert.True(await TestHelpers.ReadUntilCachedAsync(handle.Cache, key, "v1"));

            RedisCli.Standalone("SET", key, "v2");
            Assert.True(await Poll.UntilAsync(() => !handle.Cache.TryGetLocal<string>(key, out _), TimeSpan.FromSeconds(5)),
                "the un-namespaced cache was not invalidated by a foreign write to its key.");
            Assert.Equal("v2", await handle.Cache.GetAsync<string>(key));
        }
        finally
        {
            await handle.DisposeAsync();
            RedisCli.Standalone("DEL", key);
        }
    }

    /// <summary>
    /// The internal hook the timing tests use sees the FULL key - i.e. the translation really has happened by the
    /// time the read path runs, rather than being applied somewhere further down.
    /// </summary>
    [Fact]
    public async Task TheAfterReadHookSeesTheFullKey()
    {
        var ns = NewNamespace();
        var key = TestHelpers.Key("ns-hook");
        var full = ns + key;
        var seen = new List<string>();
        var handle = await EdgeCaseSupport.BuildAsync(StandaloneCacheFixture.ConnectionString, o =>
        {
            o.KeyNamespace = ns;
            o.TestHooks.AfterRedisReadBeforeStore = k => { lock (seen) seen.Add(k); return Task.CompletedTask; };
        });
        try
        {
            RedisCli.Standalone("SET", full, "v1");

            Assert.Equal("v1", await handle.Cache.GetAsync<string>(key));

            lock (seen) Assert.Equal(new[] { full }, seen.ToArray());
        }
        finally
        {
            await handle.DisposeAsync();
            RedisCli.Standalone("DEL", full);
        }
    }
}

/// <summary>
/// <see cref="RedisNearCacheOptions.KeyPrefixes"/> relative to <see cref="RedisNearCacheOptions.KeyNamespace"/>
/// in <see cref="TrackingMode.Redirect"/>, against real Redis: the caller's <c>user:1</c> (Redis
/// <c>ns:user:1</c>) is cached and tracked with a plain <c>GET</c>, and its <c>order:1</c> is answered by a
/// <c>MULTI</c> / <c>CLIENT CACHING NO</c> / <c>GET</c> / <c>EXEC</c>, never cached, and never invalidated - the
/// same proof <see cref="OptOutUntrackedReadsTests"/> uses for a cache without a namespace.
/// </summary>
public class KeyNamespacePrefixOptInTests
{
    [Fact]
    public async Task OnlyKeysInsideTheNamespacedPrefixAreCachedAndTracked()
    {
        var ns = $"ns-{Guid.NewGuid():N}:";
        var inside = "user:" + TestHelpers.Key("in");
        var outside = "order:" + TestHelpers.Key("out");
        var insideFull = ns + inside;
        var outsideFull = ns + outside;
        var handle = await EdgeCaseSupport.BuildAsync(StandaloneCacheFixture.ConnectionString, o =>
        {
            o.KeyNamespace = ns;
            o.KeyPrefixes.Add("user:");
        });
        try
        {
            var cache = handle.Cache;
            var server = handle.Multiplexer.GetServer(handle.Multiplexer.GetEndPoints()[0]);
            RedisCli.Standalone("SET", insideFull, "v1");
            RedisCli.Standalone("SET", outsideFull, "v1");

            var multiBefore = TestHelpers.CommandCalls(server, "multi");
            var execBefore = TestHelpers.CommandCalls(server, "exec");

            // Outside the prefix: answered, never cached, and sent as an untracked transaction.
            Assert.Equal("v1", await cache.GetAsync<string>(outside));
            Assert.False(cache.TryGetLocal<string>(outside, out _),
                "a key outside the namespaced KeyPrefixes must never be cached in L1.");
            Assert.Equal(multiBefore + 1, TestHelpers.CommandCalls(server, "multi"));
            Assert.Equal(execBefore + 1, TestHelpers.CommandCalls(server, "exec"));

            var invalidationsBefore = cache.Statistics.Invalidations;
            RedisCli.Standalone("SET", outsideFull, "v2");
            Assert.False(
                await Poll.UntilAsync(() => cache.Statistics.Invalidations > invalidationsBefore, TimeSpan.FromSeconds(1)),
                "a key outside the namespaced KeyPrefixes must not be tracked, but a write to it produced an invalidation.");
            Assert.Equal("v2", await cache.GetAsync<string>(outside));

            // Inside the prefix: a plain GET (no transaction), cached, and invalidated by a foreign write.
            var multiBeforeInside = TestHelpers.CommandCalls(server, "multi");
            Assert.True(await TestHelpers.ReadUntilCachedAsync(cache, inside, "v1"),
                "the key inside the namespaced KeyPrefixes was not cached.");
            Assert.Equal(multiBeforeInside, TestHelpers.CommandCalls(server, "multi"));

            RedisCli.Standalone("SET", insideFull, "v2");
            Assert.True(await Poll.UntilAsync(() => !cache.TryGetLocal<string>(inside, out _), TimeSpan.FromSeconds(5)),
                "a write to the tracked, prefixed key did not evict it.");
            Assert.True(cache.Statistics.Invalidations > invalidationsBefore);
        }
        finally
        {
            await handle.DisposeAsync();
            RedisCli.Standalone("DEL", insideFull, outsideFull);
        }
    }
}
