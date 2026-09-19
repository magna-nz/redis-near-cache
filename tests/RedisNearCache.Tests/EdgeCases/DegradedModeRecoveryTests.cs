using Xunit;
using Xunit.Abstractions;

namespace RedisNearCache.Tests.EdgeCases;

/// <summary>
/// The recovery path added by the "code review (high) fixes" commit: when the initial arming pass fails on
/// every master, <see cref="IRedisNearCache.Ready"/> faults, the cache degrades to a pass-through (reads go
/// to Redis, nothing is stored in L1, no exception per call), and a background loop keeps retrying every
/// 5 s. Once arming becomes possible the loop arms with <c>ArmReason.Recovered</c>, the facade flushes L1,
/// clears the degraded flag and - by the intent of that commit - starts caching again.
///
/// "Arming becomes possible" is simulated with a Redis ACL user that may run everything except
/// <c>CLIENT TRACKING</c>, which makes the arm fail with NOPERM while leaving GET, SUBSCRIBE, CLIENT LIST and
/// CLIENT TRACKINGINFO working: the connection is perfectly healthy and only the tracking call fails.
/// </summary>
public class DegradedModeRecoveryTests
{
    private const string AclUser = "rnc-limited";
    private const string AclPassword = "pw";

    /// <summary>
    /// Verified against the Redis 7.4 container: <c>-client|tracking</c> is accepted and leaves
    /// <c>client|list</c>, <c>client|setname</c> and <c>client|trackinginfo</c> permitted, so only the arm fails.
    /// </summary>
    private static void DenyTracking() =>
        RedisCli.Standalone("ACL", "SETUSER", AclUser, "on", $">{AclPassword}", "~*", "&*", "+@all", "-client|tracking");

    private static void AllowTracking() => RedisCli.Standalone("ACL", "SETUSER", AclUser, "+client|tracking");

    private static void DeleteUser() => RedisCli.Standalone("ACL", "DELUSER", AclUser);

    private static Task<EdgeCaseProvider> ConnectAsLimitedUserAsync() =>
        EdgeCaseSupport.BuildAsync($"localhost:6379,user={AclUser},password={AclPassword}", awaitReady: false);

    private readonly ITestOutputHelper _out;

    public DegradedModeRecoveryTests(ITestOutputHelper output) => _out = output;

    /// <summary>
    /// Denying a single subcommand (<c>-client|tracking</c>) needs Redis 7.0: Redis 6.x accepts the rule but
    /// does not enforce it, so the "arm fails" precondition of these tests cannot be set up there.
    /// </summary>
    private bool ServerSupportsSubcommandDeny()
    {
        var major = EdgeCaseSupport.ServerMajorVersion();
        if (major >= 7) return true;
        _out.WriteLine($"skipped: server major version {major} cannot deny a single subcommand via ACL (needs 7.0+)");
        return false;
    }

    /// <summary>
    /// The half of the contract that holds today: startup faults, reads keep working through Redis, nothing is
    /// served locally, and the background loop does keep retrying and does eventually arm the node.
    /// </summary>
    [Fact]
    public async Task DegradedModeServesEveryReadFromRedisAndKeepsRetrying()
    {
        if (!ServerSupportsSubcommandDeny()) return;
        DenyTracking();
        EdgeCaseProvider? handle = null;
        var key = TestHelpers.Key("degraded-passthrough");
        try
        {
            handle = await ConnectAsLimitedUserAsync();
            var cache = handle.Cache;

            var startupFailure = await Assert.ThrowsAnyAsync<Exception>(() => cache.Ready);
            _out.WriteLine($"Ready faulted with {startupFailure.GetType().Name}: {startupFailure.Message}");

            RedisCli.Standalone("SET", key, "v1");

            var hitsBefore = cache.Statistics.Hits;
            for (var i = 0; i < 5; i++)
            {
                Assert.Equal("v1", await cache.GetAsync<string>(key));
                Assert.False(cache.TryGetLocal<string>(key, out _), $"read {i + 1} populated L1 while tracking was impossible.");
            }

            Assert.Equal(hitsBefore, cache.Statistics.Hits);
            _out.WriteLine($"degraded: {cache.Statistics}");

            // The armer must not give up: once the permission is there, the slow loop arms the node.
            AllowTracking();
            var armed = await Poll.UntilAsync(
                () => handle.Armer.RedirectTargets.Count > 0 && cache.Statistics.Rearms >= 1,
                TimeSpan.FromSeconds(20),
                TimeSpan.FromMilliseconds(100));
            Assert.True(armed, $"the background retry loop never armed the master. stats={cache.Statistics}");
            _out.WriteLine($"after grant: {cache.Statistics}, redirect targets={handle.Armer.RedirectTargets.Count}");
        }
        finally
        {
            if (handle is not null) await handle.DisposeAsync();
            RedisCli.Standalone("DEL", key);
            DeleteUser();
        }
    }

    /// <summary>
    /// Regression test for the other half: once the background loop arms the node, the cache must resume
    /// serving from L1. <c>RedisNearCache.GetAsync</c> settles the degraded flag from
    /// <see cref="IRedisNearCache.Ready"/> exactly once, via an <c>Interlocked.Exchange</c> guard
    /// (src/RedisNearCache/Caching/RedisNearCache.cs), rather than re-deriving it from <c>Ready</c> on every
    /// call — <c>Ready</c> stays faulted forever once startup fails, but the cache does not. That is what
    /// lets <c>OnArmed</c>'s <c>_degraded = false</c> stick after the recovery arm, so caching resumes.
    /// </summary>
    [Fact]
    public async Task DegradedModeRecoversWhenTrackingBecomesPossible()
    {
        if (!ServerSupportsSubcommandDeny()) return;
        DenyTracking();
        EdgeCaseProvider? handle = null;
        var key = TestHelpers.Key("degraded-recovery");
        try
        {
            handle = await ConnectAsLimitedUserAsync();
            var cache = handle.Cache;

            await Assert.ThrowsAnyAsync<Exception>(() => cache.Ready);

            RedisCli.Standalone("SET", key, "v1");
            Assert.Equal("v1", await cache.GetAsync<string>(key));
            Assert.False(cache.TryGetLocal<string>(key, out _));

            AllowTracking();

            var rearmsBefore = cache.Statistics.Rearms;
            var recovered = await Poll.UntilAsync(
                async () =>
                {
                    if (cache.Statistics.Rearms <= rearmsBefore) return false;
                    // Once the node is armed a read must populate L1 and the next read must be served from it.
                    await cache.GetAsync<string>(key);
                    var hits = cache.Statistics.Hits;
                    await cache.GetAsync<string>(key);
                    return cache.Statistics.Hits > hits;
                },
                TimeSpan.FromSeconds(20),
                TimeSpan.FromMilliseconds(250));

            Assert.True(recovered,
                "the cache never started caching again after CLIENT TRACKING was permitted. " +
                $"stats={cache.Statistics}");

            // And tracking really is armed: an external write evicts.
            Assert.True(cache.TryGetLocal<string>(key, out _), "key was not cached after recovery.");
            RedisCli.Standalone("SET", key, "v2");
            var evicted = await Poll.UntilAsync(() => !cache.TryGetLocal<string>(key, out _), TimeSpan.FromSeconds(5));
            Assert.True(evicted, "an external write did not evict after the recovery re-arm.");
            Assert.Equal("v2", await cache.GetAsync<string>(key));
        }
        finally
        {
            if (handle is not null) await handle.DisposeAsync();
            RedisCli.Standalone("DEL", key);
            DeleteUser();
        }
    }
}
