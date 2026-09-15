using System.Text.RegularExpressions;
using Microsoft.Extensions.DependencyInjection;
using RedisNearCache.Internal;
using RedisNearCache.Tracking.Broadcast;
using StackExchange.Redis;
using StackExchange.Redis.Configuration;
using RedisNearCache.Tests.Resilience;
using Xunit;

namespace RedisNearCache.Tests.Broadcast;

/// <summary>
/// Exercises the credential-rotation contract of <see cref="BroadcastTracker"/>: a
/// <see cref="ConfigurationOptions.Defaults"/> provider whose <c>User</c>/<c>Password</c> mutate live (the shape of
/// the Azure Entra token provider) is picked up on every keepalive tick. A changed credential re-authenticates the
/// live broadcast socket in place (no <see cref="ITrackingArmer.TrackingLost"/>, no <see cref="ITrackingArmer.Armed"/>,
/// no L1 flush); a credential the server rejects makes the socket count as dead, so it is reported lost and
/// reconnected with whatever the provider currently holds.
/// </summary>
/// <remarks>
/// Fixture-less, like <see cref="EmptyKeyPrefixesTests"/>: every test needs its own
/// <see cref="ConfigurationOptions.Defaults"/> provider and its own ACL users, so nothing here can share the shared
/// <see cref="BroadcastStandaloneCacheFixture"/> instance.
/// </remarks>
public class CredentialRotationTests
{
    /// <summary>
    /// The shape of the Azure Entra token provider: a <see cref="DefaultOptionsProvider"/> subclass whose
    /// <c>User</c>/<c>Password</c> overrides are backed by ordinary mutable properties, so a test (standing in for a
    /// token refresh) can rotate credentials on a live <see cref="ConfigurationOptions"/> without reconnecting or
    /// re-cloning anything.
    /// </summary>
    private sealed class RotatingCredentials : DefaultOptionsProvider
    {
        public string? CurrentUser { get; set; }
        public string? CurrentPassword { get; set; }
        public override string? User => CurrentUser;
        public override string? Password => CurrentPassword;
    }

    private readonly record struct BuiltCache(ServiceProvider Provider, IRedisNearCache Cache, BroadcastTracker Tracker, RedisNearCacheConnection Connection);

    /// <summary>How long a rotated credential may take to reach the live socket: one keepalive tick plus slack.</summary>
    private static readonly TimeSpan ReauthWindow = BroadcastTracker.KeepAliveInterval + TimeSpan.FromSeconds(10);

    private static string UniqueUser(string tag) => $"rnc-rot-{tag}-{Guid.NewGuid():N}";

    private static string UniquePrefix(string tag) => $"rnc-rot-{tag}-{Guid.NewGuid():N}:";

    private static string Key(string prefix, string suffix) => $"{prefix}{Guid.NewGuid():N}:{suffix}";

    private static void CreateAclUser(string user, string password) => ResilienceSupport.CreateAclUser(user, password);

    private static void DeleteAclUser(string user) => ResilienceSupport.DeleteAclUser(user);

    /// <summary>Tracking is alive on the current socket: a key written through the cache is cached, and a foreign write evicts it.</summary>
    private static async Task AssertForeignWriteEvictsAsync(IRedisNearCache cache, string prefix, string tag, string lostMessage)
    {
        var key = Key(prefix, tag);
        await cache.SetAsync(key, "v1");
        Assert.True(await TestHelpers.ReadUntilCachedAsync(cache, key, "v1"), $"{key} was not cached.");
        RedisCli.Standalone("SET", key, "v2");
        Assert.True(await Poll.UntilAsync(() => !cache.TryGetLocal<string>(key, out _)), lostMessage);
    }

    /// <summary>Builds a fresh cache in Broadcast mode against a <see cref="ConfigurationOptions"/> whose
    /// <see cref="ConfigurationOptions.Defaults"/> is <paramref name="provider"/>. Mirrors <see cref="EmptyKeyPrefixesTests"/>:
    /// a throwaway <see cref="ServiceProvider"/> per test, not the shared fixture.</summary>
    private static async Task<BuiltCache> BuildCacheAsync(RotatingCredentials provider, string prefix)
    {
        var services = new ServiceCollection();
        services.AddRedisNearCache(BroadcastStandaloneCacheFixture.ConnectionString, o =>
        {
            var cfg = ConfigurationOptions.Parse(BroadcastStandaloneCacheFixture.ConnectionString);
            cfg.Defaults = provider; // deliberately NOT cfg.User/cfg.Password: those would shadow the provider.
            o.Configuration = cfg;
            o.TrackingMode = TrackingMode.Broadcast;
            o.KeyPrefixes.Add(prefix);
        });

        var sp = services.BuildServiceProvider();
        var cache = sp.GetRequiredService<IRedisNearCache>();
        var tracker = (BroadcastTracker)sp.GetRequiredService<ITrackingArmer>();
        var connection = sp.GetRequiredService<RedisNearCacheConnection>();
        await cache.Ready;
        return new BuiltCache(sp, cache, tracker, connection);
    }

    /// <summary>The <c>user=</c> field out of a <see cref="ClientInfo.Raw"/> CLIENT LIST line.</summary>
    private static string? UserOf(ClientInfo info)
    {
        var m = Regex.Match(info.Raw ?? string.Empty, @"\buser=(\S+)");
        return m.Success ? m.Groups[1].Value : null;
    }

    private static ClientInfo? FindBcastClient(IServer server, string clientName) =>
        server.ClientList().FirstOrDefault(c => c.Name == clientName);

    [Fact]
    public async Task RotatedCredentialsReauthenticateTheLiveSocketInPlace()
    {
        var userA = UniqueUser("t1a");
        var userB = UniqueUser("t1b");
        var prefix = UniquePrefix("t1");
        CreateAclUser(userA, "p1");
        var provider = new RotatingCredentials { CurrentUser = userA, CurrentPassword = "p1" };

        ServiceProvider? sp = null;
        try
        {
            var built = await BuildCacheAsync(provider, prefix);
            sp = built.Provider;
            var cache = built.Cache;
            var tracker = built.Tracker;
            var connection = built.Connection;
            var endpoint = connection.Multiplexer.GetEndPoints()[0];
            var server = connection.Multiplexer.GetServer(endpoint);
            var clientName = $"{connection.ClientName}-bcast";

            var before = FindBcastClient(server, clientName);
            Assert.NotNull(before);
            Assert.Equal(userA, UserOf(before!));
            var clientIdBefore = tracker.RedirectTargets[endpoint];

            var sawTrackingLost = false;
            var sawArmed = false;
            tracker.TrackingLost += ep => { if (Equals(ep, endpoint)) sawTrackingLost = true; };
            tracker.Armed += e => { if (Equals(e.EndPoint, endpoint)) sawArmed = true; };

            var flushesBefore = cache.Statistics.Flushes;
            var rearmsBefore = cache.Statistics.Rearms;

            CreateAclUser(userB, "p2");
            try
            {
                provider.CurrentUser = userB;
                provider.CurrentPassword = "p2";

                var reauthed = await Poll.UntilAsync(() => tracker.Reauthentications >= 1, ReauthWindow);
                Assert.True(reauthed, $"expected at least one reauthentication within {ReauthWindow}; saw {tracker.Reauthentications}.");
                Assert.Equal(1, tracker.Reauthentications);

                var sawRotatedUser = await Poll.UntilAsync(() =>
                {
                    var current = FindBcastClient(server, clientName);
                    return current is not null && string.Equals(UserOf(current), userB, StringComparison.Ordinal);
                }, TimeSpan.FromSeconds(5));
                Assert.True(sawRotatedUser, "the -bcast client did not show the rotated user in CLIENT LIST.");

                var after = FindBcastClient(server, clientName);
                Assert.NotNull(after);
                Assert.Equal(clientIdBefore, after!.Id);
                Assert.Equal(clientIdBefore, tracker.RedirectTargets[endpoint]);

                Assert.False(sawTrackingLost, "an in-place AUTH must not raise TrackingLost.");
                Assert.False(sawArmed, "an in-place AUTH must not raise Armed.");
                Assert.Equal(flushesBefore, cache.Statistics.Flushes);
                Assert.Equal(rearmsBefore, cache.Statistics.Rearms);

                // An unchanged credential must not be re-sent: two more keepalive ticks, still exactly one AUTH.
                await Task.Delay(BroadcastTracker.KeepAliveInterval * 2 + TimeSpan.FromSeconds(1));
                Assert.Equal(1, tracker.Reauthentications);
                Assert.False(sawTrackingLost, "the socket must stay armed while the credential is unchanged.");

                var key = Key(prefix, "after-reauth");
                await cache.SetAsync(key, "v1");
                Assert.True(await TestHelpers.ReadUntilCachedAsync(cache, key, "v1"));

                RedisCli.Standalone("SET", key, "v2");
                var evicted = await Poll.UntilAsync(() => !cache.TryGetLocal<string>(key, out _));
                Assert.True(evicted, "tracking did not survive the in-place AUTH: a foreign write after rotation was not evicted.");
            }
            finally
            {
                DeleteAclUser(userB);
            }
        }
        finally
        {
            if (sp is not null) await sp.DisposeAsync();
            DeleteAclUser(userA);
        }
    }

    [Fact]
    public async Task ReconnectAfterRotationUsesTheCurrentCredentials()
    {
        var userA = UniqueUser("t2a");
        var userB = UniqueUser("t2b");
        var prefix = UniquePrefix("t2");
        CreateAclUser(userA, "p1");
        var provider = new RotatingCredentials { CurrentUser = userA, CurrentPassword = "p1" };

        ServiceProvider? sp = null;
        try
        {
            var built = await BuildCacheAsync(provider, prefix);
            sp = built.Provider;
            var cache = built.Cache;
            var tracker = built.Tracker;
            var connection = built.Connection;
            var endpoint = connection.Multiplexer.GetEndPoints()[0];
            var server = connection.Multiplexer.GetServer(endpoint);
            var clientName = $"{connection.ClientName}-bcast";

            CreateAclUser(userB, "p2");
            provider.CurrentUser = userB;
            provider.CurrentPassword = "p2";

            var reauthed = await Poll.UntilAsync(() => tracker.Reauthentications >= 1, ReauthWindow);
            Assert.True(reauthed, "expected the in-place reauth to userB to happen before the kill.");

            var clientIdBefore = tracker.RedirectTargets[endpoint];

            var sawTrackingLost = false;
            var sawRestored = false;
            tracker.TrackingLost += ep => { if (Equals(ep, endpoint)) sawTrackingLost = true; };
            tracker.Armed += e =>
            {
                if (Equals(e.EndPoint, endpoint) && e.Reason == ArmReason.PushConnectionRestored) sawRestored = true;
            };

            // userA stays valid: deleting it would also disconnect the private multiplexer (still authenticated as A,
            // since the test provider re-authenticates nothing itself) and raise the very events asserted below. The
            // proof that the reconnect used the current credential is the user name on the new socket.
            RedisCli.Standalone("CLIENT", "KILL", "ID", clientIdBefore.ToString());

            var recovered = await Poll.UntilAsync(
                () => sawTrackingLost
                      && sawRestored
                      && tracker.RedirectTargets.TryGetValue(endpoint, out var current)
                      && current != clientIdBefore,
                TimeSpan.FromSeconds(10));
            Assert.True(recovered, "expected TrackingLost then an Armed(PushConnectionRestored) with a changed client id.");

            var sawRotatedUser = await Poll.UntilAsync(() =>
            {
                var current = FindBcastClient(server, clientName);
                return current is not null && string.Equals(UserOf(current), userB, StringComparison.Ordinal);
            }, TimeSpan.FromSeconds(5));
            Assert.True(sawRotatedUser, "the reconnected -bcast client did not show the current (rotated) user.");

            await AssertForeignWriteEvictsAsync(cache, prefix, "after-reconnect", "invalidation was lost after the reconnect that followed credential rotation.");
        }
        finally
        {
            if (sp is not null) await sp.DisposeAsync();
            DeleteAclUser(userA);
            DeleteAclUser(userB);
        }
    }

    [Fact]
    public async Task RejectedRotatedCredentialKeepsTheSocketArmedUntilAGoodOneArrives()
    {
        var userA = UniqueUser("t3a");
        var userB = UniqueUser("t3b");
        var prefix = UniquePrefix("t3");
        CreateAclUser(userA, "p1");
        var provider = new RotatingCredentials { CurrentUser = userA, CurrentPassword = "p1" };

        ServiceProvider? sp = null;
        try
        {
            var built = await BuildCacheAsync(provider, prefix);
            sp = built.Provider;
            var cache = built.Cache;
            var tracker = built.Tracker;
            var connection = built.Connection;
            var endpoint = connection.Multiplexer.GetEndPoints()[0];
            var server = connection.Multiplexer.GetServer(endpoint);
            var clientName = $"{connection.ClientName}-bcast";
            var clientIdBefore = tracker.RedirectTargets[endpoint];
            var flushesBefore = cache.Statistics.Flushes;

            var sawTrackingLost = false;
            tracker.TrackingLost += ep => { if (Equals(ep, endpoint)) sawTrackingLost = true; };

            // A password the server will reject. A failed AUTH leaves a Redis connection authenticated as before, so the
            // socket must stay armed: no TrackingLost, no flush, invalidations still delivered, and no AUTH storm.
            provider.CurrentPassword = $"wrong-{Guid.NewGuid():N}";
            await Task.Delay(BroadcastTracker.KeepAliveInterval * 2 + TimeSpan.FromSeconds(1));
            Assert.False(sawTrackingLost, "a rejected AUTH must not be treated as a dead socket.");
            Assert.Equal(0, tracker.Reauthentications);
            Assert.Equal(flushesBefore, cache.Statistics.Flushes);
            Assert.Equal(clientIdBefore, tracker.RedirectTargets[endpoint]);
            await AssertForeignWriteEvictsAsync(cache, prefix, "during-bad-credential", "invalidations stopped while the provider held a rejected credential.");

            // Once the provider yields a credential the server accepts, the same socket is re-authenticated in place.
            CreateAclUser(userB, "p2");
            try
            {
                provider.CurrentUser = userB;
                provider.CurrentPassword = "p2";

                var reauthed = await Poll.UntilAsync(() => tracker.Reauthentications >= 1, ReauthWindow);
                Assert.True(reauthed, "expected the in-place reauth once a valid credential was available.");
                Assert.False(sawTrackingLost, "the recovery must happen in place, on the same socket.");
                Assert.Equal(clientIdBefore, tracker.RedirectTargets[endpoint]);

                var sawRotatedUser = await Poll.UntilAsync(() =>
                {
                    var current = FindBcastClient(server, clientName);
                    return current is not null && string.Equals(UserOf(current), userB, StringComparison.Ordinal);
                }, TimeSpan.FromSeconds(5));
                Assert.True(sawRotatedUser, "the -bcast client did not show the rotated user after recovery.");

                await AssertForeignWriteEvictsAsync(cache, prefix, "after-recovery", "invalidations did not continue after recovering from the rejected credential.");
            }
            finally
            {
                DeleteAclUser(userB);
            }
        }
        finally
        {
            if (sp is not null) await sp.DisposeAsync();
            DeleteAclUser(userA);
        }
    }
}
