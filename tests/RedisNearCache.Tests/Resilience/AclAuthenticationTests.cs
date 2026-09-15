using RedisNearCache.Tests.EdgeCases;
using StackExchange.Redis;
using Xunit;
using Xunit.Abstractions;

namespace RedisNearCache.Tests.Resilience;

/// <summary>
/// Authenticating as a Redis ACL user rather than the default one. Two things could plausibly go wrong and
/// neither is visible without a real server: the credentials could be dropped by the clone that
/// <c>RedisNearCacheConnection.BuildConfiguration</c> makes of the caller's options, and an unauthenticated
/// private connection could fail in a way that hangs startup rather than reporting it. The permissions granted
/// here are deliberately complete (<c>+@all</c>, <c>~*</c>, <c>&amp;*</c>): the narrower case, where
/// <c>CLIENT TRACKING</c> specifically is denied, is already covered by
/// <see cref="EdgeCases.DegradedModeRecoveryTests"/>.
/// </summary>
public class AclAuthenticationTests
{
    private const string AclUser = "rnc-user";
    private const string AclPassword = "secret";

    private readonly ITestOutputHelper _out;

    public AclAuthenticationTests(ITestOutputHelper output) => _out = output;

    private static void CreateUser() => ResilienceSupport.CreateAclUser(AclUser, AclPassword);

    private static void DeleteUser() => ResilienceSupport.DeleteAclUser(AclUser);

    [Fact]
    public async Task AclAuthenticatedUserWorks()
    {
        CreateUser();
        EdgeCaseProvider? handle = null;
        var key = TestHelpers.Key("acl-user");
        try
        {
            handle = await EdgeCaseSupport.BuildAsync(
                $"{StandaloneCacheFixture.ConnectionString},user={AclUser},password={AclPassword}");
            var cache = handle.Cache;

            // Tracking armed as the ACL user: CLIENT LIST, CLIENT TRACKING and SUBSCRIBE all had to succeed.
            Assert.NotEmpty(handle.Armer.RedirectTargets);
            _out.WriteLine($"armed as {AclUser}: {handle.Armer.RedirectTargets.Count} redirect target(s)");

            await cache.SetAsync(key, "v1");
            Assert.True(await TestHelpers.ReadUntilCachedAsync(cache, key, "v1"), "the key was not cached as the ACL user.");

            var hitsBefore = cache.Statistics.Hits;
            Assert.Equal("v1", await cache.GetAsync<string>(key));
            Assert.Equal(hitsBefore + 1, cache.Statistics.Hits);

            // The foreign write comes from the DEFAULT user; invalidation is per connection, not per user.
            RedisCli.Standalone("SET", key, "v2");
            var evicted = await Poll.UntilAsync(() => !cache.TryGetLocal<string>(key, out _), TimeSpan.FromSeconds(5));
            Assert.True(evicted, "an external write did not evict while the cache was authenticated as an ACL user.");
            Assert.Equal("v2", await cache.GetAsync<string>(key));
            _out.WriteLine($"acl user: {cache.Statistics}");
        }
        finally
        {
            if (handle is not null) await handle.DisposeAsync();
            RedisCli.Standalone("DEL", key);
            DeleteUser();
        }
    }

    /// <summary>
    /// A wrong password must fail loudly and promptly. The failure mode worth excluding is a hang: the private
    /// multiplexer is created inside a DI factory and <see cref="IRedisNearCache.Ready"/> is started in a
    /// constructor, so a connection that never resolves would deadlock the caller's startup rather than
    /// throwing. The whole attempt is therefore given a deadline and the deadline being hit is itself a failure.
    /// </summary>
    /// <remarks>
    /// This runs against the TLS container with a temporary <c>requirepass</c>, not against the standalone one
    /// with an ACL user, because on the standalone container a wrong password does not fail at all: its
    /// <c>default</c> user is <c>nopass ~* &amp;* +@all</c>, so a connection is already fully authorised as
    /// <c>default</c> before it sends <c>AUTH</c>, and a rejected <c>AUTH</c> leaves it exactly where it was.
    /// Verified with redis-cli against redis:7.4 - <c>redis-cli --user u --pass wrong ACL WHOAMI</c> prints the
    /// <c>WRONGPASS</c> error and then <c>default</c>. The TLS container has no replica and no other user, so
    /// setting and clearing <c>requirepass</c> on it for the duration of this test disturbs nothing else.
    /// </remarks>
    [Fact]
    public async Task WrongPasswordFailsClearly()
    {
        const string ServerPassword = "rnc-wrong-password-test";
        using var ca = ResilienceSupport.LoadPemCertificate(ResilienceSupport.CaCertPath());

        ResilienceSupport.Tls("CONFIG", "SET", "requirepass", ServerPassword);
        try
        {
            var options = ResilienceSupport.TlsConfiguration(ca);
            options.Password = "not-the-password";
            // A rejected AUTH is a definitive answer, so there is nothing to gain from retrying it; without
            // this, a contended machine can spend ConnectRetry x ConnectTimeout (3 x 15 s by default here)
            // before giving up, which was measured at 30 s on one run and would eventually hit the deadline.
            options.ConnectRetry = 1;
            options.ConnectTimeout = 5_000;

            Exception? failure;
            var clock = System.Diagnostics.Stopwatch.StartNew();
            try
            {
                failure = await Record.ExceptionAsync(() => AttemptAsync(options)).WaitAsync(TimeSpan.FromSeconds(30));
            }
            catch (TimeoutException)
            {
                Assert.Fail($"connecting with a wrong password hung for {clock.Elapsed.TotalSeconds:0} s instead of failing.");
                return;
            }

            _out.WriteLine($"wrong password produced {failure?.GetType().FullName ?? "<nothing>"} after " +
                           $"{clock.ElapsedMilliseconds} ms: {failure?.Message}");
            Assert.NotNull(failure);
            Assert.IsAssignableFrom<RedisConnectionException>(failure);
        }
        finally
        {
            var reset = ResilienceSupport.TryTls(ServerPassword, "CONFIG", "SET", "requirepass", "");
            _out.WriteLine($"requirepass reset exited {reset.ExitCode}: {reset.StdOut}{reset.StdErr}");
            var open = await Poll.UntilAsync(
                () => ResilienceSupport.TryTls(null, "PING").StdOut.Contains("PONG", StringComparison.Ordinal),
                TimeSpan.FromSeconds(15), TimeSpan.FromMilliseconds(100));
            Assert.True(open, "the TLS container was left requiring a password; later runs would fail.");
        }

        static async Task AttemptAsync(ConfigurationOptions options)
        {
            // Resolving IRedisNearCache builds the private multiplexer synchronously, so this may throw here;
            // if a future change makes the connect lazy, the fault surfaces from Ready instead. Either is fine.
            var handle = await EdgeCaseSupport.BuildAsync(o => o.Configuration = options, awaitReady: false);
            try
            {
                await handle.Cache.Ready;
            }
            finally
            {
                await handle.DisposeAsync();
            }
        }
    }
}
