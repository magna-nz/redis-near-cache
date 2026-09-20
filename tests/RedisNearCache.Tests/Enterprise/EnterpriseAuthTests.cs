using RedisNearCache.Tests.EdgeCases;
using StackExchange.Redis;
using Xunit;
using Xunit.Abstractions;

namespace RedisNearCache.Tests.Enterprise;

/// <summary>
/// Opt-in test against a PASSWORD-PROTECTED Redis Enterprise-based database, behind the same proxy as
/// <see cref="EnterpriseFact"/>'s. Skipped unless <c>RNC_ENTERPRISE_AUTH_REDIS</c> names it (e.g.
/// <c>localhost:12001</c> from <c>./enterprise-up.sh</c>). Separate from <see cref="EnterpriseFact"/> because the
/// two databases are different endpoints: <c>RNC_ENTERPRISE_REDIS</c> has no password, this one does.
/// </summary>
public sealed class EnterpriseAuthFact : FactAttribute
{
    public EnterpriseAuthFact()
    {
        if (string.IsNullOrEmpty(Environment.GetEnvironmentVariable("RNC_ENTERPRISE_AUTH_REDIS")))
        {
            Skip = "set RNC_ENTERPRISE_AUTH_REDIS (e.g. localhost:12001 from ./enterprise-up.sh) to run";
        }
    }
}

/// <summary>
/// Authentication through a Redis Enterprise-based proxy, in Broadcast mode (the proxy rejects <c>REDIRECT</c>, so
/// Redirect mode cannot arm there at all - see
/// <see cref="EnterpriseTests.RedirectModeFailsStartupWithABroadcastModeHint"/>). The thing that only a real proxy
/// can answer: the Broadcast tracker's hand-rolled RESP3 socket authenticates with
/// <c>HELLO 3 AUTH default &lt;password&gt;</c> - a password with no user - and the proxy has to accept that form,
/// arm <c>CLIENT TRACKING ON BCAST</c> on the same connection, and keep pushing invalidations.
/// </summary>
[Collection("enterprise")]
public class EnterpriseAuthTests
{
    private const string Container = "redis-near-cache-enterprise";
    private const int Port = 12001;
    private const string Password = "rnc-enterprise-pw";

    private static string EndPoint => Environment.GetEnvironmentVariable("RNC_ENTERPRISE_AUTH_REDIS")!;

    private static string ConnectionString => $"{EndPoint},password={Password}";

    private readonly ITestOutputHelper _out;

    public EnterpriseAuthTests(ITestOutputHelper output) => _out = output;

    private static string UniquePrefix() => $"ent:{Guid.NewGuid():N}:";

    /// <summary>
    /// The foreign write, from redis-cli inside the Enterprise container: a genuinely different client, and
    /// authenticated separately, so the eviction it causes proves the proxy is pushing to our tracking socket.
    /// </summary>
    private static string Cli(params string[] args) =>
        RedisCli.Run(Container, ["-p", Port.ToString(System.Globalization.CultureInfo.InvariantCulture),
                                 "-a", Password, "--no-auth-warning", .. args]);

    /// <summary>
    /// A password with no user, Broadcast mode: arms through the proxy and a foreign write evicts L1. This is the
    /// <c>HELLO 3 AUTH default &lt;pw&gt;</c> wire form against the one server type that is not OSS Redis.
    /// </summary>
    [EnterpriseAuthFact]
    public async Task PasswordOnly_Broadcast_Works()
    {
        var prefix = UniquePrefix();
        var key = prefix + Guid.NewGuid().ToString("N");
        EdgeCaseProvider? handle = null;
        try
        {
            handle = await EdgeCaseSupport.BuildAsync(ConnectionString, o =>
            {
                o.TrackingMode = TrackingMode.Broadcast;
                o.KeyPrefixes.Add(prefix);
            });
            Assert.NotEmpty(handle.Armer.RedirectTargets);
            var cache = handle.Cache;
            Assert.True(cache.IsCoherent);

            await cache.SetAsync(key, "v1");
            Assert.True(await TestHelpers.ReadUntilCachedAsync(cache, key, "v1", TimeSpan.FromSeconds(15)),
                $"{key} was not cached against the password-protected Enterprise database.");

            var hit = await Poll.UntilAsync(async () =>
            {
                var before = cache.Statistics.Hits;
                var value = await cache.GetAsync<string>(key);
                return value == "v1" && cache.Statistics.Hits == before + 1;
            }, TimeSpan.FromSeconds(15));
            Assert.True(hit, $"no read was served from L1. stats={cache.Statistics}");

            Cli("SET", key, "v2");
            var evicted = await Poll.UntilAsync(() => !cache.TryGetLocal<string>(key, out _), TimeSpan.FromSeconds(15));
            Assert.True(evicted, "a foreign write did not evict L1 within the deadline.");
            Assert.Equal("v2", await cache.GetAsync<string>(key));

            Assert.True(await cache.RemoveAsync(key));
            Assert.Null(await cache.GetAsync<string>(key));
            _out.WriteLine($"enterprise password-only broadcast: {cache.Statistics}");
        }
        finally
        {
            if (handle is not null) await handle.DisposeAsync();
            try { Cli("DEL", key); }
            catch (InvalidOperationException) { /* best effort cleanup */ }
        }
    }

    /// <summary>A wrong password must fail loudly and promptly rather than hanging or degrading silently.</summary>
    [EnterpriseAuthFact]
    public async Task WrongPassword_Broadcast_FailsLoudlyAndPromptly() =>
        await AssertFailsAsync($"{EndPoint},password=not-the-password", "enterprise, wrong password");

    /// <summary>No password at all against a database that requires one: the same loud, prompt failure.</summary>
    [EnterpriseAuthFact]
    public async Task NoPassword_Broadcast_FailsLoudlyAndPromptly() =>
        await AssertFailsAsync(EndPoint, "enterprise, no password");

    private async Task AssertFailsAsync(string connectionString, string context)
    {
        var options = ConfigurationOptions.Parse($"{connectionString},abortConnect=true,connectRetry=1,connectTimeout=5000");
        var clock = System.Diagnostics.Stopwatch.StartNew();
        Exception? failure;
        try
        {
            failure = await Record.ExceptionAsync(async () =>
            {
                var handle = await EdgeCaseSupport.BuildAsync(o =>
                {
                    o.Configuration = options;
                    o.TrackingMode = TrackingMode.Broadcast;
                    o.KeyPrefixes.Add(UniquePrefix());
                }, awaitReady: false);
                try
                {
                    await handle.Cache.Ready;
                }
                finally
                {
                    await handle.DisposeAsync();
                }
            }).WaitAsync(TimeSpan.FromSeconds(20));
        }
        catch (TimeoutException)
        {
            Assert.Fail($"{context}: hung for {clock.Elapsed.TotalSeconds:0.0} s instead of failing.");
            return;
        }

        _out.WriteLine($"{context}: {clock.ElapsedMilliseconds} ms, {Describe(failure)}");
        Assert.NotNull(failure);
        Assert.IsAssignableFrom<RedisConnectionException>(failure);
    }

    private static string Describe(Exception? ex)
    {
        if (ex is null) return "<nothing>";
        var messages = new List<string>();
        var queue = new Queue<Exception>([ex]);
        while (queue.Count > 0)
        {
            var current = queue.Dequeue();
            messages.Add($"{current.GetType().Name}: {current.Message}");
            if (current is AggregateException aggregate)
            {
                foreach (var inner in aggregate.InnerExceptions) queue.Enqueue(inner);
            }
            else if (current.InnerException is { } single)
            {
                queue.Enqueue(single);
            }
        }

        return string.Join(" | ", messages);
    }
}
