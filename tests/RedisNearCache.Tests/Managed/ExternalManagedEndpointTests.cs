using RedisNearCache.Tests.EdgeCases;
using StackExchange.Redis;
using Xunit;
using Xunit.Abstractions;

namespace RedisNearCache.Tests.Managed;

/// <summary>
/// Opt-in test against a REAL managed Redis endpoint (AWS ElastiCache, Azure Managed Redis, or any other
/// server you point it at) - the only test in this repo that is not Docker-backed. It is skipped unless
/// <c>RNC_EXTERNAL_REDIS</c> is set (see <see cref="ExternalManagedRedisFact"/>), and it is deliberately
/// conservative so it is safe to run against a production-like instance: unique key prefixes only
/// (<see cref="TestHelpers.Key"/>), no <c>FLUSHDB</c>/<c>FLUSHALL</c>, no <c>CLIENT KILL</c>, no
/// <c>CONFIG</c>, no docker calls of any kind, and it deletes only the one key it wrote.
///
/// <para>To run against AWS ElastiCache (Redis OSS or Valkey, cluster-mode enabled or disabled):</para>
/// <code>
/// RNC_EXTERNAL_REDIS="my-cluster.xxxxxx.clustercfg.use1.cache.amazonaws.com:6379" \
///   dotnet test tests/RedisNearCache.Tests --filter FullyQualifiedName~ExternalManagedEndpointTests
/// # append ",ssl=true" if in-transit encryption is enabled, and ",user=...,password=..." if an ACL/AUTH
/// # token is configured.
/// </code>
///
/// <para>To run against Azure Managed Redis (Enterprise or the OSS cluster tier):</para>
/// <code>
/// RNC_EXTERNAL_REDIS="my-cache.region.redis.azure.net:10000,ssl=true,password=&lt;access-key&gt;" \
///   dotnet test tests/RedisNearCache.Tests --filter FullyQualifiedName~ExternalManagedEndpointTests
/// </code>
///
/// The connection string is parsed with <see cref="ConfigurationOptions.Parse(string)"/>, so anything that
/// syntax accepts - standalone, cluster, or TLS - works here unchanged.
/// </summary>
public class ExternalManagedEndpointTests
{
    private readonly ITestOutputHelper _out;

    public ExternalManagedEndpointTests(ITestOutputHelper output) => _out = output;

    [ExternalManagedRedisFact]
    public async Task ArmsAndInvalidatesAgainstTheConfiguredExternalEndpoint()
    {
        var connectionString = Environment.GetEnvironmentVariable("RNC_EXTERNAL_REDIS")!;
        var key = TestHelpers.Key("managed-external");

        EdgeCaseProvider? handle = null;
        ConnectionMultiplexer? foreign = null;
        try
        {
            handle = await EdgeCaseSupport.BuildAsync(connectionString);
            var cache = handle.Cache;

            Assert.NotEmpty(handle.Armer.RedirectTargets);
            foreach (var (endpoint, redirectId) in handle.Armer.RedirectTargets)
            {
                _out.WriteLine($"armed {endpoint} -> redirect client id {redirectId}");
            }

            try
            {
                var anyServer = handle.Connection.ConnectedMasters().FirstOrDefault();
                var raw = anyServer?.InfoRaw("server");
                var versionLine = raw?
                    .Split('\n')
                    .FirstOrDefault(l => l.StartsWith("redis_version:", StringComparison.Ordinal) || l.StartsWith("valkey_version:", StringComparison.Ordinal));
                if (versionLine is not null) _out.WriteLine($"server: {versionLine.Trim()}");
            }
            catch (Exception ex)
            {
                // Some managed offerings restrict INFO sections for non-admin users; this is diagnostic only.
                _out.WriteLine($"could not read server version from INFO (may be restricted): {ex.Message}");
            }

            await cache.SetAsync(key, "v1");
            Assert.True(await TestHelpers.ReadUntilCachedAsync(cache, key, "v1"), $"{key} was not cached against the external endpoint.");

            // A foreign write, through a completely separate plain multiplexer, must still evict L1.
            var foreignCfg = ConfigurationOptions.Parse(connectionString);
            foreignCfg.ClientName = $"rnc-managed-external-foreign-{Guid.NewGuid():N}";
            foreign = await ConnectionMultiplexer.ConnectAsync(foreignCfg);
            await foreign.GetDatabase().StringSetAsync(key, "v2");

            var evicted = await Poll.UntilAsync(() => !cache.TryGetLocal<string>(key, out _), TimeSpan.FromSeconds(15));
            Assert.True(evicted, "an external write did not evict L1 within the deadline.");
            Assert.Equal("v2", await cache.GetAsync<string>(key));
            _out.WriteLine($"external endpoint: {cache.Statistics}");
        }
        finally
        {
            if (handle is not null)
            {
                try { await handle.Cache.RemoveAsync(key); }
                catch (Exception ex) { _out.WriteLine($"cleanup RemoveAsync failed: {ex.Message}"); }
                await handle.DisposeAsync();
            }
            if (foreign is not null)
            {
                try { await foreign.GetDatabase().KeyDeleteAsync(key); }
                catch (Exception ex) { _out.WriteLine($"cleanup KeyDeleteAsync failed: {ex.Message}"); }
                await foreign.DisposeAsync();
            }
        }
    }
}
