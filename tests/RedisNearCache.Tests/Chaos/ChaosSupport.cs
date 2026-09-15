using System.Diagnostics;
using StackExchange.Redis;

namespace RedisNearCache.Tests.Chaos;

/// <summary>
/// A genuinely foreign Redis client: a plain <see cref="ConnectionMultiplexer"/> with default settings
/// (no admin mode, no forced protocol, its own client name and therefore its own client ids). Writes made
/// through it are writes by "some other application" as far as the cache's private, tracked multiplexer is
/// concerned, so they produce real server-side invalidations. It is used instead of
/// <see cref="RedisCli"/> wherever a test needs thousands of writes per second; <c>docker exec redis-cli</c>
/// costs ~80 ms per call and would make the stress windows meaningless.
/// </summary>
internal sealed class ForeignClient : IAsyncDisposable
{
    public IConnectionMultiplexer Multiplexer { get; }

    private ForeignClient(IConnectionMultiplexer multiplexer) => Multiplexer = multiplexer;

    public IDatabase Db => Multiplexer.GetDatabase();

    public static async Task<ForeignClient> ConnectAsync(string connectionString, bool allowAdmin = false)
    {
        var cfg = ConfigurationOptions.Parse(connectionString);
        cfg.ClientName = $"chaos-foreign-{Guid.NewGuid():N}";
        cfg.AbortOnConnectFail = false;
        cfg.ConnectRetry = 5;
        cfg.ConnectTimeout = 5_000;
        if (allowAdmin) cfg.AllowAdmin = true;
        var mux = await ConnectionMultiplexer.ConnectAsync(cfg);
        return new ForeignClient(mux);
    }

    public async ValueTask DisposeAsync() => await Multiplexer.DisposeAsync();
}

/// <summary>
/// <c>docker exec</c> for commands that <see cref="RedisCli"/> cannot express: it always runs
/// <c>redis-cli</c> and always throws on a non-zero exit. Restarting a cluster node needs to run
/// <c>redis-server</c>, and <c>SHUTDOWN NOSAVE</c> legitimately drops the connection mid-command.
/// </summary>
internal static class DockerExec
{
    public static (int ExitCode, string StdOut, string StdErr) Run(string container, params string[] args)
    {
        var psi = new ProcessStartInfo("docker")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };
        psi.ArgumentList.Add("exec");
        psi.ArgumentList.Add(container);
        foreach (var a in args) psi.ArgumentList.Add(a);

        using var p = Process.Start(psi)!;
        var stdout = p.StandardOutput.ReadToEnd();
        var stderr = p.StandardError.ReadToEnd();
        p.WaitForExit();
        return (p.ExitCode, stdout.Trim(), stderr.Trim());
    }

    /// <summary><see cref="Run"/> without holding a thread-pool thread while docker runs (see <see cref="DockerProcess"/>).</summary>
    public static Task<(int ExitCode, string StdOut, string StdErr)> RunAsync(string container, params string[] args) =>
        DockerProcess.RunAsync(["exec", container, .. args]);
}

internal static class ChaosSupport
{
    /// <summary>
    /// Waits for the system to settle instead of sleeping a fixed amount: polls until the cache has counted
    /// no new invalidation and no new flush for <paramref name="stableFor"/>. Every stale-check in this
    /// directory runs after this, so "L1 holds an old value" can only mean an invalidation was lost, never
    /// that one was still on its way.
    /// </summary>
    public static async Task<bool> QuiesceAsync(
        IRedisNearCache cache,
        TimeSpan? stableFor = null,
        TimeSpan? timeout = null)
    {
        var window = stableFor ?? TimeSpan.FromMilliseconds(500);
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(15));
        var step = TimeSpan.FromMilliseconds(50);

        long last = -1;
        var lastChanged = DateTime.UtcNow;
        while (DateTime.UtcNow < deadline)
        {
            long now = cache.Statistics.Invalidations + cache.Statistics.Flushes + cache.Statistics.Rearms;
            if (now != last)
            {
                last = now;
                lastChanged = DateTime.UtcNow;
            }
            else if (DateTime.UtcNow - lastChanged >= window)
            {
                return true;
            }

            await Task.Delay(step);
        }

        return false;
    }

    /// <summary>
    /// The core assertion of this whole directory, factored out because eight tests make it: once the system
    /// has settled, L1 must either hold nothing for a key or hold exactly what Redis holds. Returns the list
    /// of keys that violated it, with both values, so a failure message names the actual divergence.
    /// </summary>
    public static async Task<List<string>> FindStaleAsync(
        IRedisNearCache cache,
        ForeignClient truth,
        IEnumerable<string> keys)
    {
        var stale = new List<string>();
        foreach (var key in keys)
        {
            RedisValue actual = await truth.Db.StringGetAsync(key);
            if (!cache.TryGetLocal<string>(key, out var local)) continue;
            string? server = actual.IsNull ? null : actual.ToString();
            if (!string.Equals(local, server, StringComparison.Ordinal))
            {
                stale.Add($"{key}: L1='{local}' redis='{server ?? "<nil>"}'");
            }
        }

        return stale;
    }

    /// <summary>
    /// Retries an operation while the private multiplexer is still reconnecting. Tests that kill connections
    /// need this: the first command after a kill can fail before StackExchange.Redis has finished reconnecting,
    /// and that failure is not the thing under test.
    /// </summary>
    public static async Task<T> WithReconnectRetryAsync<T>(Func<Task<T>> operation, TimeSpan? timeout = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(15));
        while (true)
        {
            try
            {
                return await operation();
            }
            catch (Exception ex) when (ex is RedisConnectionException or RedisTimeoutException or RedisServerException)
            {
                if (DateTime.UtcNow >= deadline) throw;
                await Task.Delay(100);
            }
        }
    }

    public static Task WithReconnectRetryAsync(Func<Task> operation, TimeSpan? timeout = null) =>
        WithReconnectRetryAsync(async () => { await operation(); return true; }, timeout);
}
