using System.Diagnostics;

namespace RedisNearCache.Tests;

/// <summary>
/// Runs `redis-cli` inside the already-running docker containers via `docker exec`, so that writes used
/// to prove server-side invalidation genuinely come from a foreign client and never touch the cache's
/// own private multiplexer.
/// </summary>
internal static class RedisCli
{
    public const string StandaloneContainer = "redis-near-cache-redis";
    public const string ClusterContainer = "redis-near-cache-cluster";

    public static string Run(string container, params string[] args)
    {
        var psi = new ProcessStartInfo("docker")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };
        psi.ArgumentList.Add("exec");
        psi.ArgumentList.Add(container);
        psi.ArgumentList.Add("redis-cli");
        foreach (var a in args) psi.ArgumentList.Add(a);

        using var p = Process.Start(psi)!;
        var output = p.StandardOutput.ReadToEnd();
        var error = p.StandardError.ReadToEnd();
        p.WaitForExit();
        if (p.ExitCode != 0)
        {
            throw new InvalidOperationException(
                $"redis-cli {string.Join(' ', args)} in {container} exited {p.ExitCode}: {error}");
        }
        return output.Trim();
    }

    /// <summary>Runs redis-cli against the single-node standalone container.</summary>
    public static string Standalone(params string[] args) => Run(StandaloneContainer, args);

    /// <summary>Runs redis-cli in cluster mode (`-c`) against one master port of the cluster container.</summary>
    public static string Cluster(int port, params string[] args)
    {
        var full = new List<string> { "-c", "-p", port.ToString() };
        full.AddRange(args);
        return Run(ClusterContainer, full.ToArray());
    }
}
