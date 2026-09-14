using Xunit.Abstractions;

namespace RedisNearCache.Tests.Compat;

/// <summary>
/// Helpers shared by the compatibility tests only. Everything general (key generation, polling, redis-cli,
/// the foreign multiplexer, throwaway providers) comes from the existing helpers elsewhere in this project;
/// this file adds the two things only these tests need: asking the server what it is (so a test can skip a
/// command the matrix's oldest server does not have), and saving/restoring server configuration.
/// </summary>
internal static class CompatSupport
{
    /// <summary>Parses an INFO section into its <c>field:value</c> pairs.</summary>
    public static Dictionary<string, string> ParseInfo(string info)
    {
        var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var raw in info.Split('\n', StringSplitOptions.RemoveEmptyEntries))
        {
            var line = raw.Trim();
            if (line.Length == 0 || line[0] == '#') continue;
            var colon = line.IndexOf(':');
            if (colon <= 0) continue;
            result[line[..colon]] = line[(colon + 1)..];
        }

        return result;
    }

    /// <summary>One INFO section of the standalone server, read with redis-cli.</summary>
    public static Dictionary<string, string> Info(string section) =>
        ParseInfo(RedisCli.Standalone("INFO", section));

    /// <summary>
    /// The version the server reports as <c>redis_version</c>. Valkey reports a Redis-compatible value here
    /// (plus its own <c>valkey_version</c>), so feature gating on this field works across the whole matrix.
    /// </summary>
    public static Version ServerVersion()
    {
        var info = Info("server");
        return ParseVersion(info.GetValueOrDefault("redis_version"))
            ?? ParseVersion(info.GetValueOrDefault("valkey_version"))
            ?? new Version(0, 0);
    }

    private static Version? ParseVersion(string? value)
    {
        if (string.IsNullOrWhiteSpace(value)) return null;
        // Some builds append suffixes ("7.4.0-rc1"); Version.Parse only wants the numeric part.
        var numeric = new string(value.TakeWhile(c => char.IsDigit(c) || c == '.').ToArray()).Trim('.');
        return Version.TryParse(numeric, out var parsed) ? parsed : null;
    }

    /// <summary>
    /// Skip-if for the CI matrix (redis 6.2/7.0/7.2/7.4/8, valkey 8.1): returns false and writes why to the
    /// test output when the server predates <paramref name="minimum"/>. xunit has no runtime Skip, so callers
    /// return early instead, leaving a passing test with an explicit note in the matrix log.
    /// </summary>
    public static bool RequireServer(Version minimum, string feature, ITestOutputHelper output)
    {
        var actual = ServerVersion();
        if (actual >= minimum) return true;
        output.WriteLine($"SKIPPED: {feature} needs Redis {minimum} or later; this server reports {actual}.");
        return false;
    }

    /// <summary>Reads one CONFIG parameter of the standalone server (empty string when the server has no such parameter).</summary>
    public static string ConfigGet(string parameter)
    {
        var lines = RedisCli.Standalone("CONFIG", "GET", parameter)
            .Split('\n', StringSplitOptions.RemoveEmptyEntries);
        // redis-cli prints the name on one line and the value on the next; an unset/absent parameter prints nothing.
        return lines.Length >= 2 ? lines[1].Trim() : string.Empty;
    }

    /// <summary>Sets one CONFIG parameter on the standalone server.</summary>
    public static void ConfigSet(string parameter, string value) =>
        RedisCli.Standalone("CONFIG", "SET", parameter, value);

    /// <summary>
    /// Number of keys the server currently has in its tracking table, from <c>INFO stats</c>. Server-wide:
    /// it counts every tracking client, not just ours, so tests assert a lower bound.
    /// </summary>
    public static long TrackingTotalKeys() =>
        long.TryParse(Info("stats").GetValueOrDefault("tracking_total_keys"), out var keys) ? keys : -1;
}
