namespace RedisNearCache.Bench.Micro;

/// <summary>A small JSON payload (5 fields) used alongside the 1 KB string value. Same shape for both micro tools.</summary>
public sealed record SampleRecord(string Name, int Age, bool Active, double Score, string Description);

/// <summary>Which of the two payload shapes a given hit/miss benchmark exercises.</summary>
public enum DataKind
{
    String,
    Json,
}

/// <summary>Payload construction and the environment variables the out-of-process BenchmarkDotNet child processes use to
/// learn the endpoint the parent process was given on the command line (children inherit the parent's environment, not
/// its argv).</summary>
public static class Payloads
{
    public const string EnvEndpoint = "RNC_BENCH_ENDPOINT";
    public const string EnvTlsCa = "RNC_BENCH_TLS_CA";

    /// <summary>A 1 KB string value.</summary>
    public static readonly string StringValue = new('x', 1024);

    public static readonly SampleRecord JsonValue =
        new("bench-user", 42, true, 3.14159, "A small sample JSON payload used for benchmarking.");

    public static object ValueFor(DataKind kind) => kind == DataKind.String ? StringValue : JsonValue;
}
