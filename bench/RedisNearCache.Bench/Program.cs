using System.Globalization;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Engines;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Filters;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Running;
using BenchmarkDotNet.Toolchains;
using BenchmarkDotNet.Toolchains.CsProj;
using BenchmarkDotNet.Toolchains.DotNetCli;
using BenchmarkDotNet.Toolchains.InProcess.Emit;
using Microsoft.Extensions.DependencyInjection;
using RedisNearCache.Bench.Micro;
using StackExchange.Redis;
using JsonExporter = BenchmarkDotNet.Exporters.Json.JsonExporter;

namespace RedisNearCache.Bench;

public static class Program
{
    public static async Task<int> Main(string[] args)
    {
        if (args.Contains("--demo"))
        {
            await RunDemoAsync();
            return 0;
        }

        if (args.Contains("--load"))
        {
            await LoadTest.RunAsync(args);
            return 0;
        }

        if (args.Contains("--bdn"))
        {
            RunBdn(args);
            return 0;
        }

        Console.Error.WriteLine("usage: RedisNearCache.Bench --demo | --load [options] | --bdn [--quick] --endpoint E --latency-label L --artifacts DIR [--tls-ca P]");
        Console.Error.WriteLine("Sailfish per-call comparisons moved to a separate project (Perfolizer version conflict with BenchmarkDotNet): RedisNearCache.Bench.Sailfish [--quick] --endpoint E --latency-label L --output DIR [--tls-ca P]");
        return 1;
    }

    // --- shared arg parsing ----------------------------------------------------------------------------------

    private static string? GetArg(string[] args, string name)
    {
        var i = Array.IndexOf(args, name);
        return i >= 0 && i + 1 < args.Length ? args[i + 1] : null;
    }

    /// <summary>Publishes <c>--endpoint</c>/<c>--tls-ca</c>/<c>--quick</c> to the environment: BenchmarkDotNet's
    /// out-of-process child processes only inherit the parent's environment, not its argv, and Sailfish's
    /// in-process global setup reads the same variables for a single code path (see <see cref="MicroConfig"/>).</summary>
    private static (bool Quick, string Endpoint, string? TlsCa, string LatencyLabel) ParseMicroArgs(string[] args)
    {
        var quick = args.Contains("--quick");
        var endpoint = GetArg(args, "--endpoint") ?? "localhost:6379";
        var tlsCa = GetArg(args, "--tls-ca");
        var latencyLabel = GetArg(args, "--latency-label") ?? "0ms";

        Environment.SetEnvironmentVariable(Payloads.EnvEndpoint, endpoint);
        Environment.SetEnvironmentVariable(Payloads.EnvTlsCa, tlsCa ?? "");
        Environment.SetEnvironmentVariable(MicroConfig.EnvQuick, quick ? "1" : "0");

        return (quick, endpoint, tlsCa, latencyLabel);
    }

    /// <summary>Resolves a dotnet CLI executable for BenchmarkDotNet's out-of-process toolchain: the <c>DOTNET</c>
    /// environment variable, then <c>dotnet</c> on <c>PATH</c>, then <c>~/.dotnet/dotnet</c> (this machine does not
    /// have the SDK on PATH; only the last of these three normally resolves here).</summary>
    private static string ResolveDotNetPath()
    {
        var env = Environment.GetEnvironmentVariable("DOTNET");
        if (env is { Length: > 0 } && File.Exists(env)) return env;

        var pathVar = Environment.GetEnvironmentVariable("PATH");
        if (pathVar is not null)
        {
            foreach (var dir in pathVar.Split(Path.PathSeparator))
            {
                var candidate = Path.Combine(dir, "dotnet");
                if (File.Exists(candidate)) return candidate;
            }
        }

        var home = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
        var fallback = Path.Combine(home, ".dotnet", "dotnet");
        if (File.Exists(fallback)) return fallback;

        throw new InvalidOperationException("cannot resolve a dotnet executable: set DOTNET, put dotnet on PATH, or install the SDK to ~/.dotnet/dotnet");
    }

    // --- --bdn -------------------------------------------------------------------------------------------------

    private static void RunBdn(string[] args)
    {
        var (quick, endpoint, tlsCa, latencyLabel) = ParseMicroArgs(args);
        var artifacts = GetArg(args, "--artifacts") ?? throw new ArgumentException("--bdn requires --artifacts DIR");

        var dotnetPath = ResolveDotNetPath();
        IToolchain toolchain;
        try
        {
            var netCoreAppSettings = new NetCoreAppSettings("net10.0", runtimeFrameworkVersion: null, name: ".NET 10", customDotNetCliPath: dotnetPath);
            toolchain = CsProjCoreToolchain.From(netCoreAppSettings);
            Console.WriteLine($"BenchmarkDotNet toolchain: out-of-process (CsProjCoreToolchain), dotnet={dotnetPath}");
        }
        catch (Exception ex)
        {
            // NOTE: if the out-of-process toolchain genuinely cannot build/run on this machine, fall back to running
            // every benchmark in this same process. This loses the isolation between benchmark cases that
            // out-of-process gives for free (see HitBenchmarks/MissBenchmarks remarks) -- flagged loudly here and in
            // the final report, not silently swallowed.
            Console.WriteLine($"WARNING: could not construct the out-of-process BenchmarkDotNet toolchain ({ex.GetType().Name}: {ex.Message}).");
            Console.WriteLine("WARNING: falling back to InProcessEmitToolchain. Benchmark cases share one process; isolation between contenders is reduced.");
            toolchain = InProcessEmitToolchain.Instance;
        }

        Console.WriteLine($"BenchmarkDotNet: endpoint={endpoint} tls-ca={tlsCa ?? "(none)"} latency-label={latencyLabel} quick={quick} artifacts={artifacts}");

        var hitJob = Job.Default
            .WithStrategy(RunStrategy.Throughput)
            .WithWarmupCount(quick ? MicroJobSettings.QuickHitWarmupCount : MicroJobSettings.HitWarmupCount)
            .WithIterationCount(quick ? MicroJobSettings.QuickHitIterationCount : MicroJobSettings.HitIterationCount)
            .WithToolchain(toolchain);

        var missJob = Job.Default
            .WithStrategy(RunStrategy.Throughput)
            .WithInvocationCount(quick ? MicroJobSettings.QuickMissInvocationCount : MicroJobSettings.MissInvocationCount)
            .WithUnrollFactor(quick ? MicroJobSettings.QuickMissUnrollFactor : MicroJobSettings.MissUnrollFactor)
            .WithWarmupCount(quick ? MicroJobSettings.QuickMissWarmupCount : MicroJobSettings.MissWarmupCount)
            .WithIterationCount(quick ? MicroJobSettings.QuickMissIterationCount : MicroJobSettings.MissIterationCount)
            .WithToolchain(toolchain);

        // HitBenchmarks<T>/MissBenchmarks<T> are generic over the payload type ([GenericTypeArguments]), not
        // [Params]-driven. BenchmarkDotNet's generic-type expansion (GenericBenchmarksBuilder) only accepts the open
        // generic type definition -- passing a closed type like HitBenchmarks<string> directly throws ("is not a
        // GenericTypeDefinition"), verified directly -- so both payloads are always passed to Run, and --quick
        // restricts to the String closure with a case filter on the closed generic argument instead.
        IFilter[] filters = quick
            ? new IFilter[] { new SimpleFilter(bc => bc.Descriptor.Type.GetGenericArguments() is [var arg] && arg == typeof(string)) }
            : Array.Empty<IFilter>();

        var hitConfig = ManualConfig.Create(DefaultConfig.Instance)
            .AddJob(hitJob)
            .WithArtifactsPath(artifacts)
            .AddExporter(MarkdownExporter.GitHub, JsonExporter.Full)
            .AddFilter(filters);

        var missConfig = ManualConfig.Create(DefaultConfig.Instance)
            .AddJob(missJob)
            .WithArtifactsPath(artifacts)
            .AddExporter(MarkdownExporter.GitHub, JsonExporter.Full)
            .AddFilter(filters);

        BenchmarkRunner.Run(typeof(HitBenchmarks<>), hitConfig, Array.Empty<string>());
        BenchmarkRunner.Run(typeof(MissBenchmarks<>), missConfig, Array.Empty<string>());
    }

    /// <summary>
    /// Zero-traffic demonstration: shows that 1,000 reads of a key already cached in L1 generate zero Redis
    /// GET commands, and measures how long it takes an external write (via a second, plain multiplexer) to
    /// reach the near cache and evict the local copy.
    /// </summary>
    private static async Task RunDemoAsync()
    {
        const string connectionString = "localhost:6379";
        const string key = "demo:near-cache:zero-traffic";
        const int repeatCount = 1000;

        Console.WriteLine("RedisNearCache zero-traffic demo");
        Console.WriteLine("=================================");
        Console.WriteLine($"Connecting to {connectionString} ...");

        var services = new ServiceCollection();
        services.AddRedisNearCache(connectionString);
        await using var provider = services.BuildServiceProvider();
        var cache = provider.GetRequiredService<IRedisNearCache>();
        await cache.Ready;

        // A second, entirely separate multiplexer standing in for "some other client" writing to Redis.
        // AllowAdmin is only needed here for the demo's own INFO commandstats probe; RedisNearCache's private
        // connection is unrelated to this one.
        var plainOptions = ConfigurationOptions.Parse(connectionString);
        plainOptions.AllowAdmin = true;
        using var plainMultiplexer = await ConnectionMultiplexer.ConnectAsync(plainOptions);
        var plainDb = plainMultiplexer.GetDatabase();
        var server = plainMultiplexer.GetServer(plainMultiplexer.GetEndPoints()[0]);

        // Seed the key from outside the near cache, then read it once through the near cache (a miss: this is
        // the tracked GET that populates L1 and arms server-side invalidation for this key).
        await plainDb.StringSetAsync(key, "initial-value");
        var seeded = await cache.GetAsync<string>(key);
        Console.WriteLine($"Seeded '{key}' = '{seeded}' (first read through the near cache; this one is a miss).");

        var getCallsBefore = await GetCmdStatGetCallsAsync(server);

        for (var i = 0; i < repeatCount; i++)
        {
            await cache.GetAsync<string>(key);
        }

        var getCallsAfter = await GetCmdStatGetCallsAsync(server);
        var getCallDelta = getCallsAfter - getCallsBefore;

        Console.WriteLine();
        Console.WriteLine($"{repeatCount} reads of '{key}' through the near cache (all should be L1 hits):");
        Console.WriteLine($"  cmdstat_get calls before : {getCallsBefore}");
        Console.WriteLine($"  cmdstat_get calls after  : {getCallsAfter}");
        Console.WriteLine($"  delta (Redis GETs issued): {getCallDelta}  (expected 0)");

        // Now write the key from the *other* client and time how long it takes the near cache to notice.
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        await plainDb.StringSetAsync(key, "updated-value");

        var timeout = TimeSpan.FromSeconds(5);
        var spinWait = new SpinWait();
        while (cache.TryGetLocal<string>(key, out _) && stopwatch.Elapsed < timeout)
        {
            spinWait.SpinOnce();
        }

        stopwatch.Stop();
        var evicted = !cache.TryGetLocal<string>(key, out _);

        Console.WriteLine();
        if (evicted)
        {
            Console.WriteLine($"Write-to-eviction latency  : {stopwatch.Elapsed.TotalMicroseconds:F1} µs");
        }
        else
        {
            Console.WriteLine($"L1 entry was NOT evicted within the {timeout.TotalSeconds}s timeout.");
        }

        Console.WriteLine();
        Console.WriteLine($"Statistics: {cache.Statistics}");

        Console.WriteLine();
        Console.WriteLine("Summary");
        Console.WriteLine("-------");
        Console.WriteLine($"{"Metric",-32} | Value");
        Console.WriteLine(new string('-', 48));
        Console.WriteLine($"{"Redis GETs for 1000 L1 hits",-32} | {getCallDelta}");
        Console.WriteLine($"{"Write-to-eviction latency (µs)",-32} | {(evicted ? stopwatch.Elapsed.TotalMicroseconds.ToString("F1", CultureInfo.InvariantCulture) : "timed out")}");
        Console.WriteLine($"{"L1 hits",-32} | {cache.Statistics.Hits}");
        Console.WriteLine($"{"L1 misses",-32} | {cache.Statistics.Misses}");
        Console.WriteLine($"{"Invalidations received",-32} | {cache.Statistics.Invalidations}");
    }

    /// <summary>Reads the current <c>calls</c> counter for the GET command out of <c>INFO commandstats</c>.</summary>
    private static async Task<long> GetCmdStatGetCallsAsync(IServer server)
    {
        var groups = await server.InfoAsync("commandstats");
        foreach (var group in groups)
        {
            foreach (var entry in group)
            {
                if (entry.Key != "cmdstat_get")
                {
                    continue;
                }

                // Value looks like "calls=123,usec=456,usec_per_call=3.71,rejected_calls=0,failed_calls=0".
                var callsField = entry.Value.Split(',')[0];
                var callsValue = callsField.Split('=')[1];
                return long.Parse(callsValue, CultureInfo.InvariantCulture);
            }
        }

        return 0;
    }
}
