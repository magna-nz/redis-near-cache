using RedisNearCache.Bench.Micro;
using RedisNearCache.Bench.Micro.Comparisons;
using Sailfish;
using Sailfish.Execution;

namespace RedisNearCache.Bench.Sailfish;

/// <summary>
/// Sailfish per-call latency comparison, split into its own project/process because Sailfish requires
/// Perfolizer >= 0.7.1, which is binary-incompatible with the Perfolizer 0.6.1 that BenchmarkDotNet (in the sibling
/// RedisNearCache.Bench project) requires exactly -- forcing one Perfolizer version onto both broke whichever tool
/// didn't ask for it (verified directly: TypeLoadException/FileNotFoundException in BenchmarkDotNet's own internals
/// when forced onto 0.7.1).
/// </summary>
public static class Program
{
    public static async Task<int> Main(string[] args)
    {
        var quick = args.Contains("--quick");
        var endpoint = GetArg(args, "--endpoint") ?? "localhost:6379";
        var tlsCa = GetArg(args, "--tls-ca");
        var latencyLabel = GetArg(args, "--latency-label") ?? "0ms";
        var output = GetArg(args, "--output");
        if (output is null)
        {
            Console.Error.WriteLine("usage: RedisNearCache.Bench.Sailfish [--quick] --endpoint E --latency-label L --output DIR [--tls-ca P]");
            return 1;
        }

        Directory.CreateDirectory(output);

        // Published to the environment (not read from argv) so MicroConfig's single code path -- shared with
        // BenchmarkDotNet's out-of-process children in the sibling project -- works unchanged here.
        Environment.SetEnvironmentVariable(Payloads.EnvEndpoint, endpoint);
        Environment.SetEnvironmentVariable(Payloads.EnvTlsCa, tlsCa ?? "");
        Environment.SetEnvironmentVariable(MicroConfig.EnvQuick, quick ? "1" : "0");

        Console.WriteLine($"Sailfish: endpoint={endpoint} tls-ca={tlsCa ?? "(none)"} latency-label={latencyLabel} quick={quick} output={output}");

        // Hit and miss run separately (like BenchmarkDotNet's two jobs) because --quick needs a different sample
        // size/warmup override for each: the miss-path pool is sized to an exact sample count (see
        // MicroJobSettings.SailfishMissPoolSize), so a single WithGlobalSampleSize shared with the hit path would
        // either starve or oversize one of the two.
        var hitNames = quick
            ? new[] { nameof(HitComparisonString) }
            : new[] { nameof(HitComparisonString), nameof(HitComparisonJson) };
        var missNames = quick
            ? new[] { nameof(MissComparisonString) }
            : new[] { nameof(MissComparisonString), nameof(MissComparisonJson) };

        var hitBuilder = RunSettingsBuilder.CreateBuilder()
            .TestsFromAssembliesContaining(new[] { typeof(HitComparisonString) })
            .WithLocalOutputDirectory(output)
            .WithTestNames(hitNames)
            .WithSailDiff()
            .CreateTrackingFiles(false)
            .WithSeed(20260915); // fixed seed: deterministic run order
        if (quick)
        {
            hitBuilder = hitBuilder
                .WithGlobalSampleSize(MicroJobSettings.QuickSailfishHitMaximumSampleSize)
                .WithGlobalNumWarmupIterations(MicroJobSettings.QuickSailfishHitWarmupIterations);
        }

        var hitResult = await SailfishRunner.Run(hitBuilder.Build(), CancellationToken.None);
        EnsureValid(hitResult, "hit");

        var missBuilder = RunSettingsBuilder.CreateBuilder()
            .TestsFromAssembliesContaining(new[] { typeof(HitComparisonString) })
            .WithLocalOutputDirectory(output)
            .WithTestNames(missNames)
            .WithSailDiff()
            .CreateTrackingFiles(false)
            .WithSeed(20260916);
        if (quick)
        {
            missBuilder = missBuilder
                .WithGlobalSampleSize(MicroJobSettings.QuickSailfishMissSampleSize)
                .WithGlobalNumWarmupIterations(MicroJobSettings.QuickSailfishMissWarmupIterations);
        }

        var missResult = await SailfishRunner.Run(missBuilder.Build(), CancellationToken.None);
        EnsureValid(missResult, "miss");

        Console.WriteLine($"Sailfish run complete; output written to {output}");
        return 0;
    }

    private static string? GetArg(string[] args, string name)
    {
        var i = Array.IndexOf(args, name);
        return i >= 0 && i + 1 < args.Length ? args[i + 1] : null;
    }

    private static void EnsureValid(SailfishRunResult result, string label)
    {
        if (result.IsValid) return;
        foreach (var ex in result.Exceptions ?? Enumerable.Empty<Exception>()) Console.Error.WriteLine($"Sailfish {label} exception: {ex}");
        throw new InvalidOperationException($"Sailfish {label} run reported failures; see exceptions above.");
    }
}
