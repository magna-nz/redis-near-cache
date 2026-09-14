using System.Globalization;
using System.Text;
using System.Text.Json;
using RedisNearCache.Bench.Results;

namespace RedisNearCache.Bench.Report;

/// <summary>
/// Reads every <c>load/*.json</c> file (schema: <see cref="LoadResult"/>) under a results directory produced by
/// <c>bench/run-matrix.sh</c> and renders <c>summary.md</c> in that same directory.
/// </summary>
internal static class Program
{
    // Row order requested for the contender-comparison and internals tables. Deliberately NOT the
    // ContenderKind enum declaration order (which has NearCache before NearCacheHybridCache).
    private static readonly string[] ContenderOrder =
    {
        "Plain", "MemoryCacheTtl", "HybridCache", "FusionCache", "NearCacheHybridCache", "NearCache",
    };

    private static readonly string[] WriteModeOrder = { "Foreign", "Api" };

    private static readonly string[] PreferredLatencyOrder = { "0ms", "0.5ms", "2ms" };

    private static int Main(string[] args)
    {
        if (args.Length < 1)
        {
            Console.Error.WriteLine("usage: RedisNearCache.Bench.Report <results-dir>");
            return 1;
        }

        var resultsDir = args[0];
        if (!Directory.Exists(resultsDir))
        {
            Console.Error.WriteLine($"results directory not found: {resultsDir}");
            return 1;
        }

        var results = LoadResults(resultsDir);

        var sb = new StringBuilder();
        sb.AppendLine($"# Benchmark results — {Path.GetFileName(resultsDir.TrimEnd('/', '\\'))}");
        sb.AppendLine();

        WriteEnvironmentSection(sb, resultsDir, results);
        WriteContenderComparisonSection(sb, results);
        WriteLatencySweepSection(sb, results);
        WriteTopologiesSection(sb, results);
        WriteChaosSection(sb, results);
        WriteInternalsSection(sb, results);
        WriteMicroBenchmarkSections(sb, resultsDir);

        var summaryPath = Path.Combine(resultsDir, "summary.md");
        File.WriteAllText(summaryPath, sb.ToString());
        Console.WriteLine(summaryPath);
        return 0;
    }

    // --- loading -----------------------------------------------------------------------------------------

    private static List<LoadResult> LoadResults(string resultsDir)
    {
        var loadDir = Path.Combine(resultsDir, "load");
        var results = new List<LoadResult>();
        if (!Directory.Exists(loadDir))
        {
            return results;
        }

        foreach (var file in Directory.EnumerateFiles(loadDir, "*.json").OrderBy(f => f, StringComparer.Ordinal))
        {
            try
            {
                var json = File.ReadAllText(file);
                var result = JsonSerializer.Deserialize<LoadResult>(json, LoadResult.JsonOptions);
                if (result is null)
                {
                    Console.Error.WriteLine($"warning: {file} deserialized to null, skipping");
                    continue;
                }

                results.Add(result);
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"warning: failed to parse {file}: {ex.Message}, skipping");
            }
        }

        return results;
    }

    // --- sections ------------------------------------------------------------------------------------------

    private static void WriteEnvironmentSection(StringBuilder sb, string resultsDir, List<LoadResult> results)
    {
        sb.AppendLine("## Run environment");
        sb.AppendLine();

        var envPath = Path.Combine(resultsDir, "environment.txt");
        if (File.Exists(envPath))
        {
            sb.AppendLine("```");
            sb.AppendLine(File.ReadAllText(envPath).TrimEnd());
            sb.AppendLine("```");
            sb.AppendLine();
        }

        var profile = results.FirstOrDefault();
        if (profile is null)
        {
            sb.AppendLine("_No load results found; profile unknown._");
            sb.AppendLine();
            return;
        }

        sb.AppendLine("Profile (from results):");
        sb.AppendLine();
        sb.AppendLine("| Setting | Value |");
        sb.AppendLine("|---|---:|");
        sb.AppendLine($"| Instances | {profile.Instances} |");
        sb.AppendLine($"| Readers per instance | {profile.ReadersPerInstance} |");
        sb.AppendLine($"| Writers | {profile.Writers} |");
        sb.AppendLine($"| Target writes/s | {FormatCount(profile.TargetWritesPerSecond)} |");
        sb.AppendLine($"| Keys | {FormatCount(profile.Keys)} |");
        sb.AppendLine($"| Hot keys | {FormatCount(profile.HotKeys)} |");
        sb.AppendLine($"| Hot fraction | {FormatPercent(profile.HotFraction)} |");
        sb.AppendLine($"| Value bytes | {FormatCount(profile.ValueBytes)} |");
        sb.AppendLine($"| TTL (s) | {profile.TtlSeconds.ToString("N1", CultureInfo.InvariantCulture)} |");
        sb.AppendLine($"| Duration (s) | {profile.DurationSeconds.ToString("N0", CultureInfo.InvariantCulture)} |");
        sb.AppendLine();
    }

    private static void WriteContenderComparisonSection(StringBuilder sb, List<LoadResult> results)
    {
        var standalone = results.Where(r => IsStandalone(r) && !r.Chaos).ToList();
        var labels = OrderLatencyLabels(standalone.Select(r => r.LatencyLabel).Distinct());

        if (labels.Count == 0)
        {
            sb.AppendLine("## Contender comparison — standalone");
            sb.AppendLine();
            sb.AppendLine("_No standalone results found._");
            sb.AppendLine();
            return;
        }

        foreach (var label in labels)
        {
            sb.AppendLine($"## Contender comparison — standalone, {label}");
            sb.AppendLine();
            sb.AppendLine("| Contender | Write mode | Reads/s | Local hit % | Local read p50 / p99 (µs) | Remote read p50 / p99 (µs) | Server cmds/s | Stale reads (count, %) | Staleness age p50 / p99 / max (ms) | Stale local entries after quiescence |");
            sb.AppendLine("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|");

            foreach (var contender in ContenderOrder)
            {
                foreach (var mode in WriteModeOrder)
                {
                    var r = standalone.FirstOrDefault(x =>
                        string.Equals(x.Contender, contender, StringComparison.OrdinalIgnoreCase) &&
                        string.Equals(x.WriteMode, mode, StringComparison.OrdinalIgnoreCase) &&
                        x.LatencyLabel == label);
                    if (r is null)
                    {
                        continue;
                    }

                    sb.AppendLine(FormatContenderRow(contender, mode, r));
                }
            }

            sb.AppendLine();
        }
    }

    private static string FormatContenderRow(string contender, string mode, LoadResult r)
    {
        var localHit = r.LocalHitRatio.HasValue ? FormatPercent(r.LocalHitRatio.Value) : "n/a";
        var localLatency = r.LocalReadLatencyMicros is { } l ? $"{FormatMicros(l.P50)} / {FormatMicros(l.P99)}" : "n/a";
        var remoteLatency = $"{FormatMicros(r.RemoteReadLatencyMicros.P50)} / {FormatMicros(r.RemoteReadLatencyMicros.P99)}";
        var staleReads = $"{FormatCount(r.StaleReads)} ({FormatStalePercent(r.StaleReadFraction)})";
        var stalenessAge = r.StalenessAgeMillis is { } a ? $"{FormatMillis(a.P50)} / {FormatMillis(a.P99)} / {FormatMillis(a.Max)}" : "n/a";
        var staleEntries = r.StaleEntriesAfterQuiescence.HasValue ? FormatCount(r.StaleEntriesAfterQuiescence.Value) : "n/a";

        return $"| {contender} | {mode} | {FormatRate(r.ReadsPerSecond)} | {localHit} | {localLatency} | {remoteLatency} | {FormatRate(r.ServerCommandsPerSecond)} | {staleReads} | {stalenessAge} | {staleEntries} |";
    }

    private static void WriteLatencySweepSection(StringBuilder sb, List<LoadResult> results)
    {
        sb.AppendLine("## Latency sweep");
        sb.AppendLine();

        var foreign = results.Where(r => IsStandalone(r) && !r.Chaos &&
            string.Equals(r.WriteMode, "Foreign", StringComparison.OrdinalIgnoreCase)).ToList();
        var labels = OrderLatencyLabels(foreign.Select(r => r.LatencyLabel).Distinct());

        if (labels.Count == 0)
        {
            sb.AppendLine("_No standalone foreign-mode results found._");
            sb.AppendLine();
            return;
        }

        var header = new StringBuilder("| Contender |");
        var separator = new StringBuilder("|---|");
        foreach (var label in labels)
        {
            header.Append($" {label} Reads/s | {label} Server cmds/s |");
            separator.Append("---:|---:|");
        }

        sb.AppendLine(header.ToString());
        sb.AppendLine(separator.ToString());

        foreach (var contender in ContenderOrder)
        {
            var row = new StringBuilder($"| {contender} |");
            var any = false;
            foreach (var label in labels)
            {
                var r = foreign.FirstOrDefault(x =>
                    string.Equals(x.Contender, contender, StringComparison.OrdinalIgnoreCase) && x.LatencyLabel == label);
                if (r is not null)
                {
                    any = true;
                    row.Append($" {FormatRate(r.ReadsPerSecond)} | {FormatRate(r.ServerCommandsPerSecond)} |");
                }
                else
                {
                    row.Append(" n/a | n/a |");
                }
            }

            if (any)
            {
                sb.AppendLine(row.ToString());
            }
        }

        sb.AppendLine();
    }

    private static void WriteTopologiesSection(StringBuilder sb, List<LoadResult> results)
    {
        sb.AppendLine("## Topologies");
        sb.AppendLine();

        var topologies = results.Where(r => !IsStandalone(r) && !r.Chaos).ToList();
        if (topologies.Count == 0)
        {
            sb.AppendLine("_No non-standalone topology results found._");
            sb.AppendLine();
            return;
        }

        sb.AppendLine("| Topology | Server version | Contender | Reads/s | Local hit % | Stale reads | Stale entries |");
        sb.AppendLine("|---|---|---|---:|---:|---:|---:|");

        foreach (var r in topologies
                     .OrderBy(r => r.Topology, StringComparer.Ordinal)
                     .ThenBy(r => Array.IndexOf(ContenderOrder, r.Contender)))
        {
            var localHit = r.LocalHitRatio.HasValue ? FormatPercent(r.LocalHitRatio.Value) : "n/a";
            var staleEntries = r.StaleEntriesAfterQuiescence.HasValue ? FormatCount(r.StaleEntriesAfterQuiescence.Value) : "n/a";
            sb.AppendLine($"| {r.Topology} | {r.ServerVersion} | {r.Contender} | {FormatRate(r.ReadsPerSecond)} | {localHit} | {FormatCount(r.StaleReads)} | {staleEntries} |");
        }

        sb.AppendLine();
    }

    private static void WriteChaosSection(StringBuilder sb, List<LoadResult> results)
    {
        sb.AppendLine("## Chaos");
        sb.AppendLine();

        var chaos = results.Where(r => r.Chaos).ToList();
        if (chaos.Count == 0)
        {
            sb.AppendLine("_No chaos results found._");
            sb.AppendLine();
            return;
        }

        sb.AppendLine("| Contender | Write mode | Topology | Latency | Invalidations | Race discards | Flushes | Re-arms | Tracking keys | Stale entries after quiescence |");
        sb.AppendLine("|---|---|---|---|---:|---:|---:|---:|---:|---:|");

        foreach (var r in chaos)
        {
            sb.AppendLine($"| {r.Contender} | {r.WriteMode} | {r.Topology} | {r.LatencyLabel} | {FormatNullableCount(r.Invalidations)} | {FormatNullableCount(r.RaceDiscards)} | {FormatNullableCount(r.Flushes)} | {FormatNullableCount(r.Rearms)} | {FormatNullableCount(r.TrackingKeys)} | {FormatNullableCount(r.StaleEntriesAfterQuiescence)} |");
        }

        sb.AppendLine();
    }

    private static void WriteInternalsSection(StringBuilder sb, List<LoadResult> results)
    {
        sb.AppendLine("## RedisNearCache internals");
        sb.AppendLine();

        var internals = results.Where(r => IsStandalone(r) && !r.Chaos &&
            (string.Equals(r.Contender, "NearCache", StringComparison.OrdinalIgnoreCase) ||
             string.Equals(r.Contender, "NearCacheHybridCache", StringComparison.OrdinalIgnoreCase))).ToList();

        if (internals.Count == 0)
        {
            sb.AppendLine("_No RedisNearCache standalone results found._");
            sb.AppendLine();
            return;
        }

        sb.AppendLine("| Contender | Write mode | Latency | Invalidations | Invalidations/write | Race discards | Flushes | Re-arms | Tracking keys | Server used mem before | Server used mem after |");
        sb.AppendLine("|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|");

        var latencyRank = OrderLatencyLabels(internals.Select(r => r.LatencyLabel).Distinct())
            .Select((label, index) => (label, index))
            .ToDictionary(x => x.label, x => x.index, StringComparer.Ordinal);

        foreach (var r in internals
                     .OrderBy(r => Array.IndexOf(ContenderOrder, r.Contender))
                     .ThenBy(r => Array.IndexOf(WriteModeOrder, r.WriteMode))
                     .ThenBy(r => latencyRank[r.LatencyLabel]))
        {
            var perWrite = r.Invalidations.HasValue && r.Writes > 0
                ? ((double)r.Invalidations.Value / r.Writes).ToString("N2", CultureInfo.InvariantCulture)
                : "n/a";
            sb.AppendLine($"| {r.Contender} | {r.WriteMode} | {r.LatencyLabel} | {FormatNullableCount(r.Invalidations)} | {perWrite} | {FormatNullableCount(r.RaceDiscards)} | {FormatNullableCount(r.Flushes)} | {FormatNullableCount(r.Rearms)} | {FormatNullableCount(r.TrackingKeys)} | {FormatMegabytes(r.ServerUsedMemoryBytesBefore)} | {FormatMegabytes(r.ServerUsedMemoryBytesAfter)} |");
        }

        sb.AppendLine();
    }

    // Micro-benchmark sections (BenchmarkDotNet, Sailfish) are added here. A later agent wires these up to
    // read `bdn/<latency>/` and `sailfish/<latency>/` subfolders under resultsDir; this stub renders nothing.
    private static void WriteMicroBenchmarkSections(StringBuilder sb, string resultsDir)
    {
    }

    // --- helpers -------------------------------------------------------------------------------------------

    private static bool IsStandalone(LoadResult r) => string.Equals(r.Topology, "standalone", StringComparison.OrdinalIgnoreCase);

    private static List<string> OrderLatencyLabels(IEnumerable<string> labels)
    {
        var remaining = labels.ToHashSet(StringComparer.Ordinal);
        var ordered = new List<string>();
        foreach (var preferred in PreferredLatencyOrder)
        {
            if (remaining.Remove(preferred))
            {
                ordered.Add(preferred);
            }
        }

        ordered.AddRange(remaining.OrderBy(x => x, StringComparer.Ordinal));
        return ordered;
    }

    private static string FormatCount(long v) => v.ToString("N0", CultureInfo.InvariantCulture);

    private static string FormatNullableCount(long? v) => v.HasValue ? FormatCount(v.Value) : "n/a";

    private static string FormatRate(double v) => v.ToString("N0", CultureInfo.InvariantCulture);

    private static string FormatMicros(double v) => v < 10
        ? v.ToString("N1", CultureInfo.InvariantCulture)
        : v.ToString("N0", CultureInfo.InvariantCulture);

    private static string FormatMillis(double v) => v.ToString("N1", CultureInfo.InvariantCulture);

    private static string FormatMegabytes(long bytes) => (bytes / 1024.0 / 1024.0).ToString("N1", CultureInfo.InvariantCulture) + " MB";

    /// <summary>Percentage with 2 dp, e.g. local hit ratio. <paramref name="fraction"/> is 0..1.</summary>
    private static string FormatPercent(double fraction) => (fraction * 100).ToString("N2", CultureInfo.InvariantCulture) + " %";

    /// <summary>
    /// Like <see cref="FormatPercent"/> but a tiny nonzero fraction (rounds to 0.00 at 2 dp) prints as "&lt;0.01 %"
    /// instead of "0.00 %", so a handful of stale reads out of millions doesn't read as "no stale reads".
    /// </summary>
    private static string FormatStalePercent(double fraction)
    {
        var percent = fraction * 100;
        if (percent == 0)
        {
            return "0.00 %";
        }

        var rounded = Math.Round(percent, 2, MidpointRounding.AwayFromZero);
        return rounded == 0 ? "<0.01 %" : rounded.ToString("N2", CultureInfo.InvariantCulture) + " %";
    }
}
