using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;
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
            sb.AppendLine("| Contender | Write mode | Reads/s | Local hit % | Local read p50 / p99 (µs) | Remote read p50 / p99 (µs) | Server cmds/s | Source loads / read | Errors | Stale reads (count, %) | Staleness age p50 / p99 / max (ms) | Stale local entries after quiescence |");
            sb.AppendLine("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|");

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
        var sourceLoadsPerRead = r.Reads > 0 ? FormatPercent((double)r.SourceLoads / r.Reads) : "n/a";
        var errors = FormatCount(r.ReadErrors + r.WriteErrors);

        return $"| {contender} | {mode} | {FormatRate(r.ReadsPerSecond)} | {localHit} | {localLatency} | {remoteLatency} | {FormatRate(r.ServerCommandsPerSecond)} | {sourceLoadsPerRead} | {errors} | {staleReads} | {stalenessAge} | {staleEntries} |";
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

    // --- micro-benchmark sections (BenchmarkDotNet, Sailfish) --------------------------------------------------
    //
    // BenchmarkDotNet: reads <resultsDir>/bdn/<label>/results/{HitBenchmarks,MissBenchmarks}-report-full.json
    // (JsonExporter.Full; schema verified directly against a real run: Benchmarks[].Method/Parameters/
    // Statistics.{Mean,Median,ConfidenceInterval.Margin}/Memory.BytesAllocatedPerOperation, all in nanoseconds).
    //
    // Sailfish: reads every <resultsDir>/sailfish/<label>/PerformanceResults_*.csv (Median/Mean/RawExecutionResults
    // in milliseconds; p95/p99 are not columns Sailfish emits for [SailfishMethod] results, so they are computed
    // here from RawExecutionResults) and every TestSession_*_Results_*.csv's "# Method Comparisons" section
    // (Method1/Method2/Ratio/CI95_Lower/CI95_Upper/q_value; Method1 is the Plain baseline because each Hit/Miss x
    // payload class has exactly one baseline test case -- see HitComparisonString's remarks). Both file sets are
    // globbed because a run makes two separate SailfishRunner.Run calls (hit classes, miss classes) into the same
    // --output directory, each producing its own timestamped files.

    private static readonly string[] MicroPathOrder = { "Hit", "Miss" };
    private static readonly string[] MicroPayloadOrder = { "String", "Json" };

    private sealed record BdnRow(string Path, string Contender, string Payload, double MeanNs, double ErrorNs, double MedianNs, long AllocatedBytes);

    private sealed record SailfishPerfRow(string Path, string Contender, string Payload, double MedianMs, double MeanMs, double P95Ms, double P99Ms);

    private sealed record SailfishComparisonRow(string Path, string Contender, string Payload, double Ratio, double CiLower, double CiUpper, double QValue);

    private static void WriteMicroBenchmarkSections(StringBuilder sb, string resultsDir)
    {
        var bdnRoot = Path.Combine(resultsDir, "bdn");
        var sailfishRoot = Path.Combine(resultsDir, "sailfish");

        var labels = OrderLatencyLabels(
            SubdirectoryNames(bdnRoot).Concat(SubdirectoryNames(sailfishRoot)).Distinct(StringComparer.Ordinal));

        if (labels.Count == 0)
        {
            return;
        }

        foreach (var label in labels)
        {
            var bdnRows = LoadBdnRows(Path.Combine(bdnRoot, label));
            var sailfishPerf = LoadSailfishPerfRows(Path.Combine(sailfishRoot, label));
            var sailfishComparisons = LoadSailfishComparisonRows(Path.Combine(sailfishRoot, label));

            WriteBdnMicroSection(sb, label, bdnRows);
            WriteSailfishMicroSection(sb, label, sailfishPerf, sailfishComparisons);
            WriteCrossCheckSection(sb, label, bdnRows, sailfishPerf);
        }
    }

    private static IEnumerable<string> SubdirectoryNames(string root) =>
        Directory.Exists(root)
            ? Directory.EnumerateDirectories(root).Select(d => Path.GetFileName(d.TrimEnd('/', '\\'))).Where(n => n is { Length: > 0 })!
            : Enumerable.Empty<string>();

    // --- BenchmarkDotNet parsing ---------------------------------------------------------------------------------

    private sealed class BdnReportFile
    {
        public List<BdnBenchmark>? Benchmarks { get; set; }
    }

    private sealed class BdnBenchmark
    {
        public string? Type { get; set; }
        public string? Method { get; set; }
        public BdnStatistics? Statistics { get; set; }
        public BdnMemory? Memory { get; set; }
    }

    private sealed class BdnStatistics
    {
        public double Mean { get; set; }
        public double Median { get; set; }
        public BdnConfidenceInterval? ConfidenceInterval { get; set; }
    }

    private sealed class BdnConfidenceInterval
    {
        // A pathologically small sample count (BenchmarkDotNet's own edge case, not expected in real jobs) can
        // serialize this as an empty string instead of a number; tolerate it rather than failing the whole file.
        [JsonConverter(typeof(LenientDoubleConverter))]
        public double Margin { get; set; }
    }

    /// <summary>Reads a JSON number normally; a non-numeric string (BenchmarkDotNet emits <c>""</c> for an
    /// undefined statistic from a too-small sample) becomes 0 instead of failing deserialization of the whole file.</summary>
    private sealed class LenientDoubleConverter : JsonConverter<double>
    {
        public override double Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) =>
            reader.TokenType == JsonTokenType.Number && reader.TryGetDouble(out var value) ? value : 0;

        public override void Write(Utf8JsonWriter writer, double value, JsonSerializerOptions options) => writer.WriteNumberValue(value);
    }

    private sealed class BdnMemory
    {
        public long BytesAllocatedPerOperation { get; set; }
    }

    // AllowNamedFloatingPointLiterals: a job with very few iterations can produce a NaN/Infinity confidence
    // interval margin, which BenchmarkDotNet's own JsonExporter still writes out (verified against a real run).
    private static readonly JsonSerializerOptions BdnJsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
        NumberHandling = JsonNumberHandling.AllowNamedFloatingPointLiterals,
    };

    private static List<BdnRow> LoadBdnRows(string labelDir)
    {
        var rows = new List<BdnRow>();
        if (!Directory.Exists(labelDir))
        {
            return rows;
        }

        var resultsDir = Path.Combine(labelDir, "results");
        if (!Directory.Exists(resultsDir))
        {
            return rows;
        }

        // HitBenchmarks<T>/MissBenchmarks<T> are generic over the payload ([GenericTypeArguments], not [Params]),
        // so BenchmarkDotNet names the exported file after the closed type, e.g.
        // "RedisNearCache.Bench.Micro.HitBenchmarks_String_-report-full.json" (verified against a real run) -- one
        // file per payload closure that ran (both under a full run; only String under --quick). Glob rather than a
        // fixed name for this reason.
        foreach (var (globPattern, path) in new[] { ("*HitBenchmarks*-report-full.json", "Hit"), ("*MissBenchmarks*-report-full.json", "Miss") })
        {
            foreach (var file in Directory.EnumerateFiles(resultsDir, globPattern))
            {
                BdnReportFile? report;
                try
                {
                    report = JsonSerializer.Deserialize<BdnReportFile>(File.ReadAllText(file), BdnJsonOptions);
                }
                catch (Exception ex)
                {
                    Console.Error.WriteLine($"warning: failed to parse {file}: {ex.Message}, skipping");
                    continue;
                }

                if (report?.Benchmarks is null)
                {
                    continue;
                }

                foreach (var b in report.Benchmarks)
                {
                    if (b.Method is null || b.Statistics is null || b.Memory is null)
                    {
                        continue;
                    }

                    var payload = ParseBdnPayload(b.Type);
                    if (payload is null)
                    {
                        continue;
                    }

                    rows.Add(new BdnRow(path, b.Method, payload, b.Statistics.Mean, b.Statistics.ConfidenceInterval?.Margin ?? 0,
                        b.Statistics.Median, b.Memory.BytesAllocatedPerOperation));
                }
            }
        }

        return rows;
    }

    /// <summary>
    /// The payload comes from the closed generic type argument in the <c>Type</c> field, e.g.
    /// <c>"HitBenchmarks&lt;String&gt;"</c> or <c>"HitBenchmarks&lt;SampleRecord&gt;"</c> (verified against a real
    /// run) -- not from <c>Parameters</c> (empty; these classes use <c>[GenericTypeArguments]</c>, not <c>[Params]</c>).
    /// <c>SampleRecord</c> is the JSON payload's CLR type name; mapped to the "Json" label used everywhere else in
    /// this report.
    /// </summary>
    private static string? ParseBdnPayload(string? type)
    {
        if (string.IsNullOrEmpty(type))
        {
            return null;
        }

        var lt = type.IndexOf('<');
        var gt = type.LastIndexOf('>');
        if (lt < 0 || gt < 0 || gt <= lt)
        {
            return null;
        }

        var arg = type[(lt + 1)..gt];
        return arg switch
        {
            "String" => "String",
            "SampleRecord" => "Json",
            _ => arg,
        };
    }

    private static void WriteBdnMicroSection(StringBuilder sb, string label, List<BdnRow> rows)
    {
        sb.AppendLine($"## Per-call cost — BenchmarkDotNet, {label}");
        sb.AppendLine();

        if (rows.Count == 0)
        {
            sb.AppendLine("_No BenchmarkDotNet results found._");
            sb.AppendLine();
            return;
        }

        foreach (var path in MicroPathOrder)
        {
            var pathRows = rows.Where(r => r.Path == path).ToList();
            sb.AppendLine(path == "Hit" ? "**Hit path**" : "**Miss path**");
            sb.AppendLine();
            if (pathRows.Count == 0)
            {
                sb.AppendLine("_No results._");
                sb.AppendLine();
                continue;
            }

            sb.AppendLine("| Contender | Payload | Mean | Error | Ratio vs Plain | Allocated |");
            sb.AppendLine("|---|---|---:|---:|---:|---:|");

            foreach (var payload in MicroPayloadOrder)
            {
                var group = pathRows.Where(r => r.Payload == payload).ToList();
                if (group.Count == 0)
                {
                    continue;
                }

                var baseline = group.FirstOrDefault(r => string.Equals(r.Contender, "Plain", StringComparison.OrdinalIgnoreCase));
                foreach (var contender in ContenderOrder)
                {
                    var r = group.FirstOrDefault(x => string.Equals(x.Contender, contender, StringComparison.OrdinalIgnoreCase));
                    if (r is null)
                    {
                        continue;
                    }

                    var ratio = baseline is not null && baseline.MeanNs > 0 ? (r.MeanNs / baseline.MeanNs).ToString("N3", CultureInfo.InvariantCulture) : "n/a";
                    sb.AppendLine($"| {r.Contender} | {r.Payload} | {FormatNanosAsMicros(r.MeanNs)} | {FormatNanosAsMicros(r.ErrorNs)} | {ratio} | {FormatBytes(r.AllocatedBytes)} |");
                }
            }

            sb.AppendLine();
        }
    }

    // --- Sailfish parsing ------------------------------------------------------------------------------------------

    private static readonly Regex SailfishDisplayNamePattern = new(@"^(?<class>\w+)\.(?<method>\w+)\(\)$", RegexOptions.Compiled);

    private static (string Path, string Payload)? ClassNameToPathPayload(string className) => className switch
    {
        "HitComparisonString" => ("Hit", "String"),
        "HitComparisonJson" => ("Hit", "Json"),
        "MissComparisonString" => ("Miss", "String"),
        "MissComparisonJson" => ("Miss", "Json"),
        _ => null,
    };

    private static List<SailfishPerfRow> LoadSailfishPerfRows(string labelDir)
    {
        var rows = new List<SailfishPerfRow>();
        if (!Directory.Exists(labelDir))
        {
            return rows;
        }

        foreach (var file in Directory.EnumerateFiles(labelDir, "PerformanceResults_*.csv"))
        {
            List<string> lines;
            try
            {
                lines = File.ReadAllLines(file).ToList();
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"warning: failed to read {file}: {ex.Message}, skipping");
                continue;
            }

            if (lines.Count < 2)
            {
                continue;
            }

            var header = SplitCsvLine(lines[0]);
            int Col(string name) => header.IndexOf(name);
            var displayNameCol = Col("DisplayName");
            var medianCol = Col("Median");
            var meanCol = Col("Mean");
            var rawCol = Col("RawExecutionResults");
            if (displayNameCol < 0 || medianCol < 0 || meanCol < 0 || rawCol < 0)
            {
                continue;
            }

            foreach (var line in lines.Skip(1))
            {
                if (line.Length == 0)
                {
                    continue;
                }

                var fields = SplitCsvLine(line);
                if (fields.Count <= Math.Max(rawCol, Math.Max(medianCol, Math.Max(meanCol, displayNameCol))))
                {
                    continue;
                }

                var match = SailfishDisplayNamePattern.Match(fields[displayNameCol]);
                if (!match.Success)
                {
                    continue;
                }

                var mapped = ClassNameToPathPayload(match.Groups["class"].Value);
                if (mapped is null)
                {
                    continue;
                }

                if (!double.TryParse(fields[medianCol], NumberStyles.Float, CultureInfo.InvariantCulture, out var median) ||
                    !double.TryParse(fields[meanCol], NumberStyles.Float, CultureInfo.InvariantCulture, out var mean))
                {
                    continue;
                }

                var raw = fields[rawCol]
                    .Split(',', StringSplitOptions.RemoveEmptyEntries)
                    .Select(s => double.TryParse(s, NumberStyles.Float, CultureInfo.InvariantCulture, out var v) ? v : (double?)null)
                    .Where(v => v.HasValue)
                    .Select(v => v!.Value)
                    .OrderBy(v => v)
                    .ToArray();

                var p95 = PercentileOfSorted(raw, 0.95);
                var p99 = PercentileOfSorted(raw, 0.99);

                rows.Add(new SailfishPerfRow(mapped.Value.Path, match.Groups["method"].Value, mapped.Value.Payload, median, mean, p95, p99));
            }
        }

        return rows;
    }

    private static List<SailfishComparisonRow> LoadSailfishComparisonRows(string labelDir)
    {
        var rows = new List<SailfishComparisonRow>();
        if (!Directory.Exists(labelDir))
        {
            return rows;
        }

        foreach (var file in Directory.EnumerateFiles(labelDir, "TestSession_*_Results_*.csv"))
        {
            string[] lines;
            try
            {
                lines = File.ReadAllLines(file);
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"warning: failed to read {file}: {ex.Message}, skipping");
                continue;
            }

            var headerIndex = Array.IndexOf(lines, "# Method Comparisons");
            if (headerIndex < 0 || headerIndex + 1 >= lines.Length)
            {
                continue;
            }

            var header = SplitCsvLine(lines[headerIndex + 1]);
            int Col(string name) => header.IndexOf(name);
            var groupCol = Col("ComparisonGroup");
            var method1Col = Col("Method1");
            var method2Col = Col("Method2");
            var ratioCol = Col("Ratio");
            var ciLowerCol = Col("CI95_Lower");
            var ciUpperCol = Col("CI95_Upper");
            var qCol = Col("q_value");
            if (groupCol < 0 || method1Col < 0 || method2Col < 0 || ratioCol < 0 || qCol < 0)
            {
                continue;
            }

            for (var i = headerIndex + 2; i < lines.Length; i++)
            {
                if (lines[i].Length == 0 || lines[i].StartsWith('#'))
                {
                    break;
                }

                var fields = SplitCsvLine(lines[i]);
                if (fields.Count <= qCol)
                {
                    continue;
                }

                // Method1 is the Plain baseline (see class-remarks on why there is exactly one baseline test case
                // per class); a comparison row for a non-baseline pair would mean the N x N fallback triggered, and
                // is skipped rather than misreported as "vs Plain".
                if (!string.Equals(fields[method1Col], "Plain", StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                var mapped = ClassNameToPathPayload(fields[groupCol]);
                if (mapped is null)
                {
                    continue;
                }

                if (!double.TryParse(fields[ratioCol], NumberStyles.Float, CultureInfo.InvariantCulture, out var ratio) ||
                    !double.TryParse(fields[qCol], NumberStyles.Float, CultureInfo.InvariantCulture, out var q))
                {
                    continue;
                }

                double.TryParse(ciLowerCol >= 0 && fields.Count > ciLowerCol ? fields[ciLowerCol] : "", NumberStyles.Float, CultureInfo.InvariantCulture, out var ciLower);
                double.TryParse(ciUpperCol >= 0 && fields.Count > ciUpperCol ? fields[ciUpperCol] : "", NumberStyles.Float, CultureInfo.InvariantCulture, out var ciUpper);

                rows.Add(new SailfishComparisonRow(mapped.Value.Path, fields[method2Col], mapped.Value.Payload, ratio, ciLower, ciUpper, q));
            }
        }

        return rows;
    }

    /// <summary>Splits one CSV line, honouring double-quoted fields that may contain commas (Sailfish's own CSV
    /// writer quotes <c>RawExecutionResults</c> this way).</summary>
    private static List<string> SplitCsvLine(string line)
    {
        var fields = new List<string>();
        var current = new StringBuilder();
        var inQuotes = false;
        for (var i = 0; i < line.Length; i++)
        {
            var c = line[i];
            if (inQuotes)
            {
                if (c == '"')
                {
                    if (i + 1 < line.Length && line[i + 1] == '"') { current.Append('"'); i++; }
                    else inQuotes = false;
                }
                else current.Append(c);
            }
            else if (c == '"')
            {
                inQuotes = true;
            }
            else if (c == ',')
            {
                fields.Add(current.ToString());
                current.Clear();
            }
            else
            {
                current.Append(c);
            }
        }

        fields.Add(current.ToString());
        return fields;
    }

    /// <summary>Nearest-rank percentile of an ascending-sorted array. Sailfish does not compute p95/p99 for
    /// <c>[SailfishMethod]</c> results (only <c>Mean</c>/<c>Median</c>/confidence intervals); this derives them from
    /// the raw per-sample data it does expose (<c>RawExecutionResults</c>).</summary>
    private static double PercentileOfSorted(double[] sorted, double p)
    {
        if (sorted.Length == 0)
        {
            return 0;
        }

        var rank = (int)Math.Ceiling(p * sorted.Length) - 1;
        return sorted[Math.Clamp(rank, 0, sorted.Length - 1)];
    }

    private static void WriteSailfishMicroSection(StringBuilder sb, string label, List<SailfishPerfRow> perf, List<SailfishComparisonRow> comparisons)
    {
        sb.AppendLine($"## Per-call latency — Sailfish, {label}");
        sb.AppendLine();
        sb.AppendLine("_p95/p99 are derived here from Sailfish's raw per-sample data (RawExecutionResults); Sailfish itself does not compute percentiles for `[SailfishMethod]` results, only mean/median/confidence intervals._");
        sb.AppendLine();

        if (perf.Count == 0)
        {
            sb.AppendLine("_No Sailfish results found._");
            sb.AppendLine();
            return;
        }

        foreach (var path in MicroPathOrder)
        {
            var pathPerf = perf.Where(r => r.Path == path).ToList();
            sb.AppendLine(path == "Hit" ? "**Hit path**" : "**Miss path**");
            sb.AppendLine();
            if (pathPerf.Count == 0)
            {
                sb.AppendLine("_No results._");
                sb.AppendLine();
                continue;
            }

            sb.AppendLine("| Contender | Payload | Median | Mean | p95 | p99 | Ratio vs Plain [95% CI] | q-value |");
            sb.AppendLine("|---|---|---:|---:|---:|---:|---:|---:|");

            foreach (var payload in MicroPayloadOrder)
            {
                foreach (var contender in ContenderOrder)
                {
                    var r = pathPerf.FirstOrDefault(x => x.Payload == payload && string.Equals(x.Contender, contender, StringComparison.OrdinalIgnoreCase));
                    if (r is null)
                    {
                        continue;
                    }

                    var cmp = comparisons.FirstOrDefault(x => x.Path == path && x.Payload == payload && string.Equals(x.Contender, contender, StringComparison.OrdinalIgnoreCase));
                    var ratio = string.Equals(contender, "Plain", StringComparison.OrdinalIgnoreCase)
                        ? "1.000 (baseline)"
                        : cmp is not null
                            ? $"{cmp.Ratio.ToString("N3", CultureInfo.InvariantCulture)} [{cmp.CiLower.ToString("N3", CultureInfo.InvariantCulture)}, {cmp.CiUpper.ToString("N3", CultureInfo.InvariantCulture)}]"
                            : "n/a";
                    var qValue = string.Equals(contender, "Plain", StringComparison.OrdinalIgnoreCase) ? "n/a" : cmp is not null ? FormatQValue(cmp.QValue) : "n/a";

                    sb.AppendLine($"| {r.Contender} | {r.Payload} | {FormatMillisAsMicros(r.MedianMs)} | {FormatMillisAsMicros(r.MeanMs)} | {FormatMillisAsMicros(r.P95Ms)} | {FormatMillisAsMicros(r.P99Ms)} | {ratio} | {qValue} |");
                }
            }

            sb.AppendLine();
        }
    }

    private static void WriteCrossCheckSection(StringBuilder sb, string label, List<BdnRow> bdnRows, List<SailfishPerfRow> sailfishRows)
    {
        sb.AppendLine($"## Cross-check: BenchmarkDotNet vs Sailfish — {label}");
        sb.AppendLine();

        if (bdnRows.Count == 0 || sailfishRows.Count == 0)
        {
            sb.AppendLine("_Need both a BenchmarkDotNet and a Sailfish run for this latency point; at least one is missing._");
            sb.AppendLine();
            return;
        }

        sb.AppendLine("| Path | Contender | Payload | BDN median | Sailfish median | Difference |");
        sb.AppendLine("|---|---|---|---:|---:|---:|");

        foreach (var path in MicroPathOrder)
        {
            foreach (var payload in MicroPayloadOrder)
            {
                foreach (var contender in ContenderOrder)
                {
                    var b = bdnRows.FirstOrDefault(x => x.Path == path && x.Payload == payload && string.Equals(x.Contender, contender, StringComparison.OrdinalIgnoreCase));
                    var s = sailfishRows.FirstOrDefault(x => x.Path == path && x.Payload == payload && string.Equals(x.Contender, contender, StringComparison.OrdinalIgnoreCase));
                    if (b is null || s is null)
                    {
                        continue;
                    }

                    var bdnMedianUs = b.MedianNs / 1000.0;
                    var sailfishMedianUs = s.MedianMs * 1000.0;
                    var diffPct = bdnMedianUs == 0 ? 0 : (sailfishMedianUs - bdnMedianUs) / bdnMedianUs * 100.0;

                    sb.AppendLine($"| {path} | {contender} | {payload} | {FormatMicrosValue(bdnMedianUs)} | {FormatMicrosValue(sailfishMedianUs)} | {diffPct.ToString("N1", CultureInfo.InvariantCulture)} % |");
                }
            }
        }

        sb.AppendLine();
    }

    private static string FormatNanosAsMicros(double nanos) => FormatMicrosValue(nanos / 1000.0);

    private static string FormatMillisAsMicros(double millis) => FormatMicrosValue(millis * 1000.0);

    private static string FormatMicrosValue(double micros) => $"{FormatMicros(micros)} µs";

    private static string FormatBytes(long bytes) => bytes >= 1024
        ? (bytes / 1024.0).ToString("N2", CultureInfo.InvariantCulture) + " KB"
        : bytes.ToString("N0", CultureInfo.InvariantCulture) + " B";

    private static string FormatQValue(double q) => q < 0.001
        ? q.ToString("E1", CultureInfo.InvariantCulture)
        : q.ToString("N3", CultureInfo.InvariantCulture);

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
