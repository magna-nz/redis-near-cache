using System.Text.Json;
using System.Text.Json.Serialization;

namespace RedisNearCache.Bench.Results;

/// <summary>Latency or age percentiles. Units are in the owning property's name.</summary>
public sealed record Percentiles(double P50, double P99, double P999, double Max);

/// <summary>
/// One load-test run, written by <c>--load --json &lt;path&gt;</c> and read by the report renderer. Schema version 1.
/// Nullable properties are null when they do not apply to the contender (e.g. invalidation counters on non-RedisNearCache kinds).
/// </summary>
public sealed record LoadResult
{
    public int SchemaVersion { get; init; } = 1;

    // --- what ran ------------------------------------------------------------------------------------
    public required string Contender { get; init; }            // ContenderKind name
    public required string WriteMode { get; init; }            // WriteMode name
    public required string Topology { get; init; }             // free label: "standalone", "cluster", "tls", ...
    public required string LatencyLabel { get; init; }         // free label: "0ms", "1ms", ...
    public required string ServerVersion { get; init; }        // e.g. "redis 7.4.2" or "valkey 8.1.1", from INFO server
    public required string Endpoint { get; init; }             // with any password stripped
    public required bool Chaos { get; init; }
    public required DateTimeOffset StartedAt { get; init; }
    public required string Machine { get; init; }              // e.g. "Apple M4 Pro, 14 cores, .NET 10.0.11"

    // --- profile -------------------------------------------------------------------------------------
    public required int Instances { get; init; }
    public required int ReadersPerInstance { get; init; }
    public required int Writers { get; init; }
    public required int TargetWritesPerSecond { get; init; }
    public required int Keys { get; init; }
    public required int HotKeys { get; init; }
    public required double HotFraction { get; init; }
    public required int ValueBytes { get; init; }
    public required double TtlSeconds { get; init; }
    public required double DurationSeconds { get; init; }
    /// <summary>Local-tier entry limit given to every kind (ContenderSettings.LocalSizeLimit).</summary>
    public long LocalSizeLimit { get; init; }

    // --- client side ---------------------------------------------------------------------------------
    public required long Reads { get; init; }
    public required double ReadsPerSecond { get; init; }
    /// <summary>Reads whose ValueTask completed synchronously. Null when the contender cannot be classified this way.</summary>
    public long? LocalReads { get; init; }
    public double? LocalHitRatio { get; init; }
    /// <summary>Latency of reads classified local. Null when not classifiable or none occurred.</summary>
    public Percentiles? LocalReadLatencyMicros { get; init; }
    /// <summary>Latency of every other read (all reads when not classifiable).</summary>
    public required Percentiles RemoteReadLatencyMicros { get; init; }
    public required long SourceLoads { get; init; }
    public required long Writes { get; init; }
    public required double WritesPerSecond { get; init; }
    /// <summary>Reads that threw, or returned null / an unparseable value. Not included in Reads.</summary>
    public long ReadErrors { get; init; }
    /// <summary>Writes that threw (not registered with the staleness probe, not included in Writes).</summary>
    public long WriteErrors { get; init; }

    // --- staleness -----------------------------------------------------------------------------------
    /// <summary>
    /// Reads that returned version v although a newer version of that key had been acknowledged by Redis before the read
    /// started. A read racing a write (write acknowledged after the read started) is never counted.
    /// </summary>
    public required long StaleReads { get; init; }
    public required double StaleReadFraction { get; init; }
    /// <summary>For stale reads: read completion time minus the acknowledgement time of the oldest version newer than the one returned. Null when StaleReads is 0.</summary>
    public Percentiles? StalenessAgeMillis { get; init; }
    /// <summary>Post-run audit of local tiers against Redis after quiescence. Null when the contender exposes no local-only read.</summary>
    public long? LocalEntriesAudited { get; init; }
    public long? StaleEntriesAfterQuiescence { get; init; }

    // --- server side ---------------------------------------------------------------------------------
    public required long ServerGetCommands { get; init; }
    public required long ServerTotalCommands { get; init; }
    public required double ServerCommandsPerSecond { get; init; }
    public required double ServerNetOutputMegabytes { get; init; }
    public required long ServerUsedMemoryBytesBefore { get; init; }
    public required long ServerUsedMemoryBytesAfter { get; init; }

    // --- RedisNearCache kinds only ---------------------------------------------------------------------
    public long? Invalidations { get; init; }
    public long? RaceDiscards { get; init; }
    public long? Flushes { get; init; }
    public long? Rearms { get; init; }
    public long? TrackingKeys { get; init; }

    public static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true,
        DefaultIgnoreCondition = JsonIgnoreCondition.Never,
    };
}
