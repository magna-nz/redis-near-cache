using System.Diagnostics;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using RedisNearCache.Bench.Contenders;
using RedisNearCache.Bench.Results;
using RedisNearCache.Internal;
using StackExchange.Redis;

namespace RedisNearCache.Bench;

/// <summary>
/// Production-shaped load test: many application instances of one caching strategy (<c>--contender</c>), many concurrent
/// readers per instance, and writers producing a steady stream of changes, either foreign plain <c>SET</c>s or writes
/// through the contender's own API (<c>--write-mode</c>). Every read is checked against a per-key version registry to
/// count stale reads and measure how stale they were; the run ends with an audit of every instance's local tier against
/// Redis after quiescence. <c>--baseline</c> is an alias for <c>--contender Plain</c>. <c>--chaos</c> kills a quarter of the
/// RedisNearCache instances' connections halfway through. <c>--json</c> writes a <see cref="LoadResult"/>.
/// </summary>
internal static class LoadTest
{
    private sealed class Options
    {
        public string Endpoint = "localhost:6379";
        public int Instances = 20;
        public int ReadersPerInstance = 8;
        public int Writers = 4;
        public int WritesPerSecond = 2000;
        public int Keys = 10_000;
        public int HotKeys = 500;
        public double HotFraction = 0.8;
        public int Seconds = 30;
        public int ValueBytes = 512;
        public bool Chaos;
        public ContenderKind Contender = ContenderKind.NearCache;
        public WriteMode WriteMode = WriteMode.Foreign;
        public double TtlSeconds = 10;
        public string Topology = "standalone";
        public string LatencyLabel = "0ms";
        public string? TlsCa;
        public string? JsonPath;

        public static Options Parse(string[] args)
        {
            var o = new Options();
            for (var i = 0; i < args.Length; i++)
            {
                string Next() => args[++i];
                switch (args[i])
                {
                    case "--endpoint": o.Endpoint = Next(); break;
                    case "--instances": o.Instances = int.Parse(Next(), CultureInfo.InvariantCulture); break;
                    case "--readers": o.ReadersPerInstance = int.Parse(Next(), CultureInfo.InvariantCulture); break;
                    case "--writers": o.Writers = int.Parse(Next(), CultureInfo.InvariantCulture); break;
                    case "--writes-per-sec": o.WritesPerSecond = int.Parse(Next(), CultureInfo.InvariantCulture); break;
                    case "--keys": o.Keys = int.Parse(Next(), CultureInfo.InvariantCulture); break;
                    case "--hot-keys": o.HotKeys = int.Parse(Next(), CultureInfo.InvariantCulture); break;
                    case "--hot-fraction": o.HotFraction = double.Parse(Next(), CultureInfo.InvariantCulture); break;
                    case "--seconds": o.Seconds = int.Parse(Next(), CultureInfo.InvariantCulture); break;
                    case "--value-bytes": o.ValueBytes = int.Parse(Next(), CultureInfo.InvariantCulture); break;
                    case "--baseline": o.Contender = ContenderKind.Plain; break;
                    case "--chaos": o.Chaos = true; break;
                    case "--contender": o.Contender = Enum.Parse<ContenderKind>(Next(), ignoreCase: true); break;
                    case "--write-mode": o.WriteMode = Enum.Parse<WriteMode>(Next(), ignoreCase: true); break;
                    case "--ttl-seconds": o.TtlSeconds = double.Parse(Next(), CultureInfo.InvariantCulture); break;
                    case "--topology": o.Topology = Next(); break;
                    case "--latency-label": o.LatencyLabel = Next(); break;
                    case "--tls-ca": o.TlsCa = Next(); break;
                    case "--json": o.JsonPath = Next(); break;
                }
            }
            return o;
        }
    }

    /// <summary>
    /// Log-scale histogram: bucket i covers [1.05^i ns, 1.05^(i+1) ns), so a percentile is within about ±2.5 % of the true
    /// value (600 buckets reach ~5 minutes), plus the exact maximum. Written by one reader, merged at the end.
    /// </summary>
    private sealed class Histogram
    {
        private const double Base = 1.05;
        private static readonly double LogBase = Math.Log(Base);
        private readonly long[] _buckets = new long[600];
        public long Count;
        public long Max;

        public void Record(long nanos)
        {
            var idx = nanos <= 1 ? 0 : (int)Math.Min(_buckets.Length - 1, Math.Log(nanos) / LogBase);
            _buckets[idx]++;
            Count++;
            if (nanos > Max) Max = nanos;
        }

        public void MergeFrom(Histogram other)
        {
            for (var i = 0; i < _buckets.Length; i++) _buckets[i] += other._buckets[i];
            Count += other.Count;
            if (other.Max > Max) Max = other.Max;
        }

        public double Percentile(double p)
        {
            if (Count == 0) return 0;
            var target = (long)Math.Ceiling(Count * p);
            long seen = 0;
            for (var i = 0; i < _buckets.Length; i++)
            {
                seen += _buckets[i];
                // A bucket midpoint can exceed the largest value actually recorded; never report more than Max.
                if (seen >= target) return Math.Min(Math.Pow(Base, i + 0.5), Max);
            }
            return Max;
        }

        /// <summary>Percentiles in nanoseconds divided by <paramref name="divisor"/> (1e3 for µs, 1e6 for ms).</summary>
        public Percentiles ToPercentiles(double divisor) =>
            new(Percentile(0.50) / divisor, Percentile(0.99) / divisor, Percentile(0.999) / divisor, Max / divisor);

        public string Describe(double divisor, string unit) =>
            $"{Percentile(0.50) / divisor:F1} / {Percentile(0.99) / divisor:F1} / {Percentile(0.999) / divisor:F1} / max {Max / divisor:F1} {unit}  (n={Count:N0})";
    }

    /// <summary>One reader's counters and histograms. Fields are written only by that reader; the sampler reads the longs.</summary>
    private sealed class ReaderStats
    {
        public readonly Histogram Local = new();
        public readonly Histogram Remote = new();
        public readonly Histogram StaleAge = new();
        public long Reads;
        public long LocalReads;
        public long StaleReads;
        public long Errors;
    }

    private sealed class Instance
    {
        public required IContender Contender;
        public readonly List<ReaderStats> Readers = new();
    }

    /// <summary>
    /// Per key, the <see cref="Stopwatch"/> timestamp at which the writer saw each version acknowledged, indexed by version
    /// (0 = not acknowledged: not yet written, or the write threw). Versions per key are consecutive and a key has exactly
    /// one writer that never has two writes to it in flight, so acknowledgement times ascend with versions. Every version
    /// is kept for the whole run: a local tier can serve a value dozens of versions old (a hot key sees several writes a
    /// second against a TTL of seconds), so a bounded window would truncate staleness ages.
    /// <para>
    /// The owning writer is the only thread that writes a key's array. It stores into the array readers already see, and
    /// when the array is full it copies into a larger one, stores there, and only then publishes the new reference. A
    /// reader holding the old reference misses the newest acknowledgements, which can only undercount.
    /// </para>
    /// </summary>
    private sealed class VersionRegistry
    {
        private readonly long[][] _acksPerKey;
        private readonly long[] _latestAcked;

        public VersionRegistry(int keys, long seedTimestamp)
        {
            _acksPerKey = new long[keys][];
            _latestAcked = new long[keys];
            for (var i = 0; i < keys; i++)
            {
                _acksPerKey[i] = new long[64];
                _acksPerKey[i][0] = seedTimestamp;
            }
        }

        /// <summary>
        /// The highest acknowledged version, then the ack array. Read in that order: the writer publishes the array before
        /// the version, so the array read second always covers index <c>latest</c>.
        /// </summary>
        public (long Latest, long[] Acks) Read(int keyIndex)
        {
            var latest = Volatile.Read(ref _latestAcked[keyIndex]);
            return (latest, Volatile.Read(ref _acksPerKey[keyIndex]));
        }

        /// <summary>Called only by the key's single writer.</summary>
        public void Record(int keyIndex, long version, long ackTimestamp)
        {
            var acks = _acksPerKey[keyIndex];
            if (version < acks.Length)
            {
                acks[version] = ackTimestamp;
            }
            else
            {
                var grown = new long[Math.Max(acks.Length * 2, version + 1)];
                Array.Copy(acks, grown, acks.Length);
                grown[version] = ackTimestamp;
                Volatile.Write(ref _acksPerKey[keyIndex], grown);
            }
            Volatile.Write(ref _latestAcked[keyIndex], version);
        }

        /// <summary>
        /// A read that started at <paramref name="readStart"/> and returned <paramref name="returned"/> is stale iff a newer
        /// version was acknowledged before the read started. On true, <paramref name="ackTimestamp"/> is the acknowledgement
        /// time of the smallest acknowledged newer version (normally returned + 1; a higher one only when the writes in
        /// between threw). Acks ascend with versions, so if that version was acknowledged at or after
        /// <paramref name="readStart"/>, no newer version qualifies (the read raced the write: not stale).
        /// <para>
        /// The writer takes the ack timestamp after its write's continuation runs (never earlier than the real
        /// acknowledgement), and the reader looks the registry up only after its read completed. Both delays can make a
        /// stale read go uncounted, never the reverse: the probe can undercount but cannot report a fresh read as stale.
        /// </para>
        /// </summary>
        public static bool IsStale((long Latest, long[] Acks) registry, long returned, long readStart, out long ackTimestamp)
        {
            var (latest, acks) = registry;
            // Usual case, a fresh read: nothing newer has been acknowledged. Bounded by latest, so it's O(1) then.
            for (var v = returned + 1; v <= latest && v < acks.Length; v++)
            {
                var ack = acks[v];
                if (ack == 0) continue; // not acknowledged (yet, or ever)
                if (ack < readStart)
                {
                    ackTimestamp = ack;
                    return true;
                }
                break;
            }
            ackTimestamp = 0;
            return false;
        }
    }

    private static readonly double NanosPerTick = 1_000_000_000.0 / Stopwatch.Frequency;
    private static long ToNanos(long ticks) => (long)(ticks * NanosPerTick);

    public static async Task RunAsync(string[] args)
    {
        var o = Options.Parse(args);
        ThreadPool.SetMinThreads(Math.Max(64, o.Instances * o.ReadersPerInstance / 2), 64);
        var kind = o.Contender;
        var isRnc = ContenderFactory.IsRedisNearCache(kind);
        var chaos = o.Chaos && isRnc;
        var startedAt = DateTimeOffset.UtcNow;

        Console.WriteLine($"RedisNearCache load test (contender {kind}, {o.WriteMode} writes, topology {o.Topology}, latency {o.LatencyLabel})");
        Console.WriteLine($"  instances={o.Instances} readers/instance={o.ReadersPerInstance} writers={o.Writers} target writes/s={o.WritesPerSecond}");
        Console.WriteLine($"  keys={o.Keys} hot keys={o.HotKeys} ({o.HotFraction:P0} of traffic) value={o.ValueBytes} B ttl={o.TtlSeconds}s duration={o.Seconds}s chaos={chaos}");
        if (o.Chaos && !isRnc) Console.WriteLine($"  --chaos ignored: it only applies to RedisNearCache kinds, not {kind}");
        Console.WriteLine();

        var baseSettings = new ContenderSettings
        {
            Endpoint = o.Endpoint,
            TlsCaCertificatePath = o.TlsCa,
            Ttl = TimeSpan.FromSeconds(o.TtlSeconds),
            LocalSizeLimit = Math.Max(1, o.Keys * 2L),
        };

        var admin = await ContenderRedis.ConnectAsync(baseSettings, "bench-admin", allowAdmin: true);
        var adminDb = admin.GetDatabase();
        var serverVersion = await ServerVersionAsync(admin);
        var initialMasters = await MastersAsync(admin);
        Console.WriteLine($"server {serverVersion}, {initialMasters.Length} master(s): {string.Join(", ", initialMasters.Select(m => m.EndPoint))}");

        // --- seed ---------------------------------------------------------------------------------
        // Single-key commands, pipelined: a multi-key MSET would fail across cluster slots.
        var keys = Enumerable.Range(0, o.Keys).Select(i => $"load:k{i}").ToArray();
        var removedLeftovers = await DeleteOwnedKeysAsync(admin);
        if (removedLeftovers > 0) Console.WriteLine($"removed {removedLeftovers:N0} leftover {ContenderRedis.OwnedKeyPrefix}* keys from an earlier run");
        var seedValue = MakeValue(0, o.ValueBytes);
        await ForEachBatchAsync(keys, k => adminDb.StringSetAsync(k, seedValue));
        var registry = new VersionRegistry(keys.Length, Stopwatch.GetTimestamp());

        // --- instances -----------------------------------------------------------------------------
        var instances = new Instance[o.Instances];
        var connectSw = Stopwatch.StartNew();
        for (var i = 0; i < o.Instances; i++)
        {
            var contender = ContenderFactory.Create(kind, baseSettings with { InstanceName = $"r{i}" });
            await contender.InitializeAsync();
            instances[i] = new Instance { Contender = contender };
            if (contender is IRedisNearCacheContender rnc) HookDiagnostics(rnc, $"inst{i}");
        }

        IContender? apiWriter = null;
        var chaosExclude = new HashSet<string>(StringComparer.Ordinal);
        if (o.WriteMode == WriteMode.Api)
        {
            apiWriter = ContenderFactory.Create(kind, baseSettings with { InstanceName = "writer" });
            await apiWriter.InitializeAsync();
            if (apiWriter is IRedisNearCacheContender rncWriter)
            {
                HookDiagnostics(rncWriter, "writer");
                chaosExclude.Add(rncWriter.Provider.GetRequiredService<RedisNearCacheConnection>().ClientName);
            }
        }
        Console.WriteLine($"connected {o.Instances} instances{(apiWriter is null ? "" : " + 1 API writer instance")} in {connectSw.ElapsedMilliseconds} ms; server clients={await ClientsConnectedAsync(admin)}");

        // Locality check: the load test classifies a read as local by synchronous completion. Show that it holds.
        var classify = instances[0].Contender.LocalHitsCompleteSynchronously;
        {
            var pattern = new StringBuilder();
            string? sample = null;
            for (var i = 0; i < 6; i++)
            {
                var vt = instances[0].Contender.GetAsync<string>(keys[0]);
                pattern.Append(vt.IsCompletedSuccessfully ? 'S' : 'a');
                sample = await vt;
                await Task.Delay(20);
            }
            Console.WriteLine($"locality check on inst0 ({keys[0]}, 6 reads 20 ms apart; S = completed synchronously): {pattern}  " +
                              $"declared LocalHitsCompleteSynchronously={classify}; value {(TryParseVersion(sample, out var v0) ? $"v{v0}" : "UNPARSEABLE")}");
        }

        // --- writers (connect) ----------------------------------------------------------------------
        // Writer w owns the keys whose index % Writers == w, so every key has exactly one writer and per-key Redis order
        // equals version order. Each writer keeps the hot/cold split over its own share of the keys. All connections are
        // made before any load starts, so nothing is counted before the run clock starts.
        var writeCount = 0L;
        var writeErrors = 0L;
        var versions = new long[keys.Length];
        var writerMuxes = new List<IConnectionMultiplexer>();
        var writerPlans = new List<(int Writer, int[] OwnedHot, int[] OwnedAll, Func<string, string, Task> Write)>();
        var hotCount = Math.Min(o.HotKeys, keys.Length);
        for (var w = 0; w < o.Writers; w++)
        {
            var writer = w;
            var ownedHot = Enumerable.Range(0, hotCount).Where(k => k % o.Writers == writer).ToArray();
            var ownedAll = Enumerable.Range(0, keys.Length).Where(k => k % o.Writers == writer).ToArray();
            if (ownedAll.Length == 0) continue;
            Func<string, string, Task> write;
            if (apiWriter is not null)
            {
                write = (k, v) => apiWriter.SetAsync(k, v).AsTask();
            }
            else
            {
                var mux = await ContenderRedis.ConnectAsync(baseSettings, $"bench-writer{w}");
                writerMuxes.Add(mux);
                var db = mux.GetDatabase();
                write = (k, v) => db.StringSetAsync(k, v);
            }
            writerPlans.Add((writer, ownedHot, ownedAll, write));
        }

        var before = await SnapshotAsync(admin);
        var sourceLoadsBefore = instances.Sum(i => i.Contender.SourceLoads);
        var rncBefore = SumRnc(instances);
        var cts = new CancellationTokenSource();
        var ct = cts.Token;

        // --- readers -------------------------------------------------------------------------------
        var readerTasks = new List<Task>();
        for (var i = 0; i < instances.Length; i++)
        {
            var inst = instances[i];
            for (var r = 0; r < o.ReadersPerInstance; r++)
            {
                var stats = new ReaderStats();
                inst.Readers.Add(stats);
                var seed = HashCode.Combine(i, r, Environment.TickCount64);
                readerTasks.Add(Task.Run(() => ReaderLoopAsync(inst.Contender, stats, keys, registry, o, classify, seed, ct)));
            }
        }

        // --- writers (start) -----------------------------------------------------------------------
        var writerTasks = new List<Task>();
        var perWriter = Math.Max(1, o.WritesPerSecond / o.Writers);
        foreach (var plan in writerPlans)
        {
            writerTasks.Add(Task.Run(() => WriterLoopAsync(plan.Write, keys, plan.OwnedHot, plan.OwnedAll, versions, registry, o, perWriter,
                () => Interlocked.Increment(ref writeCount), () => Interlocked.Increment(ref writeErrors), plan.Writer * 104729 + Environment.TickCount, ct)));
        }

        // --- sampling ------------------------------------------------------------------------------
        var runSw = Stopwatch.StartNew();
        long lastReads = 0, lastWrites = 0, lastInv = 0;
        var lastSnap = before;
        var chaosDone = false;
        Console.WriteLine();
        Console.WriteLine($"{"t(s)",5} {"reads/s",12} {"local%",7} {"writes/s",9} {"inval/s",10} {"srvGET/s",10} {"srvCmd/s",10} {"trackKeys",10} {"clients",8} {"flush",6} {"rearm",6} {"race",6} {"stale",10}");
        while (runSw.Elapsed < TimeSpan.FromSeconds(o.Seconds))
        {
            await Task.Delay(5000);
            var snap = await SnapshotAsync(admin);
            var reads = instances.Sum(i => i.Readers.Sum(r => Volatile.Read(ref r.Reads)));
            var locals = instances.Sum(i => i.Readers.Sum(r => Volatile.Read(ref r.LocalReads)));
            var stale = instances.Sum(i => i.Readers.Sum(r => Volatile.Read(ref r.StaleReads)));
            var writes = Interlocked.Read(ref writeCount);
            var rnc = SumRnc(instances);
            var inv = rnc?.Invalidations ?? 0;
            var localPct = classify ? (reads == 0 ? 0 : 100.0 * locals / reads).ToString("F1", CultureInfo.InvariantCulture) : "n/a";
            Console.WriteLine($"{runSw.Elapsed.TotalSeconds,5:F0} {(reads - lastReads) / 5.0,12:N0} {localPct,7} {(writes - lastWrites) / 5.0,9:N0} {(inv - lastInv) / 5.0,10:N0} {(snap.Gets - lastSnap.Gets) / 5.0,10:N0} {(snap.Commands - lastSnap.Commands) / 5.0,10:N0} {snap.TrackingKeys,10:N0} {snap.Clients,8} {rnc?.Flushes ?? 0,6} {rnc?.Rearms ?? 0,6} {rnc?.RaceDiscards ?? 0,6} {stale,10:N0}");
            lastReads = reads; lastWrites = writes; lastInv = inv; lastSnap = snap;

            if (chaos && !chaosDone && runSw.Elapsed > TimeSpan.FromSeconds(o.Seconds / 2.0))
            {
                chaosDone = true;
                var (killed, names) = await KillQuarterAsync(admin, instances.Length, chaosExclude);
                Console.WriteLine($"   CHAOS: killed {killed} of our connections ({names} instances, every connection each, on every master)");
            }
        }

        cts.Cancel();
        await Task.WhenAll(readerTasks.Concat(writerTasks).Select(t => t.ContinueWith(_ => { }, TaskScheduler.Default)));
        var elapsed = runSw.Elapsed;
        await Task.Delay(2000); // quiesce: let the last invalidations land

        var after = await SnapshotAsync(admin);

        // --- audit: every local-tier entry must equal Redis ------------------------------------------
        long? localEntries = null, staleEntries = null;
        var peekers = instances.Select(i => i.Contender).OfType<ILocalTierPeek>().ToArray();
        if (peekers.Length == instances.Length && instances.Length > 0)
        {
            // Per-key GETs (pipelined): a multi-key MGET would fail across cluster slots.
            var truth = new string?[keys.Length];
            await ForEachBatchAsync(Enumerable.Range(0, keys.Length).ToArray(),
                async k => truth[k] = (string?)await adminDb.StringGetAsync(keys[k]));
            long audited = 0, stale = 0;
            foreach (var peeker in peekers)
            {
                for (var k = 0; k < keys.Length; k++)
                {
                    if (!peeker.TryPeekLocal<string>(keys[k], out var local)) continue;
                    audited++;
                    if (!string.Equals(local, truth[k], StringComparison.Ordinal)) stale++;
                }
            }
            localEntries = audited;
            staleEntries = stale;
        }

        // --- report ----------------------------------------------------------------------------------
        var allReaders = instances.SelectMany(i => i.Readers).ToArray();
        var totalReads = allReaders.Sum(r => r.Reads);
        var totalLocal = allReaders.Sum(r => r.LocalReads);
        var totalStale = allReaders.Sum(r => r.StaleReads);
        var readErrors = allReaders.Sum(r => r.Errors);
        var localH = new Histogram(); var remoteH = new Histogram(); var staleH = new Histogram();
        foreach (var r in allReaders) { localH.MergeFrom(r.Local); remoteH.MergeFrom(r.Remote); staleH.MergeFrom(r.StaleAge); }
        var sourceLoads = instances.Sum(i => i.Contender.SourceLoads) - sourceLoadsBefore;
        var rncAfter = SumRnc(instances);
        var secs = elapsed.TotalSeconds;

        Console.WriteLine();
        Console.WriteLine("Results");
        Console.WriteLine("-------");
        Console.WriteLine($"contender                {kind} ({o.WriteMode} writes, ttl {o.TtlSeconds}s)");
        Console.WriteLine($"duration                 {secs:F1} s");
        Console.WriteLine($"reads                    {totalReads:N0}  ({totalReads / secs:N0}/s)");
        Console.WriteLine(classify
            ? $"local hit ratio          {(totalReads == 0 ? 0 : 100.0 * totalLocal / totalReads):F2} %"
            : "local hit ratio          n/a (local hits do not complete synchronously)");
        if (classify)
            Console.WriteLine($"local latency p50/p99/p999 {localH.Describe(1000, "us")}");
        Console.WriteLine($"{(classify ? "remote" : "read")} latency p50/p99/p999 {remoteH.Describe(1000, "us")}");
        Console.WriteLine($"source loads             {sourceLoads:N0}");
        Console.WriteLine($"writes                   {writeCount:N0}  ({writeCount / secs:N0}/s){(writeErrors > 0 ? $"  write errors {writeErrors:N0}" : "")}");
        Console.WriteLine($"stale reads              {totalStale:N0}  ({(totalReads == 0 ? 0 : 100.0 * totalStale / totalReads):F4} % of reads)");
        if (totalStale > 0)
            Console.WriteLine($"staleness age p50/p99/p999 {staleH.Describe(1_000_000, "ms")}");
        Console.WriteLine($"read errors              {readErrors:N0}{(readErrors > 0 ? "   <<<<< null/unparseable values or exceptions, see log above" : "")}");
        Console.WriteLine($"server GET commands      {after.Gets - before.Gets:N0}  ({(after.Gets - before.Gets) / secs:N0}/s)");
        Console.WriteLine($"server total commands    {after.Commands - before.Commands:N0}  ({(after.Commands - before.Commands) / secs:N0}/s)");
        Console.WriteLine($"server net output        {(after.NetOut - before.NetOut) / 1024.0 / 1024.0:N1} MB");
        Console.WriteLine($"server used memory       {before.UsedMemory / 1024.0 / 1024.0:N1} MB -> {after.UsedMemory / 1024.0 / 1024.0:N1} MB");
        Console.WriteLine($"server clients           {after.Clients}");
        if (rncAfter is { } ra && rncBefore is { } rb)
        {
            var inv = ra.Invalidations - rb.Invalidations;
            Console.WriteLine($"invalidations received   {inv:N0}  ({inv / secs:N0}/s across {o.Instances} instances; {(writeCount == 0 ? 0 : (double)inv / writeCount):F2} per write)");
            Console.WriteLine($"race discards            {ra.RaceDiscards - rb.RaceDiscards:N0}");
            Console.WriteLine($"flushes / rearms         {ra.Flushes - rb.Flushes:N0} / {ra.Rearms - rb.Rearms:N0}");
            Console.WriteLine($"tracking keys on server  {after.TrackingKeys:N0}");
        }
        if (localEntries is not null)
        {
            Console.WriteLine($"local entries audited    {localEntries:N0}");
            var verdict = kind == ContenderKind.NearCache ? (staleEntries == 0 ? "OK" : "<<<<< FAIL") : "";
            Console.WriteLine($"STALE local entries      {staleEntries:N0}   {verdict}");
        }

        var result = new LoadResult
        {
            Contender = kind.ToString(),
            WriteMode = o.WriteMode.ToString(),
            Topology = o.Topology,
            LatencyLabel = o.LatencyLabel,
            ServerVersion = serverVersion,
            Endpoint = RedactEndpoint(o.Endpoint),
            Chaos = chaos,
            StartedAt = startedAt,
            Machine = DescribeMachine(),
            Instances = o.Instances,
            ReadersPerInstance = o.ReadersPerInstance,
            Writers = o.Writers,
            TargetWritesPerSecond = o.WritesPerSecond,
            Keys = o.Keys,
            HotKeys = o.HotKeys,
            HotFraction = o.HotFraction,
            ValueBytes = o.ValueBytes,
            TtlSeconds = o.TtlSeconds,
            DurationSeconds = secs,
            LocalSizeLimit = baseSettings.LocalSizeLimit,
            ReadErrors = readErrors,
            WriteErrors = writeErrors,
            Reads = totalReads,
            ReadsPerSecond = totalReads / secs,
            LocalReads = classify ? totalLocal : null,
            LocalHitRatio = classify ? (totalReads == 0 ? 0 : (double)totalLocal / totalReads) : null,
            LocalReadLatencyMicros = classify && localH.Count > 0 ? localH.ToPercentiles(1000) : null,
            RemoteReadLatencyMicros = remoteH.ToPercentiles(1000),
            SourceLoads = sourceLoads,
            Writes = writeCount,
            WritesPerSecond = writeCount / secs,
            StaleReads = totalStale,
            StaleReadFraction = totalReads == 0 ? 0 : (double)totalStale / totalReads,
            StalenessAgeMillis = totalStale > 0 ? staleH.ToPercentiles(1_000_000) : null,
            LocalEntriesAudited = localEntries,
            StaleEntriesAfterQuiescence = staleEntries,
            ServerGetCommands = after.Gets - before.Gets,
            ServerTotalCommands = after.Commands - before.Commands,
            ServerCommandsPerSecond = (after.Commands - before.Commands) / secs,
            ServerNetOutputMegabytes = (after.NetOut - before.NetOut) / 1024.0 / 1024.0,
            ServerUsedMemoryBytesBefore = before.UsedMemory,
            ServerUsedMemoryBytesAfter = after.UsedMemory,
            Invalidations = rncAfter?.Invalidations - rncBefore?.Invalidations,
            RaceDiscards = rncAfter?.RaceDiscards - rncBefore?.RaceDiscards,
            Flushes = rncAfter?.Flushes - rncBefore?.Flushes,
            Rearms = rncAfter?.Rearms - rncBefore?.Rearms,
            TrackingKeys = isRnc ? after.TrackingKeys : null,
        };

        if (o.JsonPath is { Length: > 0 } jsonPath)
        {
            var dir = Path.GetDirectoryName(Path.GetFullPath(jsonPath));
            if (dir is not null) Directory.CreateDirectory(dir);
            await File.WriteAllTextAsync(jsonPath, JsonSerializer.Serialize(result, LoadResult.JsonOptions));
            var roundTrip = JsonSerializer.Deserialize<LoadResult>(await File.ReadAllTextAsync(jsonPath), LoadResult.JsonOptions);
            Console.WriteLine($"json                     {jsonPath} ({(roundTrip is not null && roundTrip.Reads == result.Reads ? "re-read OK" : "RE-READ FAILED")})");
        }

        // --- teardown --------------------------------------------------------------------------------
        foreach (var inst in instances) await inst.Contender.DisposeAsync();
        if (apiWriter is not null) await apiWriter.DisposeAsync();
        foreach (var m in writerMuxes) await m.DisposeAsync();
        await ForEachBatchAsync(keys, k => adminDb.KeyDeleteAsync(k));
        await DeleteOwnedKeysAsync(admin);
        await admin.DisposeAsync();
    }

    private static int _reportedReadErrors;

    private static async Task ReaderLoopAsync(IContender contender, ReaderStats stats, string[] keys, VersionRegistry registry, Options o, bool classify, int seed, CancellationToken ct)
    {
        var rng = new Random(seed);
        long n = 0;
        while (!ct.IsCancellationRequested)
        {
            var idx = PickIndex(rng, keys.Length, o);
            var key = keys[idx];
            var t0 = Stopwatch.GetTimestamp();
            string? value;
            bool local;
            try
            {
                // A local hit completes synchronously (no network await); anything else does not.
                var vt = contender.GetAsync<string>(key, ct);
                local = classify && vt.IsCompletedSuccessfully;
                value = await vt;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                stats.Errors++;
                ReportReadError($"read {key} threw {ex.GetType().Name}: {ex.Message}");
                continue;
            }
            var tDone = Stopwatch.GetTimestamp();

            (local ? stats.Local : stats.Remote).Record(ToNanos(tDone - t0));
            Volatile.Write(ref stats.Reads, stats.Reads + 1);
            if (local) Volatile.Write(ref stats.LocalReads, stats.LocalReads + 1);

            if (!TryParseVersion(value, out var version))
            {
                stats.Errors++;
                ReportReadError($"read {key} returned {(value is null ? "null" : $"unparseable '{value[..Math.Min(24, value.Length)]}'")}");
            }
            else if (VersionRegistry.IsStale(registry.Read(idx), version, t0, out var ackTimestamp))
            {
                Volatile.Write(ref stats.StaleReads, stats.StaleReads + 1);
                stats.StaleAge.Record(ToNanos(tDone - ackTimestamp));
            }

            // Hits complete synchronously; yield now and then so the writers and invalidation handlers get
            // thread-pool time instead of being starved by spinning readers.
            if ((++n & 31) == 0) await Task.Yield();
        }
    }

    private static void ReportReadError(string message)
    {
        var count = Interlocked.Increment(ref _reportedReadErrors);
        if (count <= 20) Diag($"READ ERROR {message}");
        else if (count == 21) Diag("READ ERROR further read errors are counted but not printed");
    }

    private static async Task WriterLoopAsync(Func<string, string, Task> write, string[] keys, int[] ownedHot, int[] ownedAll, long[] versions,
        VersionRegistry registry, Options o, int perSecond, Func<long> onWrite, Func<long> onError, int seed, CancellationToken ct)
    {
        var rng = new Random(seed);
        const int tickMs = 10;
        var perTick = Math.Min(Math.Max(1, perSecond * tickMs / 1000), ownedAll.Length);
        var picked = new HashSet<int>();
        var sw = Stopwatch.StartNew();
        var ticks = 0L;
        var reportedErrors = 0;

        async Task WriteOneAsync(int idx)
        {
            // Only this writer touches versions[idx], and never with two writes to idx in flight.
            var version = ++versions[idx];
            try
            {
                await write(keys[idx], MakeValue(version, o.ValueBytes));
                registry.Record(idx, version, Stopwatch.GetTimestamp());
                onWrite();
            }
            catch (Exception ex)
            {
                // Not registered: if the write did land, reads of it count as fresh and later reads of an older version
                // are judged against the next acknowledged version. Only ever an undercount.
                onError();
                if (Interlocked.Increment(ref reportedErrors) <= 5) Diag($"WRITE ERROR {keys[idx]}: {ex.GetType().Name}: {ex.Message}");
            }
        }

        while (!ct.IsCancellationRequested)
        {
            // Within one tick a key is written at most once.
            picked.Clear();
            for (var attempts = 0; picked.Count < perTick && attempts < perTick * 20; attempts++)
            {
                var pool = ownedHot.Length > 0 && rng.NextDouble() < o.HotFraction ? ownedHot : ownedAll;
                picked.Add(pool[rng.Next(pool.Length)]);
            }
            var tasks = new Task[picked.Count];
            var t = 0;
            foreach (var idx in picked) tasks[t++] = WriteOneAsync(idx);
            await Task.WhenAll(tasks);
            ticks++;
            var due = TimeSpan.FromMilliseconds(ticks * tickMs) - sw.Elapsed;
            if (due > TimeSpan.Zero) { try { await Task.Delay(due, ct); } catch (OperationCanceledException) { } }
        }
    }

    private static readonly Stopwatch DiagClock = Stopwatch.StartNew();
    private static void Diag(string message) => Console.WriteLine($"   [{DiagClock.Elapsed.TotalSeconds,7:F2}s] {message}");

    /// <summary>Every lifecycle event of a RedisNearCache instance, so unexplained flushes can be attributed.</summary>
    private static void HookDiagnostics(IRedisNearCacheContender contender, string id)
    {
        var armer = contender.Provider.GetRequiredService<ITrackingArmer>();
        var listener = contender.Provider.GetRequiredService<IInvalidationListener>();
        armer.Armed += e => Diag($"{id} Armed {e.Reason} {e.EndPoint} -> {e.RedirectClientId}");
        armer.TrackingLost += ep => Diag($"{id} TrackingLost {ep}");
        listener.FlushAll += () => Diag($"{id} FlushAll (null invalidation)");
        var mux = contender.Provider.GetRequiredService<RedisNearCacheConnection>().Multiplexer;
        mux.ConnectionFailed += (_, e) => Diag($"{id} ConnectionFailed {e.EndPoint} {e.ConnectionType} {e.FailureType} {e.Exception?.GetType().Name}: {e.Exception?.Message}");
        mux.ConnectionRestored += (_, e) => Diag($"{id} ConnectionRestored {e.EndPoint} {e.ConnectionType}");
        mux.ErrorMessage += (_, e) => Diag($"{id} ErrorMessage {e.Message}");
        mux.InternalError += (_, e) => Diag($"{id} InternalError {e.Origin} {e.Exception?.Message}");
    }

    private static int PickIndex(Random rng, int keyCount, Options o)
    {
        if (rng.NextDouble() < o.HotFraction) return rng.Next(Math.Min(o.HotKeys, keyCount));
        return rng.Next(keyCount);
    }

    private static string MakeValue(long version, int bytes)
    {
        var sb = new StringBuilder(bytes);
        sb.Append('v').Append(version).Append(':');
        while (sb.Length < bytes) sb.Append('x');
        return sb.ToString();
    }

    private static bool TryParseVersion(string? value, out long version)
    {
        version = 0;
        if (value is null || value.Length < 3 || value[0] != 'v') return false;
        var colon = value.IndexOf(':', 1);
        return colon > 1 && long.TryParse(value.AsSpan(1, colon - 1), NumberStyles.None, CultureInfo.InvariantCulture, out version);
    }

    /// <summary>Runs <paramref name="op"/> for every item, 1,000 pipelined commands at a time.</summary>
    private static async Task ForEachBatchAsync<TItem>(IReadOnlyList<TItem> items, Func<TItem, Task> op)
    {
        for (var i = 0; i < items.Count; i += 1000)
        {
            var batch = new Task[Math.Min(1000, items.Count - i)];
            for (var j = 0; j < batch.Length; j++) batch[j] = op(items[i + j]);
            await Task.WhenAll(batch);
        }
    }

    /// <summary>Deletes every key under <see cref="ContenderRedis.OwnedKeyPrefix"/> on every master (L2 entries of any kind).</summary>
    private static async Task<long> DeleteOwnedKeysAsync(IConnectionMultiplexer admin)
    {
        var db = admin.GetDatabase();
        long deleted = 0;
        foreach (var server in await MastersAsync(admin))
        {
            var batch = new List<Task<bool>>();
            await foreach (var key in server.KeysAsync(pattern: ContenderRedis.OwnedKeyPrefix + "*", pageSize: 1000))
            {
                batch.Add(db.KeyDeleteAsync(key));
                if (batch.Count >= 1000) { deleted += (await Task.WhenAll(batch)).Count(d => d); batch.Clear(); }
            }
            deleted += (await Task.WhenAll(batch)).Count(d => d);
        }
        return deleted;
    }

    /// <summary>
    /// Every connected master, once. The multiplexer can list one node under two endpoints (the configured
    /// <c>localhost:7100</c> and the <c>127.0.0.1:7100</c> that CLUSTER NODES announces), which would double-count its
    /// INFO counters, so servers are de-duplicated by <c>run_id</c>.
    /// </summary>
    private static async Task<IServer[]> MastersAsync(IConnectionMultiplexer admin)
    {
        var result = new List<IServer>();
        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var server in admin.GetServers().Where(s => s.IsConnected && !s.IsReplica))
        {
            var info = await server.InfoAsync("server");
            var runId = info.SelectMany(g => g).FirstOrDefault(kv => kv.Key == "run_id").Value ?? server.EndPoint.ToString()!;
            if (seen.Add(runId)) result.Add(server);
        }
        return result.ToArray();
    }

    private readonly record struct Snapshot(long Gets, long Commands, long NetOut, long TrackingKeys, int Clients, long UsedMemory);

    /// <summary>Server counters summed over every master (one master outside cluster mode).</summary>
    private static async Task<Snapshot> SnapshotAsync(IConnectionMultiplexer admin)
    {
        long gets = 0, commands = 0, netOut = 0, tracking = 0, usedMemory = 0; var clients = 0;
        foreach (var server in await MastersAsync(admin))
        {
            var info = await server.InfoAsync("all"); // "commandstats" is not part of the default INFO
            foreach (var section in info)
            {
                foreach (var kv in section)
                {
                    switch (kv.Key)
                    {
                        case "cmdstat_get":
                            var calls = kv.Value.Split(',').FirstOrDefault(p => p.StartsWith("calls=", StringComparison.Ordinal));
                            if (calls is not null) gets += long.Parse(calls["calls=".Length..], CultureInfo.InvariantCulture);
                            break;
                        case "total_commands_processed": commands += long.Parse(kv.Value, CultureInfo.InvariantCulture); break;
                        case "total_net_output_bytes": netOut += long.Parse(kv.Value, CultureInfo.InvariantCulture); break;
                        case "tracking_total_keys": tracking += long.Parse(kv.Value, CultureInfo.InvariantCulture); break;
                        case "connected_clients": clients += int.Parse(kv.Value, CultureInfo.InvariantCulture); break;
                        case "used_memory": usedMemory += long.Parse(kv.Value, CultureInfo.InvariantCulture); break;
                    }
                }
            }
        }
        return new Snapshot(gets, commands, netOut, tracking, clients, usedMemory);
    }

    private static async Task<string> ServerVersionAsync(IConnectionMultiplexer admin)
    {
        var server = (await MastersAsync(admin)).FirstOrDefault();
        if (server is null) return "unknown";
        string? redis = null, valkey = null;
        foreach (var section in await server.InfoAsync("server"))
        {
            foreach (var kv in section)
            {
                if (kv.Key == "valkey_version") valkey = kv.Value;
                else if (kv.Key == "redis_version") redis = kv.Value;
            }
        }
        return valkey is not null ? $"valkey {valkey}" : redis is not null ? $"redis {redis}" : "unknown";
    }

    private readonly record struct RncCounters(long Invalidations, long RaceDiscards, long Flushes, long Rearms);

    private static RncCounters? SumRnc(Instance[] instances)
    {
        var rnc = instances.Select(i => i.Contender).OfType<IRedisNearCacheContender>().ToArray();
        if (rnc.Length == 0) return null;
        return new RncCounters(
            rnc.Sum(c => c.Cache.Statistics.Invalidations),
            rnc.Sum(c => c.Cache.Statistics.RaceDiscards),
            rnc.Sum(c => c.Cache.Statistics.Flushes),
            rnc.Sum(c => c.Cache.Statistics.Rearms));
    }

    private static async Task<int> ClientsConnectedAsync(IConnectionMultiplexer admin) => (await MastersAsync(admin)).Sum(s => s.ClientList().Length);

    /// <summary>
    /// Kills every connection, on every master, of a quarter of the RedisNearCache instances (found by the default "rnc-"
    /// client name prefix; each instance uses one client name on all its connections).
    /// </summary>
    private static async Task<(int Killed, int Instances)> KillQuarterAsync(IConnectionMultiplexer admin, int instances, ISet<string> exclude)
    {
        var perServer = (await MastersAsync(admin)).Select(s => (Server: s, Clients: s.ClientList())).ToArray();
        var names = perServer
            .SelectMany(p => p.Clients)
            .Select(c => c.Name)
            .OfType<string>()
            .Where(n => n.StartsWith("rnc-", StringComparison.Ordinal) && !exclude.Contains(n))
            .Distinct()
            .Take(Math.Max(1, instances / 4))
            .ToHashSet();
        var killed = 0;
        foreach (var (server, clients) in perServer)
        {
            foreach (var c in clients.Where(c => c.Name is { } n && names.Contains(n)))
            {
                try { server.Execute("CLIENT", "KILL", "ID", c.Id); killed++; } catch { /* already gone */ }
            }
        }
        return (killed, names.Count);
    }

    private static string RedactEndpoint(string endpoint)
    {
        try { return ConfigurationOptions.Parse(endpoint).ToString(includePassword: false); }
        catch (ArgumentException) { return "(unparseable endpoint)"; }
    }

    private static string DescribeMachine()
    {
        string? cpu = null;
        try
        {
            if (OperatingSystem.IsMacOS())
            {
                var psi = new ProcessStartInfo("sysctl", "-n machdep.cpu.brand_string") { RedirectStandardOutput = true, RedirectStandardError = true };
                using var p = Process.Start(psi);
                if (p is not null)
                {
                    var output = p.StandardOutput.ReadToEnd().Trim();
                    if (p.WaitForExit(2000) && p.ExitCode == 0 && output.Length > 0) cpu = output;
                }
            }
            else if (OperatingSystem.IsLinux() && File.Exists("/proc/cpuinfo"))
            {
                cpu = File.ReadLines("/proc/cpuinfo").FirstOrDefault(l => l.StartsWith("model name", StringComparison.Ordinal))?.Split(':', 2)[1].Trim();
            }
        }
        catch (Exception)
        {
            // best effort
        }
        return $"{cpu ?? "unknown CPU"}, {Environment.ProcessorCount} cores, {RuntimeInformation.FrameworkDescription}, {RuntimeInformation.RuntimeIdentifier}";
    }
}
