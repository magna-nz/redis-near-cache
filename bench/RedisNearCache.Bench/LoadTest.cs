using System.Diagnostics;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using RedisNearCache.Internal;
using StackExchange.Redis;

namespace RedisNearCache.Bench;

/// <summary>
/// Production-shaped load test: many application instances (each with its own private multiplexer, i.e. two
/// tracked connections), many concurrent readers per instance, and foreign writers producing a steady stream
/// of invalidations that fan out to every instance tracking the key. Ends with a staleness audit of every
/// instance's L1 against Redis. <c>--baseline</c> runs the same read/write load through plain
/// StackExchange.Redis (no near cache) for comparison. <c>--chaos</c> kills a quarter of the instances'
/// connections halfway through.
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
        public bool Baseline;
        public bool Chaos;

        public static Options Parse(string[] args)
        {
            var o = new Options();
            for (var i = 0; i < args.Length; i++)
            {
                string Next() => args[++i];
                switch (args[i])
                {
                    case "--endpoint": o.Endpoint = Next(); break;
                    case "--instances": o.Instances = int.Parse(Next()); break;
                    case "--readers": o.ReadersPerInstance = int.Parse(Next()); break;
                    case "--writers": o.Writers = int.Parse(Next()); break;
                    case "--writes-per-sec": o.WritesPerSecond = int.Parse(Next()); break;
                    case "--keys": o.Keys = int.Parse(Next()); break;
                    case "--hot-keys": o.HotKeys = int.Parse(Next()); break;
                    case "--hot-fraction": o.HotFraction = double.Parse(Next(), System.Globalization.CultureInfo.InvariantCulture); break;
                    case "--seconds": o.Seconds = int.Parse(Next()); break;
                    case "--value-bytes": o.ValueBytes = int.Parse(Next()); break;
                    case "--baseline": o.Baseline = true; break;
                    case "--chaos": o.Chaos = true; break;
                }
            }
            return o;
        }
    }

    /// <summary>Log-scale latency histogram: bucket i covers [1.2^i ns, 1.2^(i+1) ns). Lock-free per reader, merged at the end.</summary>
    private sealed class Histogram
    {
        private const double Base = 1.2;
        private static readonly double LogBase = Math.Log(Base);
        private readonly long[] _buckets = new long[160];
        public long Count;

        public void Record(long nanos)
        {
            var idx = nanos <= 1 ? 0 : (int)Math.Min(_buckets.Length - 1, Math.Log(nanos) / LogBase);
            _buckets[idx]++;
            Count++;
        }

        public void MergeFrom(Histogram other)
        {
            for (var i = 0; i < _buckets.Length; i++) _buckets[i] += other._buckets[i];
            Count += other.Count;
        }

        public double Percentile(double p)
        {
            if (Count == 0) return 0;
            var target = (long)Math.Ceiling(Count * p);
            long seen = 0;
            for (var i = 0; i < _buckets.Length; i++)
            {
                seen += _buckets[i];
                if (seen >= target) return Math.Pow(Base, i + 0.5);
            }
            return Math.Pow(Base, _buckets.Length);
        }
    }

    private sealed class Instance
    {
        public ServiceProvider? Provider;
        public IRedisNearCache? Cache;
        public IConnectionMultiplexer? Plain;
        public long Reads;
        public long Hits;
        public readonly List<(Histogram hit, Histogram miss)> ReaderHistograms = new();
    }

    public static async Task RunAsync(string[] args)
    {
        var o = Options.Parse(args);
        ThreadPool.SetMinThreads(Math.Max(64, o.Instances * o.ReadersPerInstance / 2), 64);

        Console.WriteLine($"RedisNearCache load test ({(o.Baseline ? "BASELINE: plain StackExchange.Redis" : "near cache")})");
        Console.WriteLine($"  instances={o.Instances} readers/instance={o.ReadersPerInstance} writers={o.Writers} target writes/s={o.WritesPerSecond}");
        Console.WriteLine($"  keys={o.Keys} hot keys={o.HotKeys} ({o.HotFraction:P0} of traffic) value={o.ValueBytes} B duration={o.Seconds}s chaos={o.Chaos}");
        Console.WriteLine();

        var admin = await ConnectionMultiplexer.ConnectAsync(o.Endpoint + ",allowAdmin=true");
        var server = admin.GetServer(admin.GetEndPoints()[0]);
        var adminDb = admin.GetDatabase();

        // --- seed ---------------------------------------------------------------------------------
        var keys = Enumerable.Range(0, o.Keys).Select(i => $"load:k{i}").ToArray();
        var seedValue = MakeValue(0, o.ValueBytes);
        for (var i = 0; i < keys.Length; i += 1000)
        {
            var batch = keys.Skip(i).Take(1000).Select(k => new KeyValuePair<RedisKey, RedisValue>(k, seedValue)).ToArray();
            await adminDb.StringSetAsync(batch);
        }

        // --- instances -----------------------------------------------------------------------------
        var instances = new Instance[o.Instances];
        var connectSw = Stopwatch.StartNew();
        for (var i = 0; i < o.Instances; i++)
        {
            var inst = new Instance();
            if (o.Baseline)
            {
                inst.Plain = await ConnectionMultiplexer.ConnectAsync(o.Endpoint);
            }
            else
            {
                var services = new ServiceCollection();
                services.AddRedisNearCache(o.Endpoint, opt => opt.L1SizeLimit = o.Keys * 2);
                inst.Provider = services.BuildServiceProvider();
                inst.Cache = inst.Provider.GetRequiredService<IRedisNearCache>();
                await inst.Cache.Ready;
                // Diagnostics: every lifecycle event, so unexplained flushes can be attributed.
                var id = i;
                var armer = inst.Provider.GetRequiredService<ITrackingArmer>();
                var listener = inst.Provider.GetRequiredService<IInvalidationListener>();
                armer.Armed += e => Diag($"inst{id} Armed {e.Reason} {e.EndPoint} -> {e.RedirectClientId}");
                armer.TrackingLost += ep => Diag($"inst{id} TrackingLost {ep}");
                listener.FlushAll += () => Diag($"inst{id} FlushAll (null invalidation)");
                var mux = inst.Provider.GetRequiredService<RedisNearCacheConnection>().Multiplexer;
                mux.ConnectionFailed += (_, e) => Diag($"inst{id} ConnectionFailed {e.ConnectionType} {e.FailureType} {e.Exception?.GetType().Name}: {e.Exception?.Message}");
                mux.ConnectionRestored += (_, e) => Diag($"inst{id} ConnectionRestored {e.ConnectionType}");
                mux.ErrorMessage += (_, e) => Diag($"inst{id} ErrorMessage {e.Message}");
                mux.InternalError += (_, e) => Diag($"inst{id} InternalError {e.Origin} {e.Exception?.Message}");
            }
            instances[i] = inst;
        }
        Console.WriteLine($"connected {o.Instances} instances in {connectSw.ElapsedMilliseconds} ms; server clients={ClientsConnected(server)}");

        var before = await SnapshotAsync(server);
        var cts = new CancellationTokenSource();
        var ct = cts.Token;

        // --- readers -------------------------------------------------------------------------------
        var readerTasks = new List<Task>();
        foreach (var inst in instances)
        {
            for (var r = 0; r < o.ReadersPerInstance; r++)
            {
                var hist = (hit: new Histogram(), miss: new Histogram());
                lock (inst.ReaderHistograms) inst.ReaderHistograms.Add(hist);
                readerTasks.Add(Task.Run(() => ReaderLoopAsync(inst, keys, o, hist.hit, hist.miss, ct)));
            }
        }

        // --- writers -------------------------------------------------------------------------------
        var writeCount = 0L;
        var writerTasks = new List<Task>();
        var writerMuxes = new List<IConnectionMultiplexer>();
        for (var w = 0; w < o.Writers; w++)
        {
            var mux = await ConnectionMultiplexer.ConnectAsync(o.Endpoint);
            writerMuxes.Add(mux);
            var perWriter = Math.Max(1, o.WritesPerSecond / o.Writers);
            writerTasks.Add(Task.Run(() => WriterLoopAsync(mux.GetDatabase(), keys, o, perWriter, () => Interlocked.Increment(ref writeCount), ct)));
        }

        // --- sampling ------------------------------------------------------------------------------
        var runSw = Stopwatch.StartNew();
        long lastReads = 0, lastWrites = 0, lastInv = 0;
        var lastSnap = before;
        var chaosDone = false;
        Console.WriteLine();
        Console.WriteLine($"{"t(s)",5} {"reads/s",12} {"hit%",7} {"writes/s",9} {"inval/s",10} {"srvGET/s",10} {"srvCmd/s",10} {"trackKeys",10} {"clients",8} {"flush",6} {"rearm",6} {"race",6}");
        while (runSw.Elapsed < TimeSpan.FromSeconds(o.Seconds))
        {
            await Task.Delay(5000);
            var snap = await SnapshotAsync(server);
            var reads = instances.Sum(i => Interlocked.Read(ref i.Reads));
            var hits = instances.Sum(i => Interlocked.Read(ref i.Hits));
            var writes = Interlocked.Read(ref writeCount);
            var inv = o.Baseline ? 0 : instances.Sum(i => i.Cache!.Statistics.Invalidations);
            var flushes = o.Baseline ? 0 : instances.Sum(i => i.Cache!.Statistics.Flushes);
            var rearms = o.Baseline ? 0 : instances.Sum(i => i.Cache!.Statistics.Rearms);
            var races = o.Baseline ? 0 : instances.Sum(i => i.Cache!.Statistics.RaceDiscards);
            Console.WriteLine($"{runSw.Elapsed.TotalSeconds,5:F0} {(reads - lastReads) / 5.0,12:N0} {(reads == 0 ? 0 : 100.0 * hits / reads),7:F1} {(writes - lastWrites) / 5.0,9:N0} {(inv - lastInv) / 5.0,10:N0} {(snap.Gets - lastSnap.Gets) / 5.0,10:N0} {(snap.Commands - lastSnap.Commands) / 5.0,10:N0} {snap.TrackingKeys,10:N0} {snap.Clients,8} {flushes,6} {rearms,6} {races,6}");
            lastReads = reads; lastWrites = writes; lastInv = inv; lastSnap = snap;

            if (o.Chaos && !chaosDone && !o.Baseline && runSw.Elapsed > TimeSpan.FromSeconds(o.Seconds / 2.0))
            {
                chaosDone = true;
                var killed = KillQuarter(server, instances.Length);
                Console.WriteLine($"   CHAOS: killed {killed} of our connections (a quarter of the instances, both connections each)");
            }
        }

        cts.Cancel();
        await Task.WhenAll(readerTasks.Concat(writerTasks).Select(t => t.ContinueWith(_ => { })));
        var elapsed = runSw.Elapsed;
        await Task.Delay(2000); // quiesce: let the last invalidations land

        var after = await SnapshotAsync(server);

        // --- audit: every L1 entry must equal Redis ---------------------------------------------------
        long localEntries = 0, stale = 0;
        if (!o.Baseline)
        {
            var truth = new Dictionary<string, string?>(keys.Length);
            for (var i = 0; i < keys.Length; i += 1000)
            {
                var chunk = keys.Skip(i).Take(1000).ToArray();
                var values = await adminDb.StringGetAsync(chunk.Select(k => (RedisKey)k).ToArray());
                for (var j = 0; j < chunk.Length; j++) truth[chunk[j]] = values[j].IsNull ? null : (string?)values[j];
            }
            foreach (var inst in instances)
            {
                foreach (var k in keys)
                {
                    if (!inst.Cache!.TryGetLocal<string>(k, out var local)) continue;
                    localEntries++;
                    if (!string.Equals(local, truth[k], StringComparison.Ordinal)) stale++;
                }
            }
        }

        // --- report ----------------------------------------------------------------------------------
        var totalReads = instances.Sum(i => i.Reads);
        var totalHits = instances.Sum(i => i.Hits);
        var hitH = new Histogram(); var missH = new Histogram();
        foreach (var inst in instances) foreach (var (h, m) in inst.ReaderHistograms) { hitH.MergeFrom(h); missH.MergeFrom(m); }

        Console.WriteLine();
        Console.WriteLine("Results");
        Console.WriteLine("-------");
        Console.WriteLine($"duration                 {elapsed.TotalSeconds:F1} s");
        Console.WriteLine($"reads                    {totalReads:N0}  ({totalReads / elapsed.TotalSeconds:N0}/s)");
        Console.WriteLine($"hit ratio                {(totalReads == 0 ? 0 : 100.0 * totalHits / totalReads):F2} %");
        if (!o.Baseline)
            Console.WriteLine($"hit latency  p50/p99/p999 {hitH.Percentile(0.50) / 1000:F1} / {hitH.Percentile(0.99) / 1000:F1} / {hitH.Percentile(0.999) / 1000:F1} us  (n={hitH.Count:N0})");
        Console.WriteLine($"{(o.Baseline ? "read" : "miss")} latency p50/p99/p999 {missH.Percentile(0.50) / 1000:F1} / {missH.Percentile(0.99) / 1000:F1} / {missH.Percentile(0.999) / 1000:F1} us  (n={missH.Count:N0})");
        Console.WriteLine($"writes                   {writeCount:N0}  ({writeCount / elapsed.TotalSeconds:N0}/s)");
        Console.WriteLine($"server GET commands      {after.Gets - before.Gets:N0}  ({(after.Gets - before.Gets) / elapsed.TotalSeconds:N0}/s)");
        Console.WriteLine($"server total commands    {after.Commands - before.Commands:N0}  ({(after.Commands - before.Commands) / elapsed.TotalSeconds:N0}/s)");
        Console.WriteLine($"server net output        {(after.NetOut - before.NetOut) / 1024.0 / 1024.0:N1} MB");
        Console.WriteLine($"server clients           {after.Clients}");
        if (!o.Baseline)
        {
            var inv = instances.Sum(i => i.Cache!.Statistics.Invalidations);
            Console.WriteLine($"invalidations received   {inv:N0}  ({inv / elapsed.TotalSeconds:N0}/s across {o.Instances} instances; {(writeCount == 0 ? 0 : (double)inv / writeCount):F2} per write)");
            Console.WriteLine($"race discards            {instances.Sum(i => i.Cache!.Statistics.RaceDiscards):N0}");
            Console.WriteLine($"flushes / rearms         {instances.Sum(i => i.Cache!.Statistics.Flushes):N0} / {instances.Sum(i => i.Cache!.Statistics.Rearms):N0}");
            Console.WriteLine($"tracking keys on server  {after.TrackingKeys:N0}");
            Console.WriteLine($"L1 entries audited       {localEntries:N0}");
            Console.WriteLine($"STALE L1 entries         {stale:N0}   {(stale == 0 ? "OK" : "<<<<< FAIL")}");
        }

        // --- teardown --------------------------------------------------------------------------------
        foreach (var inst in instances)
        {
            if (inst.Provider is not null) await inst.Provider.DisposeAsync();
            if (inst.Plain is not null) await inst.Plain.DisposeAsync();
        }
        foreach (var m in writerMuxes) await m.DisposeAsync();
        await adminDb.KeyDeleteAsync(keys.Select(k => (RedisKey)k).ToArray());
        await admin.DisposeAsync();
    }

    private static async Task ReaderLoopAsync(Instance inst, string[] keys, Options o, Histogram hitHist, Histogram missHist, CancellationToken ct)
    {
        var rng = new Random(Environment.CurrentManagedThreadId * 7919 + inst.GetHashCode());
        var db = inst.Plain?.GetDatabase();
        long n = 0;
        while (!ct.IsCancellationRequested)
        {
            var key = PickKey(rng, keys, o);
            var t0 = Stopwatch.GetTimestamp();
            var hit = false;
            if (db is not null)
            {
                _ = await db.StringGetAsync(key);
            }
            else
            {
                // An L1 hit completes synchronously (no network await); a miss does not. Cheap and exact.
                var vt = inst.Cache!.GetAsync<string>(key, ct);
                hit = vt.IsCompletedSuccessfully;
                _ = await vt;
                if (hit) Interlocked.Increment(ref inst.Hits);
            }
            (hit ? hitHist : missHist).Record((long)((Stopwatch.GetTimestamp() - t0) * (1_000_000_000.0 / Stopwatch.Frequency)));
            Interlocked.Increment(ref inst.Reads);
            // Hits complete synchronously; yield now and then so the writers and invalidation handlers get
            // thread-pool time instead of being starved by spinning readers.
            if ((++n & 31) == 0) await Task.Yield();
        }
    }

    private static async Task WriterLoopAsync(IDatabase db, string[] keys, Options o, int perSecond, Func<long> onWrite, CancellationToken ct)
    {
        var rng = new Random(Environment.CurrentManagedThreadId * 104729);
        var version = 0L;
        var tickMs = 10;
        var perTick = Math.Max(1, perSecond * tickMs / 1000);
        var sw = Stopwatch.StartNew();
        var ticks = 0L;
        while (!ct.IsCancellationRequested)
        {
            var tasks = new Task[perTick];
            for (var i = 0; i < perTick; i++)
            {
                var key = PickKey(rng, keys, o);
                tasks[i] = db.StringSetAsync(key, MakeValue(++version, o.ValueBytes));
                onWrite();
            }
            try { await Task.WhenAll(tasks); } catch (RedisException) { /* keep going */ }
            ticks++;
            var due = TimeSpan.FromMilliseconds(ticks * tickMs) - sw.Elapsed;
            if (due > TimeSpan.Zero) { try { await Task.Delay(due, ct); } catch (OperationCanceledException) { } }
        }
    }

    private static readonly Stopwatch DiagClock = Stopwatch.StartNew();
    private static void Diag(string message) => Console.WriteLine($"   [{DiagClock.Elapsed.TotalSeconds,7:F2}s] {message}");

    private static string PickKey(Random rng, string[] keys, Options o)
    {
        if (rng.NextDouble() < o.HotFraction) return keys[rng.Next(Math.Min(o.HotKeys, keys.Length))];
        return keys[rng.Next(keys.Length)];
    }

    private static string MakeValue(long version, int bytes)
    {
        var sb = new StringBuilder(bytes);
        sb.Append('v').Append(version).Append(':');
        while (sb.Length < bytes) sb.Append('x');
        return sb.ToString();
    }

    private readonly record struct Snapshot(long Gets, long Commands, long NetOut, long TrackingKeys, int Clients);

    private static async Task<Snapshot> SnapshotAsync(IServer server)
    {
        var info = await server.InfoAsync("all"); // "commandstats" is not part of the default INFO
        long gets = 0, commands = 0, netOut = 0, tracking = 0; var clients = 0;
        foreach (var section in info)
        {
            foreach (var kv in section)
            {
                switch (kv.Key)
                {
                    case "cmdstat_get":
                        var calls = kv.Value.Split(',').FirstOrDefault(p => p.StartsWith("calls=", StringComparison.Ordinal));
                        if (calls is not null) gets = long.Parse(calls["calls=".Length..]);
                        break;
                    case "total_commands_processed": commands = long.Parse(kv.Value); break;
                    case "total_net_output_bytes": netOut = long.Parse(kv.Value); break;
                    case "tracking_total_keys": tracking = long.Parse(kv.Value); break;
                    case "connected_clients": clients = int.Parse(kv.Value); break;
                }
            }
        }
        return new Snapshot(gets, commands, netOut, tracking, clients);
    }

    private static int ClientsConnected(IServer server) => server.ClientList().Length;

    /// <summary>Kills both connections of a quarter of the instances by client name prefix.</summary>
    private static int KillQuarter(IServer server, int instances)
    {
        var ours = server.ClientList().Where(c => c.Name is { } n && n.StartsWith("rnc-", StringComparison.Ordinal)).ToArray();
        var names = ours.Select(c => c.Name!).Distinct().Take(Math.Max(1, instances / 4)).ToHashSet();
        var killed = 0;
        foreach (var c in ours.Where(c => names.Contains(c.Name!)))
        {
            try { server.Execute("CLIENT", "KILL", "ID", c.Id); killed++; } catch { /* already gone */ }
        }
        return killed;
    }
}
