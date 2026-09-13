using System.Diagnostics;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Engines;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Running;
using BenchmarkDotNet.Toolchains.InProcess.Emit;
using Microsoft.Extensions.DependencyInjection;
using StackExchange.Redis;

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

        var remainingArgs = args.Where(a => a != "--demo").ToArray();

        // This machine does not have the dotnet SDK on PATH (only at ~/.dotnet/dotnet), so BenchmarkDotNet's
        // default toolchain -- which shells out to a `dotnet` process to build and run each benchmark in
        // isolation -- cannot be used. The in-process (emit) toolchain runs benchmarks in this same process
        // instead, at the cost of a little isolation between benchmarks.
        // RunStrategy.Monitoring runs exactly one invocation per iteration instead of the default Throughput
        // strategy's auto-unrolling (which, for a ~130 us network round trip, would otherwise pack thousands
        // of invocations into a single iteration to hit its target iteration duration) -- keeping a full run
        // to a couple of minutes as intended for these network-bound benchmarks.
        var job = Job.Default
            .WithStrategy(RunStrategy.Monitoring)
            .WithWarmupCount(3)
            .WithIterationCount(10)
            .WithToolchain(InProcessEmitToolchain.Instance);
        var config = ManualConfig.Create(DefaultConfig.Instance).AddJob(job);

        BenchmarkRunner.Run<ReadBenchmarks>(config, remainingArgs);
        return 0;
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
        var stopwatch = Stopwatch.StartNew();
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
        Console.WriteLine($"{"Write-to-eviction latency (µs)",-32} | {(evicted ? stopwatch.Elapsed.TotalMicroseconds.ToString("F1") : "timed out")}");
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
                return long.Parse(callsValue);
            }
        }

        return 0;
    }
}
