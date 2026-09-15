namespace RedisNearCache.Bench.Micro;

/// <summary>
/// Job sizing shared between the BenchmarkDotNet <c>Program</c> job configuration and the miss-path benchmarks'
/// <see cref="MissKeyPool"/> sizing, so the two never drift apart. Hit-path jobs use BenchmarkDotNet's normal
/// Throughput auto-unrolling (no per-call key to size a pool for); miss-path jobs fix InvocationCount/UnrollFactor so
/// the total number of calls in a job is exactly computable ahead of time.
/// </summary>
public static class MicroJobSettings
{
    // --- hit path (BenchmarkDotNet): Throughput strategy, default auto-unrolling ------------------------------
    public const int HitWarmupCount = 5;
    public const int HitIterationCount = 15;
    public const int QuickHitWarmupCount = 2;
    public const int QuickHitIterationCount = 5;

    // --- miss path (BenchmarkDotNet): fixed invocation count, one key per invocation --------------------------
    public const int MissInvocationCount = 50;
    public const int MissUnrollFactor = 1;
    public const int MissIterationCount = 10;
    public const int MissWarmupCount = 3;

    public const int QuickMissInvocationCount = 10;
    public const int QuickMissUnrollFactor = 1;
    public const int QuickMissIterationCount = 3;
    public const int QuickMissWarmupCount = 1;

    /// <summary>
    /// Extra batches beyond warmup + measured iterations: BenchmarkDotNet also invokes the workload in its JIT stage and,
    /// with <c>[MemoryDiagnoser]</c>, runs one more full batch after the actual run to measure allocations
    /// (<c>Engine.GetExtraStats</c>). A pool of warmup + iterations + 50 keys ran out in that extra batch. Keys are cheap,
    /// so size generously: 8 spare batches plus a flat 500.
    /// </summary>
    private const int BdnSpareBatches = 8;
    private const int BdnOverheadBuffer = 500;

    public static int BdnMissPoolSize(bool quick) => quick
        ? QuickMissInvocationCount * (QuickMissIterationCount + QuickMissWarmupCount + BdnSpareBatches) + BdnOverheadBuffer
        : MissInvocationCount * (MissIterationCount + MissWarmupCount + BdnSpareBatches) + BdnOverheadBuffer;

    // --- Sailfish: sample-size driven, no fixed invocation/iteration split -------------------------------------
    public const int SailfishHitMinimumSampleSize = 50;
    public const int SailfishHitMaximumSampleSize = 1000;
    public const int SailfishHitWarmupIterations = 20;
    public const double SailfishHitTargetCoefficientOfVariation = 0.02;

    public const int SailfishMissSampleSize = 1000;
    public const int SailfishMissWarmupIterations = 20;

    public const int QuickSailfishHitMinimumSampleSize = 20;
    public const int QuickSailfishHitMaximumSampleSize = 100;
    public const int QuickSailfishHitWarmupIterations = 5;

    public const int QuickSailfishMissSampleSize = 15;
    public const int QuickSailfishMissWarmupIterations = 2;

    /// <summary>Extra keys per contender beyond sample size + warmup, to absorb Sailfish's own overhead-estimation calls.</summary>
    private const int SailfishOverheadBufferPerContender = 10;

    /// <summary>
    /// One <see cref="MissKeyPool"/> is shared by every <c>[SailfishMethod]</c> in a miss-comparison class: unlike
    /// BenchmarkDotNet's out-of-process cases (one contender per process), Sailfish runs all six methods of a class
    /// against the same <c>[SailfishGlobalSetup]</c>-built instance, drawing from the same pool in turn. The pool
    /// must therefore cover all six contenders' draws, not just one.
    /// </summary>
    private const int ContenderKindsPerClass = 6;

    public static int SailfishMissPoolSize(bool quick) => ContenderKindsPerClass * ((quick
        ? QuickSailfishMissSampleSize + QuickSailfishMissWarmupIterations
        : SailfishMissSampleSize + SailfishMissWarmupIterations) + SailfishOverheadBufferPerContender);
}
