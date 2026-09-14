using System.Globalization;
using System.Runtime.CompilerServices;

namespace RedisNearCache.Tests;

/// <summary>
/// Raises the thread pool's minimum worker threads before any test runs. While a Sentinel-managed connection of the
/// private multiplexer is down, StackExchange.Redis retries the primary switch from a one-second timer whose callback
/// blocks a pool thread (<c>SwitchPrimary</c> waits on <c>ReconfigureAsync</c>), and the callbacks overlap. Sampling
/// the pool during the graceful failover test with 4 cores showed it starved from the first <c>ConnectionFailed</c>
/// to the last <c>ConnectionRestored</c> (8-20 s), with the runtime adding about one thread per 500 ms; replies already
/// on the socket then go unread until commands time out (release CI run 34835227690: <c>in: 373</c>,
/// <c>WORKER: (Busy=11, Min=4)</c>). A higher minimum is StackExchange.Redis's own guidance for that symptom.
/// </summary>
/// <remarks>
/// <c>RNC_TEST_MIN_WORKER_THREADS</c> overrides the value; <c>0</c> keeps the runtime default, which together with
/// <c>DOTNET_PROCESSOR_COUNT=4</c> reproduces the CI runner's pool.
/// </remarks>
internal static class ThreadPoolSetup
{
    public const int DefaultMinWorkerThreads = 32;

#pragma warning disable CA2255 // The test assembly is the application here, not a library.
    [ModuleInitializer]
#pragma warning restore CA2255
    internal static void RaiseMinWorkerThreads()
    {
        var configured = Environment.GetEnvironmentVariable("RNC_TEST_MIN_WORKER_THREADS");
        var min = int.TryParse(configured, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed) ? parsed : DefaultMinWorkerThreads;
        if (min <= 0) return;

        ThreadPool.GetMinThreads(out var worker, out var io);
        if (worker < min) ThreadPool.SetMinThreads(min, io);
    }

    public static string Describe()
    {
        ThreadPool.GetMinThreads(out var worker, out _);
        return $"thread pool min workers {worker}, processors {Environment.ProcessorCount}";
    }
}
