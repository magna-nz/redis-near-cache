using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace RedisNearCache.Samples.Worker;

/// <summary>
/// Every 500 ms, reads <c>config:feature-flags</c> through <see cref="IRedisNearCache"/> and logs whether that
/// read was an L1 hit or a Redis miss. Because the key is only invalidated when something outside this process
/// writes it, ticks are misses only right after such a write (the first read after invalidation re-populates
/// L1, so every following tick until the next external write is a hit). Prints the full
/// <see cref="RedisNearCacheStatistics"/> snapshot every 5 seconds.
/// </summary>
public sealed class ConfigPollingWorker(IRedisNearCache cache, ILogger<ConfigPollingWorker> logger) : BackgroundService
{
    private const string Key = "config:feature-flags";
    private static readonly TimeSpan PollInterval = TimeSpan.FromMilliseconds(500);
    private static readonly TimeSpan StatsInterval = TimeSpan.FromSeconds(5);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await cache.Ready;

        var lastStatsPrint = DateTimeOffset.UtcNow;

        using var timer = new PeriodicTimer(PollInterval);
        do
        {
            var missesBefore = cache.Statistics.Misses;

            var value = await cache.GetAsync<string>(Key, stoppingToken);

            var wasMiss = cache.Statistics.Misses > missesBefore;
            var outcome = wasMiss ? "MISS" : "hit ";

            logger.LogInformation(
                "{Outcome} {Key} = {Value}  (cumulative hits={Hits} misses={Misses})",
                outcome, Key, value ?? "<null>", cache.Statistics.Hits, cache.Statistics.Misses);

            if (DateTimeOffset.UtcNow - lastStatsPrint >= StatsInterval)
            {
                logger.LogInformation("Statistics: {Statistics}", cache.Statistics);
                lastStatsPrint = DateTimeOffset.UtcNow;
            }
        }
        while (await timer.WaitForNextTickAsync(stoppingToken));
    }
}
