using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using RedisNearCache.Tracking;
using Xunit;
using Facade = RedisNearCache.Caching.RedisNearCache;

namespace RedisNearCache.UnitTests;

/// <summary>
/// The mistake to expect from someone adopting <see cref="RedisNearCacheOptions.KeyNamespace"/> is to go on passing
/// full keys, which quietly become <c>ns:ns:key</c>. The cache says so once, as a warning, and still does what it
/// was asked: a key may legitimately begin with the same text as the namespace.
/// </summary>
public class KeyNamespaceDoubledKeyWarningTests
{
    private sealed class RecordingLogger : ILogger<Facade>
    {
        public List<(LogLevel Level, string Message)> Entries { get; } = [];
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            lock (Entries) Entries.Add((logLevel, formatter(state, exception)));
        }
    }

    private static async Task<(FakeMultiplexer Mux, Facade Cache, RecordingLogger Log)> StartAsync(string? keyNamespace)
    {
        var mux = new FakeMultiplexer("rnc-unit-" + Guid.NewGuid().ToString("N"));
        mux.Add(7000, isReplica: false);
        var connection = FakeRedis.Connection(mux);
        var armer = new TrackingArmer(connection, NullLogger<TrackingArmer>.Instance, Timeout.InfiniteTimeSpan);
        var log = new RecordingLogger();
        var cache = new Facade(connection, armer, new SilentListener(), Options.Create(new RedisNearCacheOptions { KeyNamespace = keyNamespace }), log);
        await cache.Ready;
        return (mux, cache, log);
    }

    private static int Warnings(RecordingLogger log)
    {
        lock (log.Entries) return log.Entries.Count(e => e.Level == LogLevel.Warning && e.Message.Contains("KeyNamespace", StringComparison.Ordinal));
    }

    [Fact]
    public async Task AKeyThatAlreadyCarriesTheNamespaceIsWarnedAboutOnceAndStillReadAsAsked()
    {
        var (mux, cache, log) = await StartAsync("app1:");
        await using var lifetime = cache;
        mux.StoredValue = "v";

        await cache.GetAsync<string>("order:9");
        Assert.Equal(0, Warnings(log));

        await cache.GetAsync<string>("app1:user:1");
        await cache.GetAsync<string>("app1:user:2");
        await cache.SetAsync("app1:user:3", "x");

        Assert.Equal(1, Warnings(log));
        lock (log.Entries) Assert.Contains(log.Entries, e => e.Message.Contains("app1:app1:user:1", StringComparison.Ordinal));
        // Warned, not rewritten: the namespace is still added, exactly as documented.
        Assert.Equal(1, mux.StringGetCallsFor("app1:app1:user:1"));
        Assert.Equal(0, mux.StringGetCallsFor("app1:user:1"));
    }

    [Fact]
    public async Task WithoutANamespaceNothingIsEverWarnedAbout()
    {
        var (mux, cache, log) = await StartAsync(null);
        await using var lifetime = cache;
        mux.StoredValue = "v";

        await cache.GetAsync<string>("app1:user:1");

        Assert.Equal(0, Warnings(log));
        Assert.Equal(1, mux.StringGetCallsFor("app1:user:1"));
    }
}
