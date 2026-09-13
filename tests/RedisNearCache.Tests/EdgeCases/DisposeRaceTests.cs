using Microsoft.Extensions.DependencyInjection;
using Xunit;
using Xunit.Abstractions;

namespace RedisNearCache.Tests.EdgeCases;

/// <summary>
/// Disposing while startup is still in flight. <c>RedisNearCache.DisposeAsync</c> disposes the armer first
/// (which cancels the arm in flight) and then awaits the startup task, swallowing its fault, so a provider
/// torn down microseconds after it was built must not throw, leak an unobserved task exception, or hang.
/// Afterwards the facade must behave like any disposed object.
/// </summary>
public class DisposeRaceTests
{
    private readonly ITestOutputHelper _out;

    public DisposeRaceTests(ITestOutputHelper output) => _out = output;

    [Fact]
    public async Task DisposeImmediatelyAfterConstruction()
    {
        var unobserved = new List<Exception>();
        void OnUnobserved(object? sender, UnobservedTaskExceptionEventArgs e)
        {
            lock (unobserved) unobserved.Add(e.Exception);
            e.SetObserved();
        }

        TaskScheduler.UnobservedTaskException += OnUnobserved;
        try
        {
            for (var round = 1; round <= 5; round++)
            {
                var services = new ServiceCollection();
                services.AddRedisNearCache(StandaloneCacheFixture.ConnectionString);
                var provider = services.BuildServiceProvider();

                var cache = provider.GetRequiredService<IRedisNearCache>();
                // Deliberately NOT awaiting Ready: the arming pass is still running.
                var readyCompletedBeforeDispose = cache.Ready.IsCompleted;
                await provider.DisposeAsync();
                _out.WriteLine($"round {round}: Ready was {(readyCompletedBeforeDispose ? "already" : "not yet")} complete at dispose");

                await Assert.ThrowsAsync<ObjectDisposedException>(
                    async () => await cache.GetAsync<string>(TestHelpers.Key("dispose-race")));
                Assert.False(cache.TryGetLocal<string>(TestHelpers.Key("dispose-race"), out _));

                // Disposing twice is a no-op, not a second teardown.
                await cache.DisposeAsync();
                await provider.DisposeAsync();
            }

            // Give any task that faulted after dispose a chance to be collected and reported.
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
        }
        finally
        {
            TaskScheduler.UnobservedTaskException -= OnUnobserved;
        }

        lock (unobserved)
        {
            Assert.True(unobserved.Count == 0,
                "dispose-during-startup left unobserved task exceptions: " +
                string.Join(" | ", unobserved.Select(e => e.ToString())));
        }
    }
}
