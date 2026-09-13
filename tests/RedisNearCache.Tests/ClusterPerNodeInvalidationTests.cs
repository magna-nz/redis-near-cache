using System.Net;
using Xunit;

namespace RedisNearCache.Tests;

public class ClusterPerNodeInvalidationTests : IClassFixture<ClusterCacheFixture>
{
    private readonly ClusterCacheFixture _fx;

    public ClusterPerNodeInvalidationTests(ClusterCacheFixture fx) => _fx = fx;

    [Fact]
    public async Task ClusterPerNodeInvalidation()
    {
        var mux = _fx.Connection.Multiplexer;
        var endpoints = mux.GetEndPoints();
        var anyServer = mux.GetServer(endpoints[0]);

        var keyByEndpoint = TestHelpers.KeyPerMaster(mux, anyServer, endpoints);
        Assert.Equal(3, keyByEndpoint.Count);

        foreach (var (endpoint, key) in keyByEndpoint)
        {
            var port = ((IPEndPoint)endpoint).Port;

            await _fx.Cache.SetAsync(key, "v1");
            Assert.Equal("v1", await _fx.Cache.GetAsync<string>(key));
            Assert.True(_fx.Cache.TryGetLocal<string>(key, out _));

            RedisCli.Cluster(port, "SET", key, "v2");

            var evicted = await Poll.UntilAsync(() => !_fx.Cache.TryGetLocal<string>(key, out _));
            Assert.True(evicted, $"key on master port {port} was not evicted after external write.");
        }

        Assert.Equal(3, _fx.Armer.RedirectTargets.Count);
    }
}
