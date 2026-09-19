using System.Net;
using StackExchange.Redis;
using Xunit;

namespace RedisNearCache.Tests;

/// <summary>The conditional-write overloads against the 3-master cluster: NX/XX must resolve correctly no
/// matter which master a key's slot lands on, exactly as an unconditional write already does per
/// <see cref="ClusterPerNodeInvalidationTests"/>.</summary>
public class ConditionalWriteClusterTests : IClassFixture<ClusterCacheFixture>
{
    private readonly ClusterCacheFixture _fx;

    public ConditionalWriteClusterTests(ClusterCacheFixture fx) => _fx = fx;

    [Fact]
    public async Task ConditionalWritesRouteToTheRightMasterAcrossTheCluster()
    {
        var mux = _fx.Connection.Multiplexer;
        var endpoints = mux.GetEndPoints();
        var anyServer = mux.GetServer(endpoints[0]);

        var keyByEndpoint = TestHelpers.KeyPerMaster(mux, anyServer, endpoints);
        Assert.Equal(3, keyByEndpoint.Count);

        foreach (var (endpoint, key) in keyByEndpoint)
        {
            var port = ((IPEndPoint)endpoint).Port;
            try
            {
                Assert.True(await _fx.Cache.SetAsync(key, "v1", When.NotExists),
                    $"NotExists on an absent key should succeed on master port {port}.");
                Assert.Equal("v1", RedisCli.Cluster(port, "GET", key));

                Assert.False(await _fx.Cache.SetAsync(key, "v2", When.NotExists),
                    $"NotExists on an existing key should fail on master port {port}.");
                Assert.Equal("v1", RedisCli.Cluster(port, "GET", key));

                Assert.True(await _fx.Cache.SetAsync(key, "v3", When.Exists),
                    $"Exists on an existing key should succeed on master port {port}.");
                Assert.Equal("v3", RedisCli.Cluster(port, "GET", key));
            }
            finally
            {
                RedisCli.Cluster(port, "DEL", key);
            }
        }
    }
}
