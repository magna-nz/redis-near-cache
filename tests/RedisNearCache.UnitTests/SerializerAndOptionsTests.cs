using System.Text;
using System.Text.Json;
using RedisNearCache.Internal;
using StackExchange.Redis;
using Xunit;

namespace RedisNearCache.UnitTests;

public class SerializerAndOptionsTests
{
    private record Person(string Name, int Age);

    [Fact]
    public void StringAndBytesPassThrough()
    {
        var s = JsonRedisNearCacheSerializer.Instance;
        Assert.Equal(Encoding.UTF8.GetBytes("héllo"), s.Serialize("héllo"));
        Assert.Equal("héllo", s.Deserialize<string>(Encoding.UTF8.GetBytes("héllo")));
        var raw = Enumerable.Range(0, 256).Select(i => (byte)i).ToArray();
        Assert.Same(raw, s.Serialize(raw));
        Assert.Equal(raw, s.Deserialize<byte[]>(raw));
    }

    [Fact]
    public void ObjectsRoundTripAsJson()
    {
        var s = JsonRedisNearCacheSerializer.Instance;
        var bytes = s.Serialize(new Person("Ada", 36));
        Assert.Equal("{\"Name\":\"Ada\",\"Age\":36}", Encoding.UTF8.GetString(bytes));
        Assert.Equal(new Person("Ada", 36), s.Deserialize<Person>(bytes));
    }

    [Fact]
    public void CustomJsonOptionsAreHonoured()
    {
        var s = new JsonRedisNearCacheSerializer(new JsonSerializerOptions(JsonSerializerDefaults.Web));
        Assert.Equal("{\"name\":\"Ada\",\"age\":36}", Encoding.UTF8.GetString(s.Serialize(new Person("Ada", 36))));
        Assert.Equal(new Person("Ada", 36), s.Deserialize<Person>(Encoding.UTF8.GetBytes("{\"NAME\":\"Ada\",\"age\":36}")));
    }

    [Fact]
    public void BuildConfigurationForcesResp2AndAdminWithoutMutatingTheCallersOptions()
    {
        var callers = ConfigurationOptions.Parse("localhost:6379");
        callers.Protocol = RedisProtocol.Resp3;
        callers.AllowAdmin = false;
        callers.ClientName = "my-app";

        var cfg = RedisNearCacheConnection.BuildConfiguration(new RedisNearCacheOptions { Configuration = callers, ClientNamePrefix = "rnc" });

        Assert.Equal(RedisProtocol.Resp2, cfg.Protocol);
        Assert.True(cfg.AllowAdmin);
        Assert.StartsWith("rnc-", cfg.ClientName);
        Assert.NotSame(callers, cfg);
        Assert.Equal(RedisProtocol.Resp3, callers.Protocol);
        Assert.False(callers.AllowAdmin);
        Assert.Equal("my-app", callers.ClientName);
        Assert.Equal(callers.EndPoints.Count, cfg.EndPoints.Count);
    }

    [Fact]
    public void BuildConfigurationFromConnectionString()
    {
        var cfg = RedisNearCacheConnection.BuildConfiguration(new RedisNearCacheOptions { ConnectionString = "a:6379,b:6380,password=pw" });
        Assert.Equal(2, cfg.EndPoints.Count);
        Assert.Equal("pw", cfg.Password);
        Assert.Equal(RedisProtocol.Resp2, cfg.Protocol);
        Assert.True(cfg.AllowAdmin);
    }

    [Fact]
    public void BuildConfigurationWithoutAnySourceThrows()
    {
        Assert.Throws<InvalidOperationException>(() => RedisNearCacheConnection.BuildConfiguration(new RedisNearCacheOptions()));
    }

    [Fact]
    public void EachBuildGetsAUniqueClientName()
    {
        var o = new RedisNearCacheOptions { ConnectionString = "localhost" };
        var a = RedisNearCacheConnection.BuildConfiguration(o).ClientName;
        var b = RedisNearCacheConnection.BuildConfiguration(o).ClientName;
        Assert.NotEqual(a, b);
    }

    [Fact]
    public void DefaultsAreAsDocumented()
    {
        var o = new RedisNearCacheOptions();
        Assert.Empty(o.KeyPrefixes);
        Assert.Equal(10_000, o.L1SizeLimit);
        Assert.Equal(TimeSpan.FromMinutes(5), o.L1MaxAge);
        Assert.Same(JsonRedisNearCacheSerializer.Instance, o.Serializer);
        Assert.Equal("rnc", o.ClientNamePrefix);
    }

    [Fact]
    public void StatisticsCountAndPrint()
    {
        var s = new RedisNearCacheStatistics();
        s.Hit(); s.Hit(); s.Miss(); s.Invalidation(); s.Flush(); s.Rearm(); s.RaceDiscard();
        Assert.Equal(2, s.Hits);
        Assert.Equal(1, s.Misses);
        Assert.Equal("hits=2 misses=1 invalidations=1 flushes=1 rearms=1 raceDiscards=1", s.ToString());
    }
}
