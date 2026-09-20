using System.Text;
using System.Text.Json;
using Xunit;

namespace RedisNearCache.UnitTests;

/// <summary>
/// What <c>Serialize&lt;T&gt;</c> writes is what <c>Deserialize&lt;T&gt;</c> for the same <c>T</c> must read back.
/// The default serializer used to dispatch <c>Serialize</c> on the runtime value and <c>Deserialize</c> on
/// <c>typeof(T)</c>, so the two disagreed in both directions: a declaration pattern never matches <c>null</c>, so
/// <c>Serialize&lt;string&gt;(null)</c> fell through to JSON and wrote the four bytes <c>null</c>, which came back as
/// the four-character string "null" - a value the caller never stored - and <c>Serialize&lt;object&gt;("abc")</c>
/// wrote raw bytes that <c>Deserialize&lt;object&gt;</c> could only throw on.
/// </summary>
public class SerializerSymmetryTests
{
    private static readonly JsonRedisNearCacheSerializer Serializer = JsonRedisNearCacheSerializer.Instance;

    private sealed record Person(string Name, int Age);

    [Fact]
    public void ANullStringIsRefusedRatherThanStoredAsTheTextNull()
    {
        // string passes through untouched, so there is nothing in Redis that means "null": bytes, or no key at all.
        var ex = Assert.Throws<ArgumentNullException>(() => Serializer.Serialize<string>(null!));
        Assert.Equal("value", ex.ParamName);
    }

    [Fact]
    public void ANullByteArrayIsRefusedRatherThanStoredAsTheTextNull()
    {
        var ex = Assert.Throws<ArgumentNullException>(() => Serializer.Serialize<byte[]>(null!));
        Assert.Equal("value", ex.ParamName);
    }

    [Fact]
    public void TheStringAndByteArrayPassThroughStillRoundTrips()
    {
        var bytes = Serializer.Serialize("hello");
        Assert.Equal("hello", Encoding.UTF8.GetString(bytes)); // readable by any other client, not JSON-quoted
        Assert.Equal("hello", Serializer.Deserialize<string>(bytes));

        var raw = new byte[] { 1, 2, 3, 0, 255 };
        Assert.Same(raw, Serializer.Serialize(raw)); // no copy on the way out
        Assert.Equal(raw, Serializer.Deserialize<byte[]>(raw));
    }

    [Fact]
    public void AStringStoredAsObjectRoundTripsAsObject()
    {
        // Dispatching on the runtime value wrote raw bytes here, which Deserialize<object> then threw on.
        var bytes = Serializer.Serialize<object>("abc");
        var back = Serializer.Deserialize<object>(bytes);
        Assert.NotNull(back);
        Assert.Equal("abc", Assert.IsType<JsonElement>(back).GetString());
    }

    [Fact]
    public void AnEmptyStringAndAnEmptyArrayAreNotNull()
    {
        Assert.Equal(string.Empty, Serializer.Deserialize<string>(Serializer.Serialize(string.Empty)));
        Assert.Empty(Serializer.Deserialize<byte[]>(Serializer.Serialize(Array.Empty<byte>()))!);
    }

    [Fact]
    public void ANullReferenceOfAJsonTypeIsStillAllowedAndComesBackNull()
    {
        // Unlike string/byte[], JSON has a null literal, so this is representable and stays legal.
        var bytes = Serializer.Serialize<Person?>(null);
        Assert.Equal("null", Encoding.UTF8.GetString(bytes));
        Assert.Null(Serializer.Deserialize<Person?>(bytes));
    }

    [Fact]
    public void AJsonTypeRoundTrips()
    {
        var person = new Person("Ada", 36);
        Assert.Equal(person, Serializer.Deserialize<Person>(Serializer.Serialize(person)));
    }

    [Theory]
    [InlineData("plain")]
    [InlineData("with \"quotes\" and \\backslashes\\")]
    [InlineData("unicode: é中文\U0001f600")]
    public void StringsThatJsonWouldEscapeStillPassThroughUnchanged(string value)
    {
        Assert.Equal(value, Serializer.Deserialize<string>(Serializer.Serialize(value)));
    }
}
