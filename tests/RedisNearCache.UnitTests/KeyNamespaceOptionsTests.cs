using Xunit;

namespace RedisNearCache.UnitTests;

/// <summary>
/// <see cref="RedisNearCacheOptions.EffectiveKeyPrefixes"/> on its own: the one place that decides what Redis is
/// told about (the Broadcast tracker's <c>BCAST PREFIX</c> list) and what the facade will cache (its own prefix
/// filter). The two read the same method, so a disagreement between them is impossible by construction - what is
/// left to pin is the mapping itself, and above all that a cache with NO namespace gets
/// <see cref="RedisNearCacheOptions.KeyPrefixes"/> back unchanged, which is what every existing user has.
/// </summary>
public class KeyNamespaceOptionsTests
{
    [Fact]
    public void WithoutANamespaceTheResultIsKeyPrefixesElementForElement()
    {
        var options = new RedisNearCacheOptions();
        options.KeyPrefixes.Add("user:");
        options.KeyPrefixes.Add("order:");
        Assert.Null(options.KeyNamespace);

        var effective = options.EffectiveKeyPrefixes();

        Assert.Equal(options.KeyPrefixes.Count, effective.Length);
        for (var i = 0; i < effective.Length; i++) Assert.Equal(options.KeyPrefixes[i], effective[i]);
        Assert.Equal(new[] { "user:", "order:" }, effective);
    }

    [Fact]
    public void WithoutANamespaceAndWithoutPrefixesTheResultIsEmpty()
    {
        var options = new RedisNearCacheOptions();

        Assert.Empty(options.EffectiveKeyPrefixes());
    }

    [Fact]
    public void ANamespaceWithNoPrefixesIsTheNamespaceAlone()
    {
        var options = new RedisNearCacheOptions { KeyNamespace = "app1:" };

        Assert.Equal(new[] { "app1:" }, options.EffectiveKeyPrefixes());
    }

    [Fact]
    public void ANamespaceWithPrefixesPutsItInFrontOfEachOfThem()
    {
        var options = new RedisNearCacheOptions { KeyNamespace = "app1:" };
        options.KeyPrefixes.Add("user:");
        options.KeyPrefixes.Add("order:");

        Assert.Equal(new[] { "app1:user:", "app1:order:" }, options.EffectiveKeyPrefixes());
    }

    /// <summary>
    /// null and "" are both "off": byte for byte what a cache without the option ever saw. A test that only covered
    /// null would not catch an implementation that treated "" as a real (empty) namespace and turned an empty
    /// <see cref="RedisNearCacheOptions.KeyPrefixes"/> into <c>[""]</c> - i.e. a prefix matching every key, which
    /// in Broadcast mode is a different arm command.
    /// </summary>
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public void ANullOrEmptyNamespaceIsOff(string? keyNamespace)
    {
        var withPrefixes = new RedisNearCacheOptions { KeyNamespace = keyNamespace };
        withPrefixes.KeyPrefixes.Add("user:");

        Assert.Equal(new[] { "user:" }, withPrefixes.EffectiveKeyPrefixes());
        Assert.Empty(new RedisNearCacheOptions { KeyNamespace = keyNamespace }.EffectiveKeyPrefixes());
    }

    /// <summary>A fresh array every call: the caller (the facade, the tracker) keeps it and must not share it.</summary>
    [Fact]
    public void EachCallReturnsItsOwnArray()
    {
        var options = new RedisNearCacheOptions { KeyNamespace = "app1:" };
        options.KeyPrefixes.Add("user:");

        Assert.NotSame(options.EffectiveKeyPrefixes(), options.EffectiveKeyPrefixes());
    }

    /// <summary>
    /// The namespace is not required to end in a separator, and nothing about a prefix is normalised: the two are
    /// concatenated exactly as given, which is the rule the invalidation path relies on (the server's key is the
    /// concatenation, and nothing translates it back).
    /// </summary>
    [Fact]
    public void ConcatenationIsVerbatim()
    {
        var options = new RedisNearCacheOptions { KeyNamespace = "tenant-7" };
        options.KeyPrefixes.Add(":user:");

        Assert.Equal(new[] { "tenant-7:user:" }, options.EffectiveKeyPrefixes());
    }
}
