namespace RedisNearCache.Tracking.Broadcast;

/// <summary>The RESP3 type of a <see cref="Resp3Value"/>.</summary>
internal enum Resp3Kind
{
    /// <summary>A null (<c>_</c>, <c>$-1</c> or <c>*-1</c>).</summary>
    Null,
    /// <summary>A simple string (<c>+</c>).</summary>
    SimpleString,
    /// <summary>A bulk string (<c>$</c>).</summary>
    BulkString,
    /// <summary>A verbatim string (<c>=</c>); <see cref="Resp3Value.Text"/> has the three-character format prefix stripped.</summary>
    Verbatim,
    /// <summary>An error, either inline (<c>-</c>) or a blob error (<c>!</c>).</summary>
    Error,
    /// <summary>An integer (<c>:</c>).</summary>
    Integer,
    /// <summary>A double (<c>,</c>).</summary>
    Double,
    /// <summary>A boolean (<c>#</c>).</summary>
    Boolean,
    /// <summary>A big number (<c>(</c>), kept verbatim in <see cref="Resp3Value.Text"/>.</summary>
    BigNumber,
    /// <summary>An array (<c>*</c>).</summary>
    Array,
    /// <summary>A set (<c>~</c>).</summary>
    Set,
    /// <summary>A map (<c>%</c>), flattened into <see cref="Resp3Value.Items"/> as key, value, key, value.</summary>
    Map,
    /// <summary>An out-of-band push (<c>&gt;</c>).</summary>
    Push,
}

/// <summary>
/// One decoded RESP3 frame. Deliberately small: the broadcast tracker only needs strings, integers and
/// aggregates, so every scalar is kept as its text (or its <see cref="Integer"/>/<see cref="Double"/>/
/// <see cref="Boolean"/> value) and aggregates keep their children in <see cref="Items"/>.
/// </summary>
internal sealed class Resp3Value
{
    private static readonly IReadOnlyList<Resp3Value> EmptyItems = Array.Empty<Resp3Value>();

    /// <summary>The single null instance; every RESP3 null spelling decodes to this.</summary>
    public static readonly Resp3Value Null = new(Resp3Kind.Null, null, 0, 0, false, null);

    private Resp3Value(Resp3Kind kind, string? text, long integer, double number, bool boolean, IReadOnlyList<Resp3Value>? items)
    {
        Kind = kind;
        Text = text;
        Integer = integer;
        Double = number;
        Boolean = boolean;
        Items = items ?? EmptyItems;
    }

    /// <summary>The RESP3 type this frame was decoded from.</summary>
    public Resp3Kind Kind { get; }

    /// <summary>The text of a string-like frame (simple, bulk, verbatim, error, big number); null otherwise.</summary>
    public string? Text { get; }

    /// <summary>The value of an <see cref="Resp3Kind.Integer"/> frame; zero otherwise.</summary>
    public long Integer { get; }

    /// <summary>The value of a <see cref="Resp3Kind.Double"/> frame; zero otherwise.</summary>
    public double Double { get; }

    /// <summary>The value of a <see cref="Resp3Kind.Boolean"/> frame; false otherwise.</summary>
    public bool Boolean { get; }

    /// <summary>The children of an aggregate frame (empty for scalars). A map is flattened to key, value, key, value.</summary>
    public IReadOnlyList<Resp3Value> Items { get; }

    /// <summary>True for a RESP3 null, in any of its spellings.</summary>
    public bool IsNull => Kind == Resp3Kind.Null;

    /// <summary>True for an error frame; <see cref="Text"/> then holds the server's message.</summary>
    public bool IsError => Kind == Resp3Kind.Error;

    /// <summary>Creates a string-like frame.</summary>
    public static Resp3Value String(Resp3Kind kind, string text) => new(kind, text, 0, 0, false, null);

    /// <summary>Creates an integer frame.</summary>
    public static Resp3Value FromInteger(long value) => new(Resp3Kind.Integer, null, value, 0, false, null);

    /// <summary>Creates a double frame.</summary>
    public static Resp3Value FromDouble(double value) => new(Resp3Kind.Double, null, 0, value, false, null);

    /// <summary>Creates a boolean frame.</summary>
    public static Resp3Value FromBoolean(bool value) => new(Resp3Kind.Boolean, null, 0, 0, value, null);

    /// <summary>Creates an aggregate frame (array, set, map or push).</summary>
    public static Resp3Value Aggregate(Resp3Kind kind, IReadOnlyList<Resp3Value> items) => new(kind, null, 0, 0, false, items);

    /// <summary>
    /// The value stored under <paramref name="name"/> in a map frame (case-insensitive on the key), or null when
    /// this is not a map or the key is absent. Used to read the fields of <c>HELLO</c> and <c>CLIENT TRACKINGINFO</c>.
    /// </summary>
    public Resp3Value? MapValue(string name)
    {
        if (Kind != Resp3Kind.Map) return null;
        for (var i = 0; i + 1 < Items.Count; i += 2)
        {
            if (string.Equals(Items[i].Text, name, StringComparison.OrdinalIgnoreCase)) return Items[i + 1];
        }

        return null;
    }

    /// <summary>A short, log-friendly rendering; aggregates show their element count rather than their contents.</summary>
    public override string ToString() => Kind switch
    {
        Resp3Kind.Null => "(null)",
        Resp3Kind.Integer => Integer.ToString(System.Globalization.CultureInfo.InvariantCulture),
        Resp3Kind.Double => Double.ToString(System.Globalization.CultureInfo.InvariantCulture),
        Resp3Kind.Boolean => Boolean ? "true" : "false",
        Resp3Kind.Array or Resp3Kind.Set or Resp3Kind.Map or Resp3Kind.Push => $"{Kind}[{Items.Count}]",
        _ => Text ?? string.Empty,
    };
}
