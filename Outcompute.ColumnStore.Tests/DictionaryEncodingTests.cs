using CommunityToolkit.HighPerformance.Buffers;

namespace Outcompute.ColumnStore.Tests;

public class DictionaryEncodingTests
{
    private readonly IServiceProvider _provider = new ServiceCollection()
        .AddSerializer()
        .AddColumnStore()
        .BuildServiceProvider();

    [Fact]
    public void RoundtripsEmpty()
    {
        // arrange
        var encoding = _provider.GetRequiredService<DictionaryEncoding<int>>();
        var data = Array.Empty<int>();

        // act - encode
        using var encoded = encoding.Encode(data);

        // assert - encoded
        Assert.Equal(2, encoded.Length);

        // act - decode
        using var decoded = encoding.Decode(encoded.Span);

        // assert - decoded
        Assert.True(data.SequenceEqual(decoded.Span.ToArray()));
    }

    [Fact]
    public void RoundtripsSmall()
    {
        // arrange
        var encoding = _provider.GetRequiredService<DictionaryEncoding<int>>();
        var data = new[] { 1, 2, 3, 1, 1, 1, 2, 2, 2, 3, 3, 3, 4 };

        // act - encode
        using var encoded = encoding.Encode(data);

        // assert - encoded
        Assert.Equal(26, encoded.Length);

        // act - decode
        using var decoded = encoding.Decode(encoded.Span);

        // assert - decoded
        Assert.True(data.SequenceEqual(decoded.Span.ToArray()));
    }

    [Fact]
    public void RoundtripsMaxPayloadWithMinCardinality()
    {
        // arrange
        var encoding = _provider.GetRequiredService<DictionaryEncoding<int>>();

        using var buffer = SpanOwner<int>.Allocate(1_000_000);
        var span = buffer.Span;
        for (var i = 0; i < span.Length; i++)
        {
            span[i] = 1;
        }

        // act - encode
        using var encoded = encoding.Encode(span);

        // assert - encoded
        Assert.Equal(12, encoded.Length);

        // act - decode
        using var decoded = encoding.Decode(encoded.Span);

        // assert - decoded
        Assert.True(span.SequenceEqual(decoded.Span));
    }

    [Fact]
    public void RoundtripsMaxPayloadWithSparseCardinality()
    {
        // arrange
        var encoding = _provider.GetRequiredService<DictionaryEncoding<int>>();

        using var buffer = SpanOwner<int>.Allocate(1_000_000);
        var span = buffer.Span;
        for (var i = 0; i < span.Length; i++)
        {
            span[i] = i % 1000;
        }

        // act - encode
        using var encoded = encoding.Encode(span);

        // assert - encoded
        Assert.Equal(2874945, encoded.Length);

        // act - decode
        using var decoded = encoding.Decode(encoded.Span);

        // assert - decoded
        Assert.True(span.SequenceEqual(decoded.Span));
    }

    [Fact]
    public void RoundtripsMaxPayloadWithMaxCardinality()
    {
        // arrange
        var encoding = _provider.GetRequiredService<DictionaryEncoding<int>>();

        using var buffer = SpanOwner<int>.Allocate(1_000_000);
        var span = buffer.Span;
        for (var i = 0; i < span.Length; i++)
        {
            span[i] = i;
        }

        // act - encode
        using var encoded = encoding.Encode(span);

        // assert - encoded
        Assert.Equal(7975242, encoded.Length);

        // act - decode
        using var decoded = encoding.Decode(encoded.Span);

        // assert - decoded
        Assert.True(span.SequenceEqual(decoded.Span));
    }
}