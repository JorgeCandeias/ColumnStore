using CommunityToolkit.HighPerformance.Buffers;

namespace Outcompute.ColumnStore.Tests;

public class DefaultEncodingTests
{
    private readonly IServiceProvider _provider = new ServiceCollection()
        .AddSerializer()
        .AddColumnStore()
        .BuildServiceProvider();

    internal class RoundtripInt32Data : TheoryData<int, int, IEnumerable<int>>
    {
        public RoundtripInt32Data()
        {
            // empty set
            Add(2, 0, Array.Empty<int>());

            // small set
            Add(28, 13, new[] { 1, 2, 3, 1, 1, 1, 2, 2, 2, 3, 3, 3, 4 });

            // large set with min cardinality
            Add(2000004, 1_000_000, Enumerable.Repeat(1, 1_000_000));

            // large set with sparse cardinality
            Add(2360005, 1_000_000, Enumerable.Range(1, 1_000_000).Select(x => x / 10000));

            // large set with wave cardinality
            Add(3174404, 1_000_000, Enumerable.Range(1, 1_000_000).Select(x => x % 10000));

            // large set with max cardinality
            Add(3991750, 1_000_000, Enumerable.Range(1, 1_000_000));
        }
    }

    internal class RoundtripStringData : TheoryData<int, int, IEnumerable<string>>
    {
        public RoundtripStringData()
        {
            // empty set
            Add(2, 0, Array.Empty<string>());

            // small set
            Add(32, 13, new[] { "A", "B", "C", "A", "A", "A", "B", "B", "B", "C", "C", "C", "D" });

            // large set with min cardinality
            Add(2000009, 1_000_000, Enumerable.Repeat("Value", 1_000_000));

            // large set with sparse cardinality
            Add(8900006, 1_000_000, Enumerable.Range(1, 1_000_000).Select(x => $"Value{x / 10000}"));

            // large set with wave cardinality
            Add(10889004, 1_000_000, Enumerable.Range(1, 1_000_000).Select(x => $"Value{x % 10000}"));

            // large set with max cardinality
            Add(12888900, 1_000_000, Enumerable.Range(1, 1_000_000).Select(x => $"Value{x}"));
        }
    }

    private void RoundtripCore<T>(int expectedLength, int bufferLength, IEnumerable<T> source)
    {
        // materialize the test data
        using var owner = SpanOwner<T>.Allocate(bufferLength);
        var data = owner.Span;
        var added = 0;
        foreach (var item in source)
        {
            data[added++] = item;
        }
        Assert.Equal(added, bufferLength);

        // arrange
        var encoding = _provider.GetRequiredService<DefaultEncoding<T>>();

        // act - encode
        using var encoded = encoding.Encode(data);

        // assert - encoded
        Assert.Equal(expectedLength, encoded.Length);

        // act - decode
        using var decoded = encoding.Decode(encoded.Span);

        // assert - decoded
        Assert.True(data.SequenceEqual(decoded.Span));
    }

    [Theory]
    [ClassData(typeof(RoundtripInt32Data))]
    public void RoundtripInt32(int expectedLength, int bufferLength, IEnumerable<int> source)
    {
        RoundtripCore(expectedLength, bufferLength, source);
    }

    [Theory]
    [ClassData(typeof(RoundtripStringData))]
    public void RoundtripString(int expectedLength, int bufferLength, IEnumerable<string> source)
    {
        RoundtripCore(expectedLength, bufferLength, source);
    }

    internal class QueryByValueInt32Data : TheoryData<int[], int, ValueRange<int>[]>
    {
        public QueryByValueInt32Data()
        {
            // empty
            Add(Array.Empty<int>(), 1, Array.Empty<ValueRange<int>>());

            // first item
            Add(new int[] { 1, 2, 3, 4, 5 }, 1, new[] { new ValueRange<int>(1, 0, 1) });

            // middle item
            Add(new int[] { 1, 2, 3, 4, 5 }, 3, new[] { new ValueRange<int>(3, 2, 1) });

            // last item
            Add(new int[] { 1, 2, 3, 4, 5 }, 5, new[] { new ValueRange<int>(5, 4, 1) });

            // first items
            Add(new int[] { 1, 1, 1, 4, 5 }, 1, new[] { new ValueRange<int>(1, 0, 3) });

            // middle items
            Add(new int[] { 1, 3, 3, 3, 5 }, 3, new[] { new ValueRange<int>(3, 1, 3) });

            // last items
            Add(new int[] { 1, 2, 5, 5, 5 }, 5, new[] { new ValueRange<int>(5, 2, 3) });

            // mixed single item
            Add(new int[] { 1, 2, 1, 5, 1 }, 1, new[]
            {
                new ValueRange<int>(1, 0, 1),
                new ValueRange<int>(1, 2, 1),
                new ValueRange<int>(1, 4, 1),
            });

            // mixed multiple items
            Add(new int[] { 1, 1, 1, 2, 3, 4, 1, 1, 1, 5, 6, 7, 1, 1, 1 }, 1, new[]
            {
                new ValueRange<int>(1, 0, 3),
                new ValueRange<int>(1, 6, 3),
                new ValueRange<int>(1, 12, 3),
            });
        }
    }

    [Theory]
    [ClassData(typeof(QueryByValueInt32Data))]
    internal void QueryByValueInt32(int[] source, int value, ValueRange<int>[] expected)
    {
        // arrange
        var encoding = _provider.GetRequiredService<DefaultEncoding<int>>();
        using var encoded = encoding.Encode(source.AsSpan());

        // act
        var result = encoding.Decode(encoded.Span, value);

        // assert
        Assert.True(result.Span.SequenceEqual(expected.AsSpan()));
    }

    /*
    private void RoundtripCore<T>(IEnumerable<T> source, T value, IEnumerable<ValueRange<int>> expected)
    {
        // materialize the test data
        using var owner = SpanOwner<T>.Allocate(bufferLength);
        var data = owner.Span;
        var added = 0;
        foreach (var item in source)
        {
            data[added++] = item;
        }
        Assert.Equal(added, bufferLength);

        // arrange
        var encoding = _provider.GetRequiredService<DefaultEncoding<T>>();

        // act - encode
        using var encoded = encoding.Encode(data);

        // assert - encoded
        Assert.Equal(expectedLength, encoded.Length);

        // act - decode
        using var decoded = encoding.Decode(encoded.Span);

        // assert - decoded
        Assert.True(data.SequenceEqual(decoded.Span));
    }
    */
}