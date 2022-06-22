using CommunityToolkit.HighPerformance.Buffers;

namespace Outcompute.ColumnStore.Tests;

public class DefaultEncodingTests
{
    private readonly IServiceProvider _provider = new ServiceCollection()
        .AddSerializer()
        .AddColumnStore()
        .BuildServiceProvider();

    public class RoundtripInt32Data : TheoryData<int, int, IEnumerable<int>>
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

    public class RoundtripStringData : TheoryData<int, int, IEnumerable<string>>
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
}