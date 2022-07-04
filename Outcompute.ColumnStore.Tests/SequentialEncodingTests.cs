using CommunityToolkit.HighPerformance.Buffers;

namespace Outcompute.ColumnStore.Tests;

public class SequentialEncodingTests
{
    private readonly IServiceProvider _provider = new ServiceCollection()
        .AddSerializer()
        .AddColumnStore()
        .BuildServiceProvider();

    [Theory]
    [ClassData(typeof(RoundtripInt32Data))]
    public void RoundtripInt32(int expectedLength, int bufferLength, IEnumerable<int> source)
    {
        RoundtripCore(expectedLength, bufferLength, source);
    }

    [Theory]
    [ClassData(typeof(RoundtripInt16Data))]
    public void RoundtripInt16(int expectedLength, int bufferLength, IEnumerable<short> source)
    {
        RoundtripCore(expectedLength, bufferLength, source);
    }

    private void RoundtripCore<T>(int expectedLength, int bufferLength, IEnumerable<T> source)
        where T : unmanaged
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
        var encoding = _provider.GetRequiredService<SequentialEncoding<T>>();

        // act - encode
        using var writer = new ArrayPoolBufferWriter<byte>();
        encoding.Encode(data, writer);
        var encoded = writer.WrittenSpan;

        // assert - encoded
        Assert.Equal(expectedLength, encoded.Length);

        // act - decode
        using var decoded = encoding.Decode(encoded);

        // assert - decoded
        Assert.True(data.SequenceEqual(decoded.Memory.Span));
    }

    internal class RoundtripInt32Data : TheoryData<
        /* expected bytes */ int,
        /* source count */ int,
        /* source generator */ IEnumerable<int>>
    {
        public RoundtripInt32Data()
        {
            // empty set
            Add(2, 0, Array.Empty<int>());

            // small set
            Add(15, 13, new[] { 1, 2, 3, 1, 1, 1, 2, 2, 2, 3, 3, 3, 4 });

            // negative value set
            Add(17, 3, new[] { -9, -8, -7 });

            // large set with min cardinality
            Add(1000004, 1_000_000, Enumerable.Repeat(1, 1_000_000));

            // large set with sparse cardinality
            Add(1000004, 1_000_000, Enumerable.Range(1, 1_000_000).Select(x => x / 10000));

            // large set with wave cardinality
            Add(1987204, 1_000_000, Enumerable.Range(1, 1_000_000).Select(x => x % 10000));

            // large set with max cardinality
            Add(2983494, 1_000_000, Enumerable.Range(1, 1_000_000));
        }
    }

    internal class RoundtripInt16Data : TheoryData<
    /* expected bytes */ int,
    /* source count */ int,
    /* source generator */ IEnumerable<short>>
    {
        public RoundtripInt16Data()
        {
            // empty set
            Add(2, 0, Array.Empty<short>());

            // small set
            Add(15, 13, new short[] { 1, 2, 3, 1, 1, 1, 2, 2, 2, 3, 3, 3, 4 });

            // negative value set
            Add(17, 3, new short[] { -9, -8, -7 });

            // large set with min cardinality
            Add(1000004, 1_000_000, Enumerable.Repeat<short>(1, 1_000_000));

            // large set with sparse cardinality
            Add(1000004, 1_000_000, Enumerable.Range(1, 1_000_000).Select(x => x / 10000).Cast<short>());

            // large set with wave cardinality
            Add(1987204, 1_000_000, Enumerable.Range(1, 1_000_000).Select(x => x % 10000).Cast<short>());

            // large set with max cardinality
            Add(2983494, 1_000_000, Enumerable.Range(1, 1_000_000).Cast<short>());
        }
    }

    [Theory]
    [ClassData(typeof(QueryByValueInt32Data))]
    internal void QueryByValueInt32(int[] source, int value, ValueRange<int>[] expected)
    {
        // arrange
        var encoding = _provider.GetRequiredService<SequentialEncoding<int>>();
        using var writer = new ArrayPoolBufferWriter<byte>();
        encoding.Encode(source.AsSpan(), writer);
        var encoded = writer.WrittenSpan;

        // act
        var result = encoding.Decode(encoded, value);

        // assert
        Assert.True(result.Memory.Span.SequenceEqual(expected.AsSpan()));
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
}