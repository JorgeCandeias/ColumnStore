using CommunityToolkit.HighPerformance.Buffers;

namespace Outcompute.ColumnStore.Tests;

public class DefaultEncodingTests
{
    private readonly IServiceProvider _provider = new ServiceCollection()
        .AddSerializer()
        .AddColumnStore()
        .BuildServiceProvider();

    public class RoundtripData : TheoryData<int, int, IEnumerable<int>>
    {
        public RoundtripData()
        {
            // empty value set
            Add(2, 0, Array.Empty<int>());

            // small value set
            Add(28, 13, new[] { 1, 2, 3, 1, 1, 1, 2, 2, 2, 3, 3, 3, 4 });

            // large value set with min cardinality
            Add(2000004, 1_000_000, Enumerable.Repeat(1, 1_000_000));

            // large value set with sparse cardinality
            Add(2936004, 1_000_000, Enumerable.Range(1, 1_000_000).Select(x => x % 1000));

            // large value set with max cardinality
            Add(3991750, 1_000_000, Enumerable.Range(1, 1_000_000));
        }
    }

    [Theory]
    [ClassData(typeof(RoundtripData))]
    public void Roundtrips(int expectedLength, int bufferLength, IEnumerable<int> source)
    {
        // materialize the test data
        using var owner = SpanOwner<int>.Allocate(bufferLength);
        var data = owner.Span;
        var added = 0;
        foreach (var item in source)
        {
            data[added++] = item;
        }
        Assert.Equal(added, bufferLength);

        // arrange
        var encoding = _provider.GetRequiredService<DefaultEncoding<int>>();

        // act - encode
        using var encoded = encoding.Encode(data);

        // assert - encoded
        Assert.Equal(expectedLength, encoded.Length);

        // act - decode
        using var decoded = encoding.Decode(encoded.Span);

        // assert - decoded
        Assert.True(data.SequenceEqual(decoded.Span));
    }
}