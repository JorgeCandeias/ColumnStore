namespace Outcompute.ColumnStore.Encodings;

internal class EncodingLookup
{
    private readonly IServiceProvider _provider;
    private readonly Dictionary<int, Type> _lookup;
    private readonly Dictionary<(Type, Type), Type> _constructed = new();

    public EncodingLookup(IServiceProvider provider, IEnumerable<EncodingLookupEntry> entries)
    {
        Guard.IsNotNull(provider, nameof(provider));
        Guard.IsNotNull(entries, nameof(entries));

        _provider = provider;
        _lookup = entries
            .GroupBy(x => x.Id)
            .Select(x => x.Last())
            .ToDictionary(x => x.Id, x => x.Type);
    }

    public Encoding<T> Get<T>(int id)
    {
        if (!_lookup.TryGetValue(id, out var type))
        {
            ThrowHelper.ThrowInvalidOperationException($"No encoding registered for id '{id}'");
        }

        if (type.IsGenericTypeDefinition)
        {
            if (!_constructed.TryGetValue((type, typeof(T)), out var constructed))
            {
                _constructed[(type, typeof(T))] = constructed = type.MakeGenericType(typeof(T));
            }

            type = constructed;
        }

        var encoding = _provider.GetService(type);

        if (encoding is null)
        {
            ThrowHelper.ThrowInvalidOperationException($"Could not resolve encoding of type '{type.ToTypeString()}' for id '{id}'");
        }

        return (Encoding<T>)encoding;
    }
}

internal record class EncodingLookupEntry(int Id, Type Type);