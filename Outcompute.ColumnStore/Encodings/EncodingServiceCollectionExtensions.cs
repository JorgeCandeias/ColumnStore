namespace Outcompute.ColumnStore.Encodings;

public static class EncodingServiceCollectionExtensions
{
    public static IServiceCollection AddEncoding(this IServiceCollection services, int id, Type type)
    {
        Guard.IsNotNull(services, nameof(services));

        if (type.IsGenericTypeDefinition)
        {
            if (!type.IsSubclassOf(typeof(Encoding<>)))
            {
                Throw();
            }
        }
        else if (type.IsConstructedGenericType)
        {
            var definition = type.GetGenericTypeDefinition();
            if (!definition.IsSubclassOf(typeof(Encoding<>)))
            {
                Throw();
            }
        }
        else
        {
            Throw();
        }

        return services
            .AddSingleton(type)
            .AddSingleton(typeof(EncodingLookupEntry), new EncodingLookupEntry(id, type));

        void Throw() => ThrowHelper.ThrowInvalidOperationException($"Type {type.ToTypeString()} must implement {typeof(Encoding<>).ToTypeString()}");
    }

    public static IServiceCollection AddEncoding<TEncoding>(this IServiceCollection services, byte id)
    {
        Guard.IsNotNull(services, nameof(services));

        return services
            .AddEncoding(id, typeof(TEncoding));
    }
}