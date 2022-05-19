using CommunityToolkit.Diagnostics;

namespace Outcompute.ColumnStore;

internal class Indexer<TKey, TValue>
{
    private readonly Action<TKey, TValue> _setCallback;
    private readonly Func<TKey, TValue> _getCallback;

    public Indexer(Action<TKey, TValue> setCallback, Func<TKey, TValue> getCallback)
    {
        Guard.IsNotNull(setCallback, nameof(setCallback));
        Guard.IsNotNull(getCallback, nameof(getCallback));

        _setCallback = setCallback;
        _getCallback = getCallback;
    }

    public TValue this[TKey key]
    {
        get
        {
            return _getCallback(key);
        }
        set
        {
            _setCallback(key, value);
        }
    }
}