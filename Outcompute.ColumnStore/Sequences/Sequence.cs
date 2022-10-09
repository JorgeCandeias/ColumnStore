using Outcompute.ColumnStore.Core.Buffers;

namespace Outcompute.ColumnStore.Sequences;

[GenerateSerializer]
internal class Sequence<T> : IDisposable
{
    private readonly IMemoryOwner<T> _memory;
    private readonly Stats<T> _stats;

    public Sequence(AlignedNativeMemoryOwner<T> memory, Stats<T> stats)
    {
        Guard.IsNotNull(memory, nameof(memory));
        Guard.IsNotNull(stats, nameof(stats));

        _memory = memory;
        _stats = stats;
    }

    public ReadOnlyMemory<T> Memory => _memory.Memory;

    public ReadOnlySpan<T> Span => _memory.Memory.Span;

    public

    protected virtual void Dispose(bool disposing)
    {
        if (disposing)
        {
            _memory.Dispose();
        }
    }

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    ~Sequence()
    {
        Dispose(false);
    }
}