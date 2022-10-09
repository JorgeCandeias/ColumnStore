using Outcompute.ColumnStore.Core.Buffers;

namespace Outcompute.ColumnStore.Sequences;

/// <summary>
/// A plain sequence of booleans values without compression.
/// Used as a baseline for better sequences.
/// </summary>

[GenerateSerializer]
internal class BooleanPlainSequence<T> : Sequence<T>
    where T : unmanaged
{
    public UnmanagedSequence(AlignedNativeMemoryOwner<T> memory)
    {
        Guard.IsNotNull(memory, nameof(memory));

        _memory = memory;
    }

    [Id(1)]
    private readonly AlignedNativeMemoryOwner<T> _memory;
}