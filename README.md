Spreads.LMDB
=====================

Low-level and the fastest LMDB .NET wrapper with some additional native helper 
methods useful for Spreads and full C# `async/await` support.


Automatically takes care or read-only transactions and cursors renewal 
if they are properly disposed as .NET objects. Doesn't allocate those 
objects every time (uses internal pools).


**Warning!** Exposes `MDB_val` directly, the struct MUST ONLY be read when inside a transaction
(or when it points to an overflow page - but that is a undocumented hack working so far). For writes, 
the memory behind Span MUST BE pinned

MDB_val is defined as:

```
[StructLayout(LayoutKind.Sequential, Pack = 4)]
public readonly unsafe struct MDB_val
{
    public readonly size_t mv_size;
    public readonly void* mv_data;

    public MDB_val(Span<byte> span)
    {
        mv_size = (size_t)span.Length;
        mv_data = Unsafe.AsPointer(ref MemoryMarshal.GetReference(span));
    }

    public ReadOnlySpan<byte> Span
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get { return new ReadOnlySpan<byte>(mv_data, checked((int)mv_size)); }
    }
}
```

It uses `Span` from `System.Memory` so it's very fast and doesn't need any marshalling. 
Only discipline with the transaction scope (read the warning above once again!).
