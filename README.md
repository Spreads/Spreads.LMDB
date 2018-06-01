# Spreads.LMDB

Low-level zero-overhead and the fastest LMDB .NET wrapper with some additional native 
methods useful for [Spreads](https://github.com/Spreads/).

Available on NuGet as [Spreads.LMDB](https://www.nuget.org/packages/Spreads.LMDB).

## Full C# `async/await` support

Read transactions could be used from async code, while write transactions are executed 
in a single thread via a blocking concurrent queue. This requires forcing [`MDB_NOTLS`](http://www.lmdb.tech/doc/group__mdb.html#ga32a193c6bf4d7d5c5d579e71f22e9340) 
attribute for environments:

> A thread may use parallel read-only transactions. A read-only transaction may span threads if the user synchronizes its use. Applications that multiplex many user threads over individual OS threads need this option. Such an application must also serialize the write transactions in an OS thread, since LMDB's write locking is unaware of the user threads.

Spreads.LMDB automatically takes care or read-only transactions and cursors renewal 
if they are properly disposed as .NET objects. It does not allocate those 
objects in steady state (uses internal pools).

**Warning!** This library exposes `MDB_val` directly, the struct *MUST ONLY* be read when inside a transaction
(or when it points to an overflow page - but that is a undocumented hack working so far). For writes, 
the memory behind Span *MUST BE pinned*.

MDB_val is defined as `ref struct`:

```
public readonly unsafe ref struct MDB_val
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

`ref struct` is similar to `Span<>` and could only live on the stack. It cannot be used 
in async code, lambdas and is optimal for LMDB to minimize the risk of exiting transaction scope.

It uses [`Span<T>`](https://msdn.microsoft.com/en-us/magazine/mt814808.aspx) from `System.Memory` so it is very fast and does not need any copying/marshalling
to bytes. Only discipline with the transaction scope (read the *warning* above once again!) is required.


# Example

There are a couple of tests, e.g.:

```
[Test]
public async Task CouldWriteAndRead()
{
    Assert.AreEqual(LMDBVersionInfo.Version, "LMDB 0.9.22: (March 21, 2018)");
    Console.WriteLine(LMDBVersionInfo.Version);
    var env = new Environment("./Data");
    env.Open();
    var stat = env.GetStat();

    var db = await env.OpenDatabase("first_db", new DatabaseConfig(DbFlags.Create));
    
    // Note: Txn is only available from this method (there is alos Read(_))
    await env.WriteAsync(txn =>
    {
        var cursor = db.OpenWriteCursor(txn);
        var values = new byte[] { 1, 2, 3, 4 };
        
        // Note: as a `ref struct` MDB_val will not allow this lambda to be async
        // or to escape the scope of txn.
        var key = new MDB_val(values);
        var value = new MDB_val(values);
        MDB_val value2 = default;

        Assert.IsTrue(cursor.TryPut(ref key, ref value, CursorPutOptions.NoOverwrite));

        Assert.IsTrue(cursor.TryGet(CursorGetOption.SetKey, ref key, ref value2));

        Assert.IsTrue(value2.Span.SequenceEqual(value.Span));

        return Task.CompletedTask;
    });
    await env.Close();
}
```

# Limitations & status

This is being deployed and tested in production. I needed a zero-overhead but convenient wrapper,
not raw P/Invoke. [`Span<T>` et al.](https://msdn.microsoft.com/en-us/magazine/mt814808.aspx) are perfect
for this! On May 31st .NET Core 2.1 was finally released - and (by accident) the first version of this
library. The perfect .NET release with the powerful memory access tools meet the perfect database!

The library targets .NET Standard 2.0 and have native binaries for Linux, but currently works on Windows only.
Cannot load Linux's native using old loader library tested long time ago outside Windows. (Linux will not be a priority 
for a couple of weeks.)

The project has required binaries in `lib` folder - they are native dlls compressed with 
`deflate` and embedded into the package dll as resources (this often simplifies deployment). 
Source code maybe added later if someone needs it. Should work with normal binaries as well
if not using two `TryFind` helper methods.


# TODOs

Quite often we need to 
```
begin txn -> open cursor -> do a single op -> close cursor -> close txn
```
That is 5 P/Invokes, which do matter in super fast LMDB context.

Current Spreads-specific helper methods are already for this, but they accept 
an open cursor and require txn renew + cursor renew + tx reset.

There is non-negligible opportinity to make methods that accept only handles 
(txn in reset state + RO sursor) and do all work in C. The read-only 
handles will remain pooled on .NET side.

Methods: 

* [ ] Env.ReadCursor - renew both txc & cursor at once
* [ ] Env.WriteCursor - create both
* [ ] Env.CloseCursor - close both
* [ ] Db.[single_cursor_op] - e.g. append or lookup

# Contributing

Issues & PRs are welcome!

# Copyright

MPL 2.0
(c) Victor Baybekov, 2018

