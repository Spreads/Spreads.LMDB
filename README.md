# Spreads.LMDB

Low-level zero-overhead and [the fastest](https://github.com/Spreads/Spreads.LMDB/commit/4085dde649ef9ebb64310f2627299762dd62d5ce) LMDB .NET wrapper with some additional native 
methods useful for [Spreads](https://github.com/Spreads/).

Available on NuGet as [Spreads.LMDB](https://www.nuget.org/packages/Spreads.LMDB).

## C# `async/await` support

> In the original version, this library provided a dedicated writer thread for background writes (disabled by default). Now this functionality is removed. 
It is hard to implement for a general case, but write serialization could be achieved in user code if needed. See #41 for more details.

LMDB's supported "normal" case is when a transaction is executed from a single thread. For .NET this means 
that if all operations on a transactions are called from a single thread then it does not matter which
thread is executing a transaction and LMDB will just work. However, it's not possible to jump threads inside 
write transactions, which means no awaits or wait handle waits.

Read transactions could be used from async code, which requires forcing [`MDB_NOTLS`](http://www.lmdb.tech/doc/group__mdb.html#ga32a193c6bf4d7d5c5d579e71f22e9340) 
attribute for environments:

> A thread may use parallel read-only transactions. A read-only transaction may span threads if the user synchronizes its use. Applications that multiplex many user threads over individual OS threads need this option. Such an application must also serialize the write transactions in an OS thread, since LMDB's write locking is unaware of the user threads.

## Read-only transaction and cursor renewal

Spreads.LMDB automatically takes care of read-only transaction and cursor renewals 
if they are properly disposed as .NET objects. It does not allocate those 
objects in steady state (uses internal pools).

## Working with memory safely

**Warning!** This library exposes `MDB_val` directly as `DirectBuffer` struct, the struct *MUST ONLY* be read when inside a transaction
(or when it points to an overflow page - but that is an undocumented hack working so far). For writes, 
the memory behind `DirectBuffer` *MUST BE pinned*. 

`DirectBuffer.Span` property allows to access `MDB_val` as `Span<byte>`. `DirectBuffer` can be easily constructed from `Span<byte>`, 
but the span must be pinned as well if it is backed by `byte[]`.

[`DirectBuffer`](https://github.com/Spreads/Spreads/blob/master/src/Spreads.Core/Buffers/DirectBuffer.cs) has many methods
 to read/write primitive and generic blittable struct values from any offset, 
e.g. `directBufferInstance.Read<ulong>(8)` to read `ulong` from offset `8`. By default
it checks bounds, and an LMDB call via P/Invoke takes much longer so there is no reason to switch the 
bounds checks off. But you can still do so e.g. if you read separate bytes of large values
 a lot (e.g. via indexer `directBufferInstance[offset]` that returns a single byte at `offset`).

## Generic key/values support

Any C# struct that has no references could be used directly as a key or a value. See [IROCR docs](https://docs.microsoft.com/en-us/dotnet/api/system.runtime.compilerservices.runtimehelpers.isreferenceorcontainsreferences).
Be aware of auto layout, padding and related issues.

## IEnumerable support

A database or duplicate values of a key in a single dupsorted database could be enumerated via `dataBaseInstance.AsEnumerable([several overloads])` methods that could return 
either `DirectBuffer`s or generic blittable structs.

# Examples

See tests. The API is very close to the C one but adapted for .NET. 

# Native libraries

Required native binaries fo x64 on Linux, Windows and macOS are included in the NuGet package. 
They are built via GitHub Actions using this [Makefile](https://github.com/Spreads/Spreads.LMDB/blob/main/lib/libspreadsdb/src/libspreadsdb/Makefile).

The LMDB version is `mdb.master` branch matching the latest edit to CHANGES in `mdb.RE/0.9` branch. 

To build locally, you could adjust `SOEXT` for your platform and call `make` or just call make with a target `libspreads_lmdb[.so|.dll|.dylib]`.

The library works with the original native LMDB binaries as well, but `TryFind` helper methods won't work.

# Limitations

The library does not support nested transactions yet - only because we do not use them currently. 
They will be added as soon as we find a real-world compelling case for them. 

# Contributing

Issues & PRs are welcome!

# Copyright

MPL 2.0
(c) Victor Baybekov, 2018-2023

