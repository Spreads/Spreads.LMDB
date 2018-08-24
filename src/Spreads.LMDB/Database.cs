// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

using Spreads.Buffers;
using Spreads.Collections.Concurrent;
using Spreads.LMDB.Interop;
using Spreads.Serialization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using static System.Runtime.CompilerServices.Unsafe;

namespace Spreads.LMDB
{
    /// <summary>
    /// Database.
    /// </summary>
    public class Database : IDisposable
    {
        internal readonly ObjectPool<ReadCursorHandle> ReadCursorHandlePool =
            new ObjectPool<ReadCursorHandle>(() => new ReadCursorHandle(), System.Environment.ProcessorCount * 16);

        internal uint _handle;
        private readonly DatabaseConfig _config;
        private readonly LMDBEnvironment _environment;
        private readonly string _name;

        internal Database(string name, TransactionImpl txn, DatabaseConfig config)
        {
            if (txn.IsReadOnly) { throw new InvalidOperationException("Cannot create a DB with RO transaction"); }
            if (txn == null) { throw new ArgumentNullException(nameof(txn)); }
            _config = config ?? throw new ArgumentNullException(nameof(config));
            _name = name;
            _environment = txn.LmdbEnvironment;

            NativeMethods.AssertExecute(NativeMethods.mdb_dbi_open(txn._writeHandle, name, _config.OpenFlags, out var handle));
            if (_config.CompareFunction != null)
            {
                NativeMethods.AssertExecute(NativeMethods.mdb_set_compare(txn._writeHandle, handle, _config.CompareFunction));
            }
            if (_config.DupSortFunction != null)
            {
                NativeMethods.AssertExecute(NativeMethods.mdb_set_dupsort(txn._writeHandle, handle, _config.DupSortFunction));
            }
            if (_config.DupSortPrefix > 0)
            {
                if (_config.DupSortPrefix == 96)
                {
                    NativeMethods.AssertExecute(NativeMethods.sdb_set_dupsort_as_uint96(txn._writeHandle, handle));
                }
                else if (_config.DupSortPrefix == 80)
                {
                    NativeMethods.AssertExecute(NativeMethods.sdb_set_dupsort_as_uint80(txn._writeHandle, handle));
                }
                else if (_config.DupSortPrefix == 64)
                {
                    NativeMethods.AssertExecute(NativeMethods.sdb_set_dupsort_as_uint64(txn._writeHandle, handle));
                }
                else if (_config.DupSortPrefix == 48)
                {
                    NativeMethods.AssertExecute(NativeMethods.sdb_set_dupsort_as_uint48(txn._writeHandle, handle));
                }
                else if (_config.DupSortPrefix == 32)
                {
                    NativeMethods.AssertExecute(NativeMethods.sdb_set_dupsort_as_uint32(txn._writeHandle, handle));
                }
                else if (_config.DupSortPrefix == 16)
                {
                    NativeMethods.AssertExecute(NativeMethods.sdb_set_dupsort_as_uint16(txn._writeHandle, handle));
                }
                else
                {
                    throw new NotSupportedException("Rethink your design if you need this!");
                }
            }

            _handle = handle;
        }

        internal bool IsReleased => _handle == default(uint);

        /// <summary>
        /// Is database opened.
        /// </summary>
        public bool IsOpen => _handle != default;

        /// <summary>
        /// Database name.
        /// </summary>
        public string Name => _name;

        /// <summary>
        /// Environment in which the database was opened.
        /// </summary>
        public LMDBEnvironment Environment => _environment;

        /// <summary>
        /// Flags with which the database was opened.
        /// </summary>
        public DbFlags OpenFlags => _config.OpenFlags;

        /// <summary>
        /// Open Read/Write cursor.
        /// </summary>
        public Cursor OpenCursor(Transaction txn)
        {
            return new Cursor(CursorImpl.Create(this, txn._impl, null));
        }

        public ReadOnlyCursor OpenReadOnlyCursor(ReadOnlyTransaction txn)
        {
            var rch = ReadCursorHandlePool.Allocate();
            return new ReadOnlyCursor(CursorImpl.Create(this, txn._impl, rch));
        }

        /// <summary>
        /// Drops the database.
        /// </summary>
        public void Drop()
        {
#pragma warning disable 618
            using (var txn = Environment.BeginTransaction())
#pragma warning restore 618
            {
                try
                {
                    NativeMethods.AssertExecute(NativeMethods.mdb_drop(txn._impl._writeHandle, _handle, true));
                    txn.Commit();
                }
                catch
                {
                    txn.Abort();
                    throw;
                }
            }
        }

        /// <summary>
        /// Drops the database inside the given transaction.
        /// </summary>
        public bool Drop(Transaction transaction)
        {
            var res = NativeMethods.AssertExecute(NativeMethods.mdb_drop(transaction._impl._writeHandle, _handle, true));
            _handle = default;
            return res == 0;
        }

        /// <summary>
        /// Truncates all data from the database.
        /// </summary>
        public void Truncate()
        {
#pragma warning disable 618
            using (var txn = Environment.BeginTransaction())
#pragma warning restore 618
            {
                try
                {
                    NativeMethods.AssertExecute(NativeMethods.mdb_drop(txn._impl._writeHandle, _handle, false));
                    txn.Commit();
                }
                catch
                {
                    txn.Abort();
                    throw;
                }
            }
        }

        /// <summary>
        /// Truncates all data from the database inside the given transaction.
        /// </summary>
        public bool Truncate(Transaction transaction)
        {
            var res = NativeMethods.AssertExecute(NativeMethods.mdb_drop(transaction._impl._writeHandle, _handle, false));
            return res == 0;
        }

        public MDB_stat GetStat()
        {
            using (var tx = TransactionImpl.Create(Environment, TransactionBeginFlags.ReadOnly))
            {
                NativeMethods.AssertRead(NativeMethods.mdb_stat(tx._readHandle.Handle, _handle, out var stat));
                return stat;
            }
        }

        public long GetEntriesCount()
        {
            var stat = GetStat();
            return stat.ms_entries.ToInt64();
        }

        public long GetUsedSize()
        {
            var stat = GetStat();
            var totalPages =
                stat.ms_branch_pages.ToInt64() +
                stat.ms_leaf_pages.ToInt64() +
                stat.ms_overflow_pages.ToInt64();
            return stat.ms_psize * totalPages;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Put(Transaction txn, ref DirectBuffer key, ref DirectBuffer value,
            TransactionPutOptions flags = TransactionPutOptions.None)
        {
            NativeMethods.AssertExecute(NativeMethods.mdb_put(txn._impl._writeHandle, _handle,
                ref key, ref value, flags));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe void Put<TKey, TValue>(Transaction txn, TKey key, TValue value,
            TransactionPutOptions flags = TransactionPutOptions.None)
            where TKey : struct where TValue : struct
        {
            var keyPtr = AsPointer(ref key);
            var valuePtr = AsPointer(ref value);
            var key1 = new DirectBuffer(TypeHelper<TKey>.EnsureFixedSize(), (byte*)keyPtr);
            var value1 = new DirectBuffer(TypeHelper<TValue>.EnsureFixedSize(), (byte*)valuePtr);
            NativeMethods.AssertExecute(NativeMethods.mdb_put(txn._impl._writeHandle, _handle,
                ref key1, ref value1,
                flags));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Put(ref DirectBuffer key, ref DirectBuffer value,
            TransactionPutOptions flags = TransactionPutOptions.None)
        {
            NativeMethods.AssertExecute(NativeMethods.sdb_put(Environment._handle.Handle, _handle,
                ref key, ref value,
                flags));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe void Put<TKey, TValue>(TKey key, TValue value,
            TransactionPutOptions flags = TransactionPutOptions.None)
            where TKey : struct where TValue : struct
        {
            var keyPtr = AsPointer(ref key);
            var valuePtr = AsPointer(ref value);
            var key1 = new DirectBuffer(TypeHelper<TKey>.EnsureFixedSize(), (byte*)keyPtr);
            var value1 = new DirectBuffer(TypeHelper<TValue>.EnsureFixedSize(), (byte*)valuePtr);
            NativeMethods.AssertExecute(NativeMethods.sdb_put(Environment._handle.Handle, _handle,
                ref key1, ref value1,
                flags));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe Task PutAsync<TKey, TValue>(TKey key, TValue value,
            TransactionPutOptions flags = TransactionPutOptions.None)
            where TKey : struct where TValue : struct
        {
            return Task.Run(() => Put(key, value, flags));
        }

        /// <summary>
        /// Delete key/value at key or all dupsort values if db supports dupsorted
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Delete(Transaction txn, ref DirectBuffer key)
        {
            NativeMethods.AssertExecute(NativeMethods.mdb_del(txn._impl._writeHandle, _handle,
                in key, IntPtr.Zero));
        }

        /// <summary>
        /// Delete key/value at key or all dupsort values if db supports dupsorted
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe void Delete<T>(Transaction txn, T key)
            where T : struct
        {
            var keyPtr = AsPointer(ref key);
            var key1 = new DirectBuffer(TypeHelper<T>.EnsureFixedSize(), (byte*)keyPtr);
            Delete(txn, ref key1);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Delete(Transaction txn, DirectBuffer key, DirectBuffer value)
        {
            if (((int)OpenFlags & (int)DbFlags.DuplicatesSort) == 0)
            {
                throw new InvalidOperationException("Value parameter should only be provided for dupsorted dbs");
            }
            NativeMethods.AssertExecute(NativeMethods.mdb_del(txn._impl._writeHandle, _handle,
                in key, in value));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe void Delete<T>(Transaction txn, DirectBuffer key, T value)
            where T : struct
        {
            var valuePtr = AsPointer(ref value);
            var value1 = new DirectBuffer(TypeHelper<T>.EnsureFixedSize(), (byte*)valuePtr);
            Delete(txn, key, value1);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe void Delete<T>(Transaction txn, T key, DirectBuffer value)
            where T : struct
        {
            var keyPtr = AsPointer(ref key);
            var key1 = new DirectBuffer(TypeHelper<T>.EnsureFixedSize(), (byte*)keyPtr);
            Delete(txn, key1, value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe void Delete<TKey, TValue>(Transaction txn, TKey key, TValue value)
            where TKey : struct where TValue : struct
        {
            var keyPtr = AsPointer(ref key);
            var valuePtr = AsPointer(ref value);
            var key1 = new DirectBuffer(TypeHelper<TKey>.EnsureFixedSize(), (byte*)keyPtr);
            var value1 = new DirectBuffer(TypeHelper<TValue>.EnsureFixedSize(), (byte*)valuePtr);
            Delete(txn, key1, value1);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGet(Transaction txn, ref DirectBuffer key, out DirectBuffer value)
        {
            var res = NativeMethods.AssertRead(NativeMethods.mdb_get(txn._impl._writeHandle, _handle, ref key, out value));
            return res != NativeMethods.MDB_NOTFOUND;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe bool TryGet<T>(Transaction txn, ref DirectBuffer key, out T value)
             where T : struct
        {
            TypeHelper<T>.EnsureFixedSize();
            var res = NativeMethods.AssertRead(NativeMethods.mdb_get(txn._impl._writeHandle, _handle, ref key, out DirectBuffer value1));
            if (res != NativeMethods.MDB_NOTFOUND)
            {
                value = ReadUnaligned<T>((byte*)value1.Data);
                return true;
            }

            value = default;
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe bool TryGet<T>(Transaction txn, ref T key, out DirectBuffer value)
            where T : struct
        {
            var keyPtr = AsPointer(ref key);
            var key1 = new DirectBuffer(TypeHelper<T>.EnsureFixedSize(), (byte*)keyPtr);
            var res = NativeMethods.AssertRead(NativeMethods.mdb_get(txn._impl._writeHandle, _handle, ref key1, out value));
            return res != NativeMethods.MDB_NOTFOUND;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe bool TryGet<TKey, TValue>(Transaction txn, ref TKey key, out TValue value)
            where TKey : struct where TValue : struct
        {
            var keyPtr = AsPointer(ref key);
            var key1 = new DirectBuffer(TypeHelper<TKey>.EnsureFixedSize(), (byte*)keyPtr);
            TypeHelper<TValue>.EnsureFixedSize();
            var res = NativeMethods.AssertRead(NativeMethods.mdb_get(txn._impl._writeHandle, _handle,
                ref key1, out DirectBuffer value1));
            if (res != NativeMethods.MDB_NOTFOUND)
            {
                value = ReadUnaligned<TValue>((byte*)value1.Data);
                return true;
            }

            value = default;
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGet(ReadOnlyTransaction txn, ref DirectBuffer key, out DirectBuffer value)
        {
            var res = NativeMethods.AssertRead(NativeMethods.mdb_get(txn._impl._readHandle.Handle, _handle, ref key, out value));
            return res != NativeMethods.MDB_NOTFOUND;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe bool TryGet<T>(ReadOnlyTransaction txn, ref DirectBuffer key, out T value)
            where T : struct
        {
            TypeHelper<T>.EnsureFixedSize();
            var res = NativeMethods.AssertRead(NativeMethods.mdb_get(txn._impl._readHandle.Handle, _handle,
                ref key, out DirectBuffer value1));
            if (res != NativeMethods.MDB_NOTFOUND)
            {
                value = ReadUnaligned<T>((byte*)value1.Data);
                return true;
            }

            value = default;
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe bool TryGet<TKey>(ReadOnlyTransaction txn, ref TKey key, out DirectBuffer value)
            where TKey : struct
        {
            var keyPtr = AsPointer(ref key);
            var key1 = new DirectBuffer(TypeHelper<TKey>.EnsureFixedSize(), (byte*)keyPtr);
            var res = NativeMethods.AssertRead(NativeMethods.mdb_get(txn._impl._readHandle.Handle, _handle, ref key1, out value));
            return res != NativeMethods.MDB_NOTFOUND;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe bool TryGet<TKey, TValue>(ReadOnlyTransaction txn, ref TKey key, out TValue value)
            where TKey : struct where TValue : struct
        {
            var keyPtr = AsPointer(ref key);
            var key1 = new DirectBuffer(TypeHelper<TKey>.EnsureFixedSize(), (byte*)keyPtr);
            TypeHelper<TValue>.EnsureFixedSize();
            var res = NativeMethods.AssertRead(NativeMethods.mdb_get(txn._impl._readHandle.Handle, _handle, ref key1, out DirectBuffer value1));
            if (res != NativeMethods.MDB_NOTFOUND)
            {
                value = ReadUnaligned<TValue>((byte*)value1.Data);
                return true;
            }

            value = default;
            return false;
        }

        /// <summary>
        /// Iterate over db values.
        /// </summary>
        public IEnumerable<KeyValuePair<DirectBuffer, DirectBuffer>> AsEnumerable(ReadOnlyTransaction txn)
        {
            DirectBuffer key = default;
            DirectBuffer value = default;
            using (var c = OpenReadOnlyCursor(txn))
            {
                if (c.TryGet(ref key, ref value, CursorGetOption.First))
                {
                    yield return new KeyValuePair<DirectBuffer, DirectBuffer>(key, value);
                    value = default;
                }

                while (c.TryGet(ref key, ref value, CursorGetOption.NextNoDuplicate))
                {
                    yield return new KeyValuePair<DirectBuffer, DirectBuffer>(key, value);
                    value = default;
                }
            }
        }

        /// <summary>
        /// Iterate over db values.
        /// </summary>
        public IEnumerable<KeyValuePair<TKey, TValue>> AsEnumerable<TKey, TValue>(ReadOnlyTransaction txn)
        {
            var keySize = TypeHelper<TKey>.EnsureFixedSize();
            var valueSize = TypeHelper<TValue>.EnsureFixedSize();

            return AsEnumerable(txn).Select(kvp =>
            {
                if (kvp.Key.Length != keySize)
                {
                    throw new InvalidOperationException("Key buffer length does not equals to key size");
                }

                if (kvp.Value.Length != valueSize)
                {
                    throw new InvalidOperationException("Value buffer length does not equals to value size");
                }
                return new KeyValuePair<TKey, TValue>(kvp.Key.Read<TKey>(0), kvp.Value.Read<TValue>(0));
            });
        }

        private IEnumerable<DirectBuffer> AsEnumerable(ReadOnlyTransaction txn, RetainedMemory<byte> key)
        {
            if (((int)OpenFlags & (int)DbFlags.DuplicatesSort) == 0)
            {
                throw new InvalidOperationException("AsEnumerable overload with key parameter should only be provided for dupsorted dbs");
            }

            try
            {
                var key1 = new DirectBuffer(key.Span);
                DirectBuffer value = default;
                using (var c = OpenReadOnlyCursor(txn))
                {
                    if (c.TryGet(ref key1, ref value, CursorGetOption.SetKey) &&
                        c.TryGet(ref key1, ref value, CursorGetOption.FirstDuplicate))
                    {
                        yield return value;
                    }

                    while (c.TryGet(ref key1, ref value, CursorGetOption.NextDuplicate))
                    {
                        yield return value;
                    }
                }
            }
            finally
            {
                key.Dispose();
            }
        }

        /// <summary>
        /// Iterate over dupsorted values by key.
        /// </summary>
        public IEnumerable<DirectBuffer> AsEnumerable(ReadOnlyTransaction txn, DirectBuffer key)
        {
            if (((int)OpenFlags & (int)DbFlags.DuplicatesSort) == 0)
            {
                throw new InvalidOperationException("AsEnumerable overload with key parameter should only be provided for dupsorted dbs");
            }

            var fixedMemory = BufferPool.Retain(checked((int)key.Length), true);
            key.Span.CopyTo(fixedMemory.Span);
            return AsEnumerable(txn, fixedMemory);
        }

        /// <summary>
        /// Iterate over dupsorted values by key.
        /// </summary>
        public unsafe IEnumerable<DirectBuffer> AsEnumerable<T>(ReadOnlyTransaction txn, T key)
        {
            var keyPtr = AsPointer(ref key);
            var keyLength = TypeHelper<T>.EnsureFixedSize();
            var key1 = new DirectBuffer(keyLength, (byte*)keyPtr);
            var fixedMemory = BufferPool.Retain(keyLength, true);
            key1.Span.CopyTo(fixedMemory.Span);
            return AsEnumerable(txn, fixedMemory);
        }

        /// <summary>
        /// Iterate over dupsorted values by key.
        /// </summary>
        public unsafe IEnumerable<TValue> AsEnumerable<TKey, TValue>(ReadOnlyTransaction txn, TKey key)
        {
            var keyPtr = AsPointer(ref key);
            var keyLength = TypeHelper<TKey>.EnsureFixedSize();
            var key1 = new DirectBuffer(keyLength, (byte*)keyPtr);
            var fixedMemory = BufferPool.Retain(keyLength, true);
            key1.Span.CopyTo(fixedMemory.Span);

            return AsEnumerable(txn, fixedMemory).Select(buf =>
              {
                  var valueSize = TypeHelper<TValue>.EnsureFixedSize();
                  if (buf.Length != valueSize)
                  {
                      throw new InvalidOperationException("Buffer length does not equals to value size");
                  }
                  return buf.Read<TValue>(0);
              });
        }

        protected virtual void Dispose(bool disposing)
        {
            if (_handle == default)
            {
                return;
            }
            NativeMethods.mdb_dbi_close(Environment._handle, _handle);
            _handle = default;
        }

        public void Dispose()
        {
            GC.SuppressFinalize(this);
            Dispose(true);
        }

        ~Database()
        {
            Dispose(false);
        }
    }
}
