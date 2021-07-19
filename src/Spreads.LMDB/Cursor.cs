// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

using Spreads.Buffers;
using Spreads.Collections.Concurrent;
using Spreads.LMDB.Interop;
using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.ConstrainedExecution;
using System.Threading;
using static System.Runtime.CompilerServices.Unsafe;
using static Spreads.LMDB.Util;

namespace Spreads.LMDB
{
    public readonly struct Cursor : ICursor
    {
        internal readonly CursorImpl _impl;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Cursor(CursorImpl cursor)
        {
            _impl = cursor;
        }

        public void Dispose()
        {
            _impl.Dispose();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryFind<TKey, TValue>(Lookup direction, ref TKey key, out TValue value) where TKey : struct where TValue : struct
        {
            return _impl.TryFind(direction, ref key, out value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryFind(Lookup direction, ref DirectBuffer key, out DirectBuffer value)
        {
            return _impl.TryFind(direction, ref key, out value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryFindDup<TKey, TValue>(Lookup direction, ref TKey key, ref TValue value) where TKey : struct where TValue : struct
        {
            return _impl.TryFindDup(direction, ref key, ref value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryFindDup(Lookup direction, ref DirectBuffer key, ref DirectBuffer value)
        {
            return _impl.TryFindDup(direction, ref key, ref value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGet<TKey, TValue>(ref TKey key, ref TValue value, CursorGetOption operation) where TKey : struct where TValue : struct
        {
            return _impl.TryGet(ref key, ref value, operation);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGet(ref DirectBuffer key, ref DirectBuffer value, CursorGetOption operation)
        {
            return _impl.TryGet(ref key, ref value, operation);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ulong Count()
        {
            return _impl.Count();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryPut<TKey, TValue>(ref TKey key, ref TValue value, CursorPutOptions options) where TKey : struct where TValue : struct
        {
            return _impl.TryPut(ref key, ref value, options);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryPut(ref DirectBuffer key, ref DirectBuffer value, CursorPutOptions options)
        {
            return _impl.TryPut(ref key, ref value, options);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Put<TKey, TValue>(ref TKey key, ref TValue value, CursorPutOptions options) where TKey : struct where TValue : struct
        {
            _impl.Put(ref key, ref value, options);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Put(ref DirectBuffer key, ref DirectBuffer value, CursorPutOptions options)
        {
            _impl.Put(ref key, ref value, options);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Add(ref DirectBuffer key, ref DirectBuffer value)
        {
            _impl.Add(ref key, ref value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Replace(ref DirectBuffer key, ref DirectBuffer value)
        {
            _impl.Replace(ref key, ref value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Append(ref DirectBuffer key, ref DirectBuffer value, bool dup = false)
        {
            _impl.Append(ref key, ref value, dup);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Reserve(ref DirectBuffer key, ref DirectBuffer value)
        {
            _impl.Reserve(ref key, ref value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Delete(bool removeAllDuplicateData = true)
        {
            return _impl.Delete(removeAllDuplicateData);
        }
    }

    public readonly struct ReadOnlyCursor : IReadOnlyCursor
    {
        internal readonly CursorImpl _impl;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ReadOnlyCursor(CursorImpl cursor)
        {
            //if (!cursor.IsReadOnly)
            //{
            //    CursorImpl.ThrowCursorIsReadOnly();
            //}
            _impl = cursor;
        }

        public void Dispose()
        {
            _impl.Dispose();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryFind<TKey, TValue>(Lookup direction, ref TKey key, out TValue value) where TKey : struct where TValue : struct
        {
            return _impl.TryFind(direction, ref key, out value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryFind(Lookup direction, ref DirectBuffer key, out DirectBuffer value)
        {
            return _impl.TryFind(direction, ref key, out value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryFindDup<TKey, TValue>(Lookup direction, ref TKey key, ref TValue value) where TKey : struct where TValue : struct
        {
            return _impl.TryFindDup(direction, ref key, ref value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryFindDup(Lookup direction, ref DirectBuffer key, ref DirectBuffer value)
        {
            return _impl.TryFindDup(direction, ref key, ref value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGet<TKey, TValue>(ref TKey key, ref TValue value, CursorGetOption operation) where TKey : struct where TValue : struct
        {
            return _impl.TryGet(ref key, ref value, operation);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGet(ref DirectBuffer key, ref DirectBuffer value, CursorGetOption operation)
        {
            return _impl.TryGet(ref key, ref value, operation);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ulong Count()
        {
            return _impl.Count();
        }
    }

    public interface ICursor : IReadOnlyCursor
    {
        bool TryPut<TKey, TValue>(ref TKey key, ref TValue value, CursorPutOptions options)
            where TKey : struct where TValue : struct;

        bool TryPut(ref DirectBuffer key, ref DirectBuffer value, CursorPutOptions options);

        void Put<TKey, TValue>(ref TKey key, ref TValue value, CursorPutOptions options)
            where TKey : struct where TValue : struct;

        void Put(ref DirectBuffer key, ref DirectBuffer value, CursorPutOptions options);

        void Add(ref DirectBuffer key, ref DirectBuffer value);

        void Replace(ref DirectBuffer key, ref DirectBuffer value);

        void Append(ref DirectBuffer key, ref DirectBuffer value, bool dup = false);

        void Reserve(ref DirectBuffer key, ref DirectBuffer value);

        /// <summary>
        /// Delete current key/data pair.
        /// This function deletes the key/data pair to which the cursor refers.
        /// </summary>
        /// <param name="removeAllDuplicateData">if true, delete all of the data items for the current key. This flag may only be specified if the database was opened with MDB_DUPSORT.</param>
        bool Delete(bool removeAllDuplicateData = true);
    }

    public interface IReadOnlyCursor : IDisposable
    {
        bool TryFind<TKey, TValue>(Lookup direction, ref TKey key, out TValue value)
            where TKey : struct where TValue : struct;

        bool TryFind(Lookup direction, ref DirectBuffer key, out DirectBuffer value);

        bool TryFindDup<TKey, TValue>(Lookup direction, ref TKey key, ref TValue value)
            where TKey : struct where TValue : struct;

        bool TryFindDup(Lookup direction, ref DirectBuffer key, ref DirectBuffer value);

        bool TryGet<TKey, TValue>(ref TKey key, ref TValue value, CursorGetOption operation)
            where TKey : struct where TValue : struct;

        bool TryGet(ref DirectBuffer key, ref DirectBuffer value, CursorGetOption operation);

        /// <summary>
        /// Return count of duplicates for current key.
        /// This call is only valid on databases that support sorted duplicate data items MDB_DUPSORT.
        /// </summary>
        ulong Count();
    }

    /// <summary>
    /// Cursor to iterate over a database
    /// </summary>
    internal class CursorImpl : CriticalFinalizerObject, ICursor
    {
        // Read-only cursors are pooled in Database instances since cursors belog to a DB
        // Here we pool only the objects

        private IntPtr _handle;

        private bool _isReadOnly;

        //internal IntPtr _writeHandle;
        //internal ReadCursorHandle _readHandle;

        internal bool _forceReadOnlyX;

        internal Database _database;
        internal TransactionImpl _transaction;

        #region Lifecycle

        private static readonly ObjectPool<CursorImpl> CursorPool =
            new ObjectPool<CursorImpl>(() => new CursorImpl(), Environment.ProcessorCount * 16);

        private CursorImpl()
        { }

        /// <summary>
        /// Creates new instance of LightningCursor
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static CursorImpl Create(Database db, TransactionImpl txn, bool readOnly)
        {
            var c = CursorPool.Rent();

            c._database = db;
            c._transaction = txn;
            c._isReadOnly = readOnly;

            NativeMethods.AssertExecute(NativeMethods.mdb_cursor_open(txn.Handle, db._handle, out IntPtr handle));
            c._handle = handle;
#if DEBUG
            Interlocked.Increment(ref txn._cursorCount);
#endif
            return c;
        }

        /// <summary>
        /// Closes the cursor and deallocates all resources associated with it.
        /// </summary>
        /// <param name="disposing">True if called from Dispose.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected void Dispose(bool disposing)
        {
#if DEBUG
            Interlocked.Decrement(ref _transaction._cursorCount);
#endif
            var isReadOnly = IsReadOnly;
            if (isReadOnly)
            {
                if (disposing)
                {
                    if (_transaction.IsReadOnly)
                    {
                        var pooled = _database.ReadCursorPool.Return(this);
                        if (pooled)
                        {
                            return;
                        }
                    }
                }
                else
                {
#if DEBUG
                    Trace.TraceWarning("Finalizing read cursor. Dispose it explicitly.");
#endif
                }
            }
            else if (!disposing)
            {
#if DEBUG
                Trace.TraceWarning("Finalizing write cursor. Dispose it explicitly.");
#endif
            }

            NativeMethods.mdb_cursor_close(_handle);

            _handle = IntPtr.Zero;

            _database = null;

            _transaction = null;
            if (disposing)
            {
                CursorPool.Return(this);
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        ~CursorImpl()
        {
            Dispose(false);
        }

        #endregion Lifecycle

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Renew(TransactionImpl txn)
        {
            if (!txn.IsReadOnly)
            {
                TransactionImpl.ThrowTransactionIsNotReadOnly();
            }
            NativeMethods.AssertExecute(NativeMethods.mdb_cursor_renew(txn.Handle, _handle));
        }

        public bool IsReadOnly
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _isReadOnly;
        }

        /// <summary>
        /// Cursor's environment.
        /// </summary>
        public LMDBEnvironment LmdbEnvironment => Database.Environment;

        /// <summary>
        /// Cursor's database.
        /// </summary>
        public Database Database => _database;

        #region sdb_cursor_find

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe bool TryFind<TKey, TValue>(Lookup direction, ref TKey key, out TValue value)
            where TKey : struct where TValue : struct
        {
            EnsureNoRefs<TKey>();
            EnsureNoRefs<TValue>();
            
            var keyPtr = AsPointer(ref key);
            var key1 = new DirectBuffer(SizeOf<TKey>(), (nint)keyPtr);
            var res = TryFind(direction, ref key1, out DirectBuffer value1);
            if (res)
            {
                key = ReadUnaligned<TKey>(key1.Data);
                value = ReadUnaligned<TValue>(value1.Data);
                return true;
            }
            value = default;
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryFind(Lookup direction, ref DirectBuffer key, out DirectBuffer value)
        {
            int res = 0;
            value = default;

            if (direction == Lookup.LE)
            {
                res = NativeMethods.AssertRead(
                    NativeMethods.sdb_cursor_get_le(_handle, ref key, out value)
                );
            }
            else if (direction == Lookup.GE)
            {
                res = NativeMethods.AssertRead(
                    NativeMethods.sdb_cursor_get_ge(_handle, ref key, out value)
                );
            }
            else if (direction == Lookup.EQ)
            {
                res = NativeMethods.AssertRead(
                    NativeMethods.sdb_cursor_get_eq(_handle, ref key, out value)
                );
            }
            else if (direction == Lookup.LT)
            {
                res = NativeMethods.AssertRead(
                    NativeMethods.sdb_cursor_get_lt(_handle, ref key, out value)
                );
            }
            else if (direction == Lookup.GT)
            {
                res = NativeMethods.AssertRead(
                    NativeMethods.sdb_cursor_get_gt(_handle, ref key, out value)
                );
            }

            return res != NativeMethods.MDB_NOTFOUND;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe bool TryFindDup<TKey, TValue>(Lookup direction, ref TKey key, ref TValue value)
            where TKey : struct where TValue : struct
        {
            EnsureNoRefs<TKey>();
            EnsureNoRefs<TValue>();
            
            var keyPtr = AsPointer(ref key);
            var key1 = new DirectBuffer(SizeOf<TKey>(), (nint)keyPtr);

            var valuePtr = AsPointer(ref value);
            var value1 = new DirectBuffer(SizeOf<TValue>(), (nint)valuePtr);

            var res = TryFindDup(direction, ref key1, ref value1);
            if (res)
            {
                key = ReadUnaligned<TKey>(key1.Data);
                value = ReadUnaligned<TValue>(value1.Data);
                return true;
            }
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryFindDup(Lookup direction, ref DirectBuffer key, ref DirectBuffer value)
        {
            int res = 0;

            if (direction == Lookup.LE)
            {
                res = NativeMethods.AssertRead(
                    NativeMethods.sdb_cursor_get_le_dup(_handle, ref key, ref value)
                );
            }
            else if (direction == Lookup.GE)
            {
                res = NativeMethods.AssertRead(
                    NativeMethods.sdb_cursor_get_ge_dup(_handle, ref key, ref value)
                );
            }
            else if (direction == Lookup.EQ)
            {
                res = NativeMethods.AssertRead(
                    NativeMethods.sdb_cursor_get_eq_dup(_handle, ref key, ref value)
                );
            }
            else if (direction == Lookup.LT)
            {
                res = NativeMethods.AssertRead(
                     NativeMethods.sdb_cursor_get_lt_dup(_handle, ref key, ref value)
                );
            }
            else if (direction == Lookup.GT)
            {
                res = NativeMethods.AssertRead(
                    NativeMethods.sdb_cursor_get_gt_dup(_handle, ref key, ref value)
                );
            }

            return res != NativeMethods.MDB_NOTFOUND;
        }

        #endregion sdb_cursor_find

        #region mdb_cursor_get

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe bool TryGet<TKey, TValue>(ref TKey key, ref TValue value, CursorGetOption operation)
            where TKey : struct where TValue : struct
        {
            EnsureNoRefs<TKey>();
            EnsureNoRefs<TValue>();
            
            var keyPtr = AsPointer(ref key);
            var valuePtr = AsPointer(ref value);
            var key1 = new DirectBuffer(SizeOf<TKey>(), (nint)keyPtr);
            var value1 = new DirectBuffer(SizeOf<TValue>(), (nint)valuePtr);
            if (TryGet(ref key1, ref value1, operation))
            {
                key = ReadUnaligned<TKey>(key1.Data);
                value = ReadUnaligned<TValue>(value1.Data);
                return true;
            }
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGet(ref DirectBuffer key, ref DirectBuffer value, CursorGetOption operation)
        {
            var res = NativeMethods.AssertRead(NativeMethods.mdb_cursor_get(_handle, ref key, ref value, operation));
            return res != NativeMethods.MDB_NOTFOUND;
        }

        #endregion mdb_cursor_get

        #region mdb_cursor_put

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void EnsureWriteable()
        {
            if (_isReadOnly || _handle == IntPtr.Zero)
            {
                ThrowCursorIsReadOnly();
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        internal static void ThrowCursorIsReadOnly()
        {
            throw new InvalidOperationException("Cursor is readonly or invalid");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe bool TryPut<TKey, TValue>(ref TKey key, ref TValue value, CursorPutOptions options)
            where TKey : struct where TValue : struct
        {
            EnsureNoRefs<TKey>();
            EnsureNoRefs<TValue>();
            
            var keyPtr = AsPointer(ref key);
            var valuePtr = AsPointer(ref value);
            var key1 = new DirectBuffer(SizeOf<TKey>(), (nint)keyPtr);
            var value1 = new DirectBuffer(SizeOf<TValue>(), (nint)valuePtr);
            if (TryPut(ref key1, ref value1, options))
            {
                key = ReadUnaligned<TKey>(key1.Data);
                value = ReadUnaligned<TValue>(value1.Data);
                return true;
            }
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryPut(ref DirectBuffer key, ref DirectBuffer value, CursorPutOptions options)
        {
            EnsureWriteable();
            var res = NativeMethods.mdb_cursor_put(_handle, ref key, ref value, options);
            return res == 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe void Put<TKey, TValue>(ref TKey key, ref TValue value, CursorPutOptions options)
            where TKey : struct where TValue : struct
        {
            EnsureNoRefs<TKey>();
            EnsureNoRefs<TValue>();
            
            var keyPtr = AsPointer(ref key);
            var valuePtr = AsPointer(ref value);
            var key1 = new DirectBuffer(SizeOf<TKey>(), (nint)keyPtr);
            var value1 = new DirectBuffer(SizeOf<TValue>(), (nint)valuePtr);
            Put(ref key1, ref value1, options);
            key = ReadUnaligned<TKey>(key1.Data);
            value = ReadUnaligned<TValue>(value1.Data);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Put(ref DirectBuffer key, ref DirectBuffer value, CursorPutOptions options)
        {
            EnsureWriteable();
            NativeMethods.AssertExecute(NativeMethods.mdb_cursor_put(_handle, ref key, ref value, options));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Add(ref DirectBuffer key, ref DirectBuffer value)
        {
            EnsureWriteable();
            NativeMethods.AssertExecute(NativeMethods.mdb_cursor_put(_handle, ref key, ref value, CursorPutOptions.NoOverwrite));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Replace(ref DirectBuffer key, ref DirectBuffer value)
        {
            EnsureWriteable();
            NativeMethods.AssertExecute(NativeMethods.mdb_cursor_put(_handle, ref key, ref value, CursorPutOptions.Current));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Append(ref DirectBuffer key, ref DirectBuffer value, bool dup = false)
        {
            EnsureWriteable();
            NativeMethods.AssertExecute(
                NativeMethods.mdb_cursor_put(_handle, ref key, ref value,
                    dup ? CursorPutOptions.AppendDuplicateData : CursorPutOptions.AppendData));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Reserve(ref DirectBuffer key, ref DirectBuffer value)
        {
            EnsureWriteable();
            if (Database.OpenFlags.HasFlag(DbFlags.DuplicatesSort)) throw new NotSupportedException("Reserve is not supported for DupSort");
            NativeMethods.AssertExecute(NativeMethods.mdb_cursor_put(_handle, ref key, ref value, CursorPutOptions.ReserveSpace));
        }

        #endregion mdb_cursor_put

        #region mdb_cursor_del + mdb_cursor_count

        /// <summary>
        /// Delete current key/data pair.
        /// This function deletes the key/data pair to which the cursor refers.
        /// </summary>
        /// <param name="removeAllDuplicateData">if true, delete all of the data items for the current key. This flag may only be specified if the database was opened with MDB_DUPSORT.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Delete(bool removeAllDuplicateData = true)
        {
            EnsureWriteable();
            return Delete(removeAllDuplicateData ? CursorDeleteOption.NoDuplicateData : CursorDeleteOption.None);
        }

        //TODO: tests
        /// <summary>
        /// Delete current key/data pair.
        /// This function deletes the key/data pair to which the cursor refers.
        /// </summary>
        /// <param name="option">Options for this operation. This parameter must be set to 0 or one of the values described here.
        ///     MDB_NODUPDATA - delete all of the data items for the current key. This flag may only be specified if the database was opened with MDB_DUPSORT.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool Delete(CursorDeleteOption option)
        {
            EnsureWriteable();
            return 0 == NativeMethods.AssertRead(NativeMethods.mdb_cursor_del(_handle, option));
        }

        /// <summary>
        /// Return count of duplicates for current key.
        /// This call is only valid on databases that support sorted duplicate data items MDB_DUPSORT.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ulong Count()
        {
            NativeMethods.AssertRead(NativeMethods.mdb_cursor_count(_handle, out var result));
            return (ulong)result;
        }

        #endregion mdb_cursor_del + mdb_cursor_count
    }
}
