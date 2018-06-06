// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

using Spreads.Collections.Concurrent;
using Spreads.LMDB.Interop;
using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Spreads.Buffers;

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
        public bool TryFind(Lookup direction, ref DirectBuffer key, out DirectBuffer value)
        {
            return _impl.TryFind(direction, ref key, out value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryFindDup(Lookup direction, ref DirectBuffer key, out DirectBuffer value)
        {
            return _impl.TryFindDup(direction, ref key, out value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGet(CursorGetOption operation, ref DirectBuffer key, ref DirectBuffer value)
        {
            return _impl.TryGet(operation, ref key, ref value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ulong Count(CursorDeleteOption option)
        {
            return _impl.Count(option);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryPut(ref DirectBuffer key, ref DirectBuffer value, CursorPutOptions options)
        {
            return _impl.TryPut(ref key, ref value, options);
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
            if (!cursor.IsReadOnly)
            {
                CursorImpl.ThrowCursorIsReadOnly();
            }
            _impl = cursor;
        }

        public void Dispose()
        {
            _impl.Dispose();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryFind(Lookup direction, ref DirectBuffer key, out DirectBuffer value)
        {
            return _impl.TryFind(direction, ref key, out value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryFindDup(Lookup direction, ref DirectBuffer key, out DirectBuffer value)
        {
            return _impl.TryFindDup(direction, ref key, out value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGet(CursorGetOption operation, ref DirectBuffer key, ref DirectBuffer value)
        {
            return _impl.TryGet(operation, ref key, ref value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ulong Count(CursorDeleteOption option)
        {
            return _impl.Count(option);
        }
    }

    public interface ICursor : IReadOnlyCursor
    {
        bool TryPut(ref DirectBuffer key, ref DirectBuffer value, CursorPutOptions options);

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
        bool TryFind(Lookup direction, ref DirectBuffer key, out DirectBuffer value);

        bool TryFindDup(Lookup direction, ref DirectBuffer key, out DirectBuffer value);

        bool TryGet(CursorGetOption operation, ref DirectBuffer key, ref DirectBuffer value);

        /// <summary>
        /// Return count of duplicates for current key.
        /// This call is only valid on databases that support sorted duplicate data items MDB_DUPSORT.
        /// </summary>
        ulong Count(CursorDeleteOption option);
    }

    /// <summary>
    /// Cursor to iterate over a database
    /// </summary>
    internal class CursorImpl : ICursor
    {
        // Read-only cursors are pooled in Database instances since cursors belog to a DB
        // Here we pool only the objects
        internal IntPtr _writeHandle;

        internal ReadCursorHandle _readHandle;
        internal bool _forceReadOnly;

        internal Database _database;
        internal TransactionImpl _transaction;

        #region Lifecycle

        private static readonly ObjectPool<CursorImpl> CursorPool =
            new ObjectPool<CursorImpl>(() => new CursorImpl(), System.Environment.ProcessorCount * 16);

        private CursorImpl()
        { }

        /// <summary>
        /// Creates new instance of LightningCursor
        /// </summary>
        internal static CursorImpl Create(Database db, TransactionImpl txn, ReadCursorHandle rh = null)
        {
            var c = CursorPool.Allocate();

            c._database = db ?? throw new ArgumentNullException(nameof(db));
            c._transaction = txn ?? throw new ArgumentNullException(nameof(txn));

            if (rh != null)
            {
                if (!txn.IsReadOnly)
                {
                    TransactionImpl.ThrowlTransactionIsReadOnly("Txn must be readonly to renew a cursor");
                }

                if (rh.IsInvalid)
                {
                    NativeMethods.AssertExecute(NativeMethods.mdb_cursor_open(txn._readHandle.Handle, db._handle, out IntPtr handle));
                    rh.SetNewHandle(handle);
                }
                else
                {
                    NativeMethods.AssertExecute(NativeMethods.mdb_cursor_renew(txn._readHandle.Handle, rh.Handle));
                }

                c._readHandle = rh;
            }
            else
            {
                NativeMethods.AssertExecute(NativeMethods.mdb_cursor_open(txn._writeHandle, db._handle, out IntPtr handle));
                c._writeHandle = handle;
            }

            return c;
        }

        /// <summary>
        /// Closes the cursor and deallocates all resources associated with it.
        /// </summary>
        /// <param name="disposing">True if called from Dispose.</param>
        protected virtual void Dispose(bool disposing)
        {
            var isReadOnly = IsReadOnly;
            if (isReadOnly)
            {
                if (disposing)
                {
                    var rh = _readHandle;
                    _readHandle = null;
                    _database.ReadHandlePool.Free(rh);
                }
                else
                {
                    Trace.TraceWarning("Finalizing read cursor. Dispose it explicitly.");
                    _readHandle.Dispose();
                }
            }
            else
            {
                NativeMethods.mdb_cursor_close(_writeHandle);

                if (!disposing)
                {
                    Trace.TraceWarning("Finalizing write cursor. Dispose it explicitly.");
                }
                _writeHandle = IntPtr.Zero;
            }

            _database = null;
            _transaction = null;
            if (disposing)
            {
                CursorPool.Free(this);
            }
        }

        public void Dispose()
        {
            GC.SuppressFinalize(this);
            Dispose(true);
        }

        ~CursorImpl()
        {
            Dispose(false);
        }

        #endregion Lifecycle

        public bool IsReadOnly => _readHandle != null;

        /// <summary>
        /// Cursor's environment.
        /// </summary>
        public LMDBEnvironment LmdbEnvironment => Database.LmdbEnvironment;

        /// <summary>
        /// Cursor's database.
        /// </summary>
        public Database Database => _database;

        #region sdb_cursor_find

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryFind(Lookup direction, ref DirectBuffer key, out DirectBuffer value)
        {
            int res = 0;
            value = default;

            switch (direction)
            {
                case Lookup.LT:
                    res = NativeMethods.AssertRead(
                        IsReadOnly
                            ? NativeMethods.sdb_cursor_get_lt(_readHandle.Handle, ref key, out value)
                            : NativeMethods.sdb_cursor_get_lt(_writeHandle, ref key, out value)
                        );
                    break;

                case Lookup.LE:
                    res = NativeMethods.AssertRead(
                        IsReadOnly
                            ? NativeMethods.sdb_cursor_get_le(_readHandle.Handle, ref key, out value)
                            : NativeMethods.sdb_cursor_get_le(_writeHandle, ref key, out value)
                    );
                    break;

                case Lookup.EQ:
                    res = NativeMethods.AssertRead(
                        IsReadOnly
                            ? NativeMethods.sdb_cursor_get_eq(_readHandle.Handle, ref key, out value)
                            : NativeMethods.sdb_cursor_get_eq(_writeHandle, ref key, out value)
                    );
                    break;

                case Lookup.GE:
                    res = NativeMethods.AssertRead(
                        IsReadOnly
                            ? NativeMethods.sdb_cursor_get_ge(_readHandle.Handle, ref key, out value)
                            : NativeMethods.sdb_cursor_get_ge(_writeHandle, ref key, out value)
                    );
                    break;

                case Lookup.GT:
                    res = NativeMethods.AssertRead(
                        IsReadOnly
                            ? NativeMethods.sdb_cursor_get_gt(_readHandle.Handle, ref key, out value)
                            : NativeMethods.sdb_cursor_get_gt(_writeHandle, ref key, out value)
                    );
                    break;
            }

            return res != NativeMethods.MDB_NOTFOUND;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryFindDup(Lookup direction, ref DirectBuffer key, out DirectBuffer value)
        {
            int res = 0;
            value = default(DirectBuffer);

            switch (direction)
            {
                case Lookup.LT:
                    res = NativeMethods.AssertRead(
                        IsReadOnly
                            ? NativeMethods.sdb_cursor_get_lt_dup(_readHandle.Handle, ref key, out value)
                            : NativeMethods.sdb_cursor_get_lt_dup(_writeHandle, ref key, out value)
                        );
                    break;

                case Lookup.LE:
                    res = NativeMethods.AssertRead(
                        IsReadOnly
                            ? NativeMethods.sdb_cursor_get_le_dup(_readHandle.Handle, ref key, out value)
                            : NativeMethods.sdb_cursor_get_le_dup(_writeHandle, ref key, out value)
                        );
                    break;

                case Lookup.EQ:
                    res = NativeMethods.AssertRead(
                        IsReadOnly
                            ? NativeMethods.sdb_cursor_get_eq_dup(_readHandle.Handle, ref key, out value)
                            : NativeMethods.sdb_cursor_get_eq_dup(_writeHandle, ref key, out value)
                    );
                    break;

                case Lookup.GE:
                    res = NativeMethods.AssertRead(
                        IsReadOnly
                            ? NativeMethods.sdb_cursor_get_ge_dup(_readHandle.Handle, ref key, out value)
                            : NativeMethods.sdb_cursor_get_ge_dup(_writeHandle, ref key, out value)
                    );
                    break;

                case Lookup.GT:
                    res = NativeMethods.AssertRead(
                        IsReadOnly
                            ? NativeMethods.sdb_cursor_get_gt_dup(_readHandle.Handle, ref key, out value)
                            : NativeMethods.sdb_cursor_get_gt_dup(_writeHandle, ref key, out value)
                    );
                    break;
            }

            return res != NativeMethods.MDB_NOTFOUND;
        }

        #endregion sdb_cursor_find

        #region mdb_cursor_get

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGet(
            CursorGetOption operation, ref DirectBuffer key, ref DirectBuffer value)
        {
            var res = IsReadOnly
                ? NativeMethods.AssertRead(NativeMethods.mdb_cursor_get(_readHandle.Handle, ref key, ref value, operation))
                : NativeMethods.AssertRead(NativeMethods.mdb_cursor_get(_writeHandle, ref key, ref value, operation));
            return res != NativeMethods.MDB_NOTFOUND;
        }

        #endregion mdb_cursor_get

        #region mdb_cursor_put

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void EnsureWriteable()
        {
            if (!(_readHandle == null && _writeHandle != IntPtr.Zero))
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
        public bool TryPut(ref DirectBuffer key, ref DirectBuffer value, CursorPutOptions options)
        {
            EnsureWriteable();
            var res = NativeMethods.mdb_cursor_put(_writeHandle, ref key, ref value, options);
            return res == 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Put(ref DirectBuffer key, ref DirectBuffer value, CursorPutOptions options)
        {
            EnsureWriteable();
            NativeMethods.AssertExecute(NativeMethods.mdb_cursor_put(_writeHandle, ref key, ref value, options));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Add(ref DirectBuffer key, ref DirectBuffer value)
        {
            EnsureWriteable();
            NativeMethods.AssertExecute(NativeMethods.mdb_cursor_put(_writeHandle, ref key, ref value, CursorPutOptions.NoOverwrite));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Replace(ref DirectBuffer key, ref DirectBuffer value)
        {
            EnsureWriteable();
            NativeMethods.AssertExecute(NativeMethods.mdb_cursor_put(_writeHandle, ref key, ref value, CursorPutOptions.Current));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Append(ref DirectBuffer key, ref DirectBuffer value, bool dup = false)
        {
            EnsureWriteable();
            NativeMethods.AssertExecute(
                NativeMethods.mdb_cursor_put(_writeHandle, ref key, ref value,
                    dup ? CursorPutOptions.AppendDuplicateData : CursorPutOptions.AppendData));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Reserve(ref DirectBuffer key, ref DirectBuffer value)
        {
            EnsureWriteable();
            if (Database.OpenFlags.HasFlag(DbFlags.DuplicatesSort)) throw new NotSupportedException("Reserve is not supported for DupSort");
            NativeMethods.AssertExecute(NativeMethods.mdb_cursor_put(_writeHandle, ref key, ref value, CursorPutOptions.ReserveSpace));
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
            return 0 == NativeMethods.AssertRead(NativeMethods.mdb_cursor_del(_writeHandle, option));
        }

        /// <summary>
        /// Return count of duplicates for current key.
        /// This call is only valid on databases that support sorted duplicate data items MDB_DUPSORT.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ulong Count(CursorDeleteOption option)
        {
            NativeMethods.AssertRead(NativeMethods.mdb_cursor_count(_readHandle.Handle, out var result));
            return (ulong)result;
        }

        #endregion mdb_cursor_del + mdb_cursor_count
    }
}
