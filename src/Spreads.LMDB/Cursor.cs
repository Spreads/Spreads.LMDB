// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

using Spreads.Collections.Concurrent;
using Spreads.LMDB.Interop;
using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Spreads.LMDB
{
    /// <summary>
    /// Cursor to iterate over a database
    /// </summary>
    public class Cursor : IDisposable
    {
        // Read-only cursors are pooled in Database instances since cursors belog to a DB
        // Here we pool only the objects
        internal IntPtr _writeHandle;

        internal ReadCursorHandle _readHandle;
        internal Database _database;
        internal Transaction _transaction;

        #region Lifecycle

        private static readonly ObjectPool<Cursor> CursorPool =
            new ObjectPool<Cursor>(() => new Cursor(), System.Environment.ProcessorCount * 16);

        private Cursor()
        { }

        /// <summary>
        /// Creates new instance of LightningCursor
        /// </summary>
        internal static Cursor Create(Database db, Transaction txn, ReadCursorHandle rh = null)
        {
            var c = CursorPool.Allocate();

            c._database = db ?? throw new ArgumentNullException(nameof(db));
            c._transaction = txn ?? throw new ArgumentNullException(nameof(txn));

            if (rh != null)
            {
                if (!txn.IsReadOnly)
                {
                    throw new InvalidOperationException("Txn must be readonly to renew a cursor");
                }

                if (rh.IsInvalid)
                {
                    NativeMethods.AssertExecute(NativeMethods.mdb_cursor_open(txn._readHandle, db._handle, out IntPtr handle));
                    rh.SetNewHandle(handle);
                }
                else
                {
                    NativeMethods.AssertExecute(NativeMethods.mdb_cursor_renew(txn._readHandle, rh));
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

        ~Cursor()
        {
            Dispose(false);
        }

        #endregion Lifecycle

        public bool IsReadOnly => _readHandle != null;

        /// <summary>
        /// Cursor's environment.
        /// </summary>
        public Environment Environment => Database.Environment;

        /// <summary>
        /// Cursor's database.
        /// </summary>
        public Database Database => _database;

        /// <summary>
        /// Cursor's transaction.
        /// </summary>
        public Transaction Transaction => _transaction;

        #region sdb_cursor_find

        internal bool TryFind(Lookup direction, ref MDB_val key, out MDB_val value)
        {
            int res = 0;
            value = default;

            switch (direction)
            {
                case Lookup.LT:
                    res = NativeMethods.AssertRead(
                        IsReadOnly
                            ? NativeMethods.sdb_cursor_get_lt(_readHandle, ref key, out value)
                            : NativeMethods.sdb_cursor_get_lt(_writeHandle, ref key, out value)
                        );
                    break;

                case Lookup.LE:
                    res = NativeMethods.AssertRead(
                        IsReadOnly
                            ? NativeMethods.sdb_cursor_get_le(_readHandle, ref key, out value)
                            : NativeMethods.sdb_cursor_get_le(_writeHandle, ref key, out value)
                    );
                    break;

                case Lookup.EQ:
                    res = NativeMethods.AssertRead(
                        IsReadOnly
                            ? NativeMethods.sdb_cursor_get_eq(_readHandle, ref key, out value)
                            : NativeMethods.sdb_cursor_get_eq(_writeHandle, ref key, out value)
                    );
                    break;

                case Lookup.GE:
                    res = NativeMethods.AssertRead(
                        IsReadOnly
                            ? NativeMethods.sdb_cursor_get_ge(_readHandle, ref key, out value)
                            : NativeMethods.sdb_cursor_get_ge(_writeHandle, ref key, out value)
                    );
                    break;

                case Lookup.GT:
                    res = NativeMethods.AssertRead(
                        IsReadOnly
                            ? NativeMethods.sdb_cursor_get_gt(_readHandle, ref key, out value)
                            : NativeMethods.sdb_cursor_get_gt(_writeHandle, ref key, out value)
                    );
                    break;
            }

            return res != NativeMethods.MDB_NOTFOUND;
        }

        internal bool TryFindDup(Lookup direction, ref MDB_val key, out MDB_val value)
        {
            int res = 0;
            value = default(MDB_val);

            switch (direction)
            {
                case Lookup.LT:
                    res = NativeMethods.AssertRead(
                        IsReadOnly
                            ? NativeMethods.sdb_cursor_get_lt_dup(_readHandle, ref key, out value)
                            : NativeMethods.sdb_cursor_get_lt_dup(_writeHandle, ref key, out value)
                        );
                    break;

                case Lookup.LE:
                    res = NativeMethods.AssertRead(
                        IsReadOnly
                            ? NativeMethods.sdb_cursor_get_le_dup(_readHandle, ref key, out value)
                            : NativeMethods.sdb_cursor_get_le_dup(_writeHandle, ref key, out value)
                        );
                    break;

                case Lookup.EQ:
                    res = NativeMethods.AssertRead(
                        IsReadOnly
                            ? NativeMethods.sdb_cursor_get_eq_dup(_readHandle, ref key, out value)
                            : NativeMethods.sdb_cursor_get_eq_dup(_writeHandle, ref key, out value)
                    );
                    break;

                case Lookup.GE:
                    res = NativeMethods.AssertRead(
                        IsReadOnly
                            ? NativeMethods.sdb_cursor_get_ge_dup(_readHandle, ref key, out value)
                            : NativeMethods.sdb_cursor_get_ge_dup(_writeHandle, ref key, out value)
                    );
                    break;

                case Lookup.GT:
                    res = NativeMethods.AssertRead(
                        IsReadOnly
                            ? NativeMethods.sdb_cursor_get_gt_dup(_readHandle, ref key, out value)
                            : NativeMethods.sdb_cursor_get_gt_dup(_writeHandle, ref key, out value)
                    );
                    break;
            }

            return res != NativeMethods.MDB_NOTFOUND;
        }

        #endregion sdb_cursor_find

        #region mdb_cursor_get

        public bool TryGet(
            CursorGetOption operation, ref MDB_val key, ref MDB_val value)
        {
            var res = IsReadOnly
                ? NativeMethods.AssertRead(NativeMethods.mdb_cursor_get(_readHandle, ref key, ref value, operation))
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
                ThrowNonWriteable();
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private void ThrowNonWriteable()
        {
            throw new InvalidOperationException("Cursor is readonly or invalid");
        }

        public bool TryPut(ref MDB_val key, ref MDB_val value, CursorPutOptions options)
        {
            EnsureWriteable();
            var res = NativeMethods.mdb_cursor_put(_writeHandle, ref key, ref value, options);
            return res == 0;
        }

        public void Put(ref MDB_val key, ref MDB_val value, CursorPutOptions options)
        {
            EnsureWriteable();
            NativeMethods.AssertExecute(NativeMethods.mdb_cursor_put(_writeHandle, ref key, ref value, options));
        }

        public void Add(ref MDB_val key, ref MDB_val value)
        {
            EnsureWriteable();
            NativeMethods.AssertExecute(NativeMethods.mdb_cursor_put(_writeHandle, ref key, ref value, CursorPutOptions.NoOverwrite));
        }

        public void Replace(ref MDB_val key, ref MDB_val value)
        {
            EnsureWriteable();
            NativeMethods.AssertExecute(NativeMethods.mdb_cursor_put(_writeHandle, ref key, ref value, CursorPutOptions.Current));
        }

        public void Append(ref MDB_val key, ref MDB_val value, bool dup = false)
        {
            EnsureWriteable();
            NativeMethods.AssertExecute(
                NativeMethods.mdb_cursor_put(_writeHandle, ref key, ref value,
                    dup ? CursorPutOptions.AppendDuplicateData : CursorPutOptions.AppendData));
        }

        public void Reserve(ref MDB_val key, ref MDB_val value)
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
        private bool Delete(CursorDeleteOption option)
        {
            EnsureWriteable();
            return 0 == NativeMethods.AssertRead(NativeMethods.mdb_cursor_del(_writeHandle, option));
        }

        /// <summary>
        /// Return count of duplicates for current key.
        /// This call is only valid on databases that support sorted duplicate data items MDB_DUPSORT.
        /// </summary>
        public ulong Count(CursorDeleteOption option)
        {
            NativeMethods.AssertRead(NativeMethods.mdb_cursor_count(_readHandle, out var result));
            return (ulong)result;
        }

        #endregion mdb_cursor_del + mdb_cursor_count
    }
}
