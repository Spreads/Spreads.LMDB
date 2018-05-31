// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

using Spreads.LMDB.Interop;
using System;
using Spreads.Collections.Concurrent;

namespace Spreads.LMDB
{
    /// <summary>
    /// Database.
    /// </summary>
    public class Database : IDisposable
    {
        internal readonly ObjectPool<ReadCursorHandle> ReadHandlePool =
            new ObjectPool<ReadCursorHandle>(() => new ReadCursorHandle(), System.Environment.ProcessorCount * 16);

        internal uint _handle;
        private readonly DatabaseConfig _config;
        private readonly Environment _environment;
        private readonly string _name;

        internal Database(string name, Transaction txn, DatabaseConfig config)
        {
            if (txn.IsReadOnly) { throw new InvalidOperationException("Cannot create a DB with RO transaction"); }
            if (txn == null) { throw new ArgumentNullException(nameof(txn)); }
            _config = config ?? throw new ArgumentNullException(nameof(config));
            _name = name;
            _environment = txn.Environment;

            NativeMethods.AssertExecute(NativeMethods.mdb_dbi_open(txn._writeHandle, name, _config.OpenFlags, out var handle));
            if (_config.CompareFunction != null)
            {
                NativeMethods.AssertExecute(NativeMethods.mdb_set_compare(txn._writeHandle, handle, _config.CompareFunction));
            }
            if (_config.DupSortFunction != null)
            {
                NativeMethods.AssertExecute(NativeMethods.mdb_set_dupsort(txn._writeHandle, handle, _config.DupSortFunction));
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
        public Environment Environment => _environment;

        /// <summary>
        /// Flags with which the database was opened.
        /// </summary>
        public DbFlags OpenFlags => _config.OpenFlags;


        public Cursor OpenWriteCursor(Transaction txn)
        {
            if (txn.IsReadOnly)
            {
                throw new InvalidOperationException("Transaction is readonly. Cannot open write cursor.");
            }
            return Cursor.Create(this, txn, null);
        }

        public Cursor OpenReadCursor(Transaction txn)
        {
            if (!txn.IsReadOnly)
            {
                throw new InvalidOperationException("Transaction is not readonly. Cannot open read cursor.");
            }

            var rh = ReadHandlePool.Allocate();
            return Cursor.Create(this, txn, rh);
        }

        /// <summary>
        /// Drops the database.
        /// </summary>
        public bool Drop(Transaction transaction)
        {
            using (var tx = Transaction.Create(_environment, TransactionBeginFlags.ReadWrite))
            {
                var res = NativeMethods.AssertExecute(NativeMethods.mdb_drop(tx._writeHandle, _handle, true));
                _handle = default;
                return res == 0;
            }
        }

        /// <summary>
        /// Truncates all data from the database.
        /// </summary>
        public bool Truncate(Transaction transaction)
        {
            using (var tx = Transaction.Create(_environment, TransactionBeginFlags.ReadWrite))
            {
                var res = NativeMethods.AssertExecute(NativeMethods.mdb_drop(tx._writeHandle, _handle, false));
                _handle = default;
                return res == 0;
            }
        }

        public MDB_stat GetStat()
        {
            using (var tx = Transaction.Create(Environment, TransactionBeginFlags.ReadOnly))
            {
                NativeMethods.AssertRead(NativeMethods.mdb_stat(tx._readHandle, _handle, out var stat));
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
