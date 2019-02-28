// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

using System;

namespace Spreads.LMDB
{
    /// <summary>
    /// Transaction open mode
    /// </summary>
    public enum TransactionBeginFlags
    {
        /// <summary>
        /// Normal mode
        /// </summary>
        ReadWrite = 0,

        /// <summary>
        /// MDB_NOSYNC. Don't flush system buffers to disk when committing a transaction.
        /// This optimization means a system crash can corrupt the database or lose the last transactions if buffers are not yet flushed to disk.
        /// The risk is governed by how often the system flushes dirty buffers to disk and how often mdb_env_sync() is called.
        /// However, if the filesystem preserves write order and the MDB_WRITEMAP flag is not used, transactions exhibit ACI (atomicity, consistency, isolation) properties and only lose D (durability).
        /// I.e. database integrity is maintained, but a system crash may undo the final transactions.
        /// Note that (MDB_NOSYNC | MDB_WRITEMAP) leaves the system with no hint for when to write transactions to disk, unless mdb_env_sync() is called.
        /// (MDB_MAPASYNC | MDB_WRITEMAP) may be preferable.
        /// This flag may be changed at any time using mdb_env_set_flags().
        /// </summary>
        NoSync = 0x10000,

        /// <summary>
        /// MDB_RDONLY. Open the environment in read-only mode.
        /// No write operations will be allowed.
        /// MDB will still modify the lock file - except on read-only filesystems, where MDB does not use locks.
        /// </summary>
        ReadOnly = 0x20000,

        /// <summary>
        /// MDB_NOMETASYNC. Flush system buffers to disk only once per transaction, omit the metadata flush.
        /// Defer that until the system flushes files to disk, or next non-MDB_RDONLY commit or mdb_env_sync().
        /// This optimization maintains database integrity, but a system crash may undo the last committed transaction.
        /// I.e. it preserves the ACI (atomicity, consistency, isolation) but not D (durability) database property.
        /// This flag may be changed at any time using mdb_env_set_flags().
        /// </summary>
        NoMetaSync = 0x40000,
    }

    /// <summary>
    /// Transaction state.
    /// </summary>
    public enum TransactionState
    {
        /// <summary>
        /// 
        /// </summary>
        Disposed,

        /// <summary>
        /// Transaction is currently active.
        /// </summary>
        Active,

        /// <summary>
        /// Transaction is aborted.
        /// </summary>
        Aborted,

        /// <summary>
        /// Transaction is committed.
        /// </summary>
        Committed,

        /// <summary>
        /// Transaction is reset.
        /// </summary>
        Reset
    }

    /// <summary>
    /// Special options for put operation.
    /// </summary>
    [Flags]
    public enum TransactionPutOptions : uint
    {
        /// <summary>
        /// No special behavior.
        /// </summary>
        None = 0,

        /// <summary>
        /// Only for MDB_DUPSORT
        /// For put: don't write if the key and data pair already exist.
        /// For mdb_cursor_del: remove all duplicate data items.
        /// </summary>
        NoDuplicateData = 0x20,

        /// <summary>
        /// For put: Don't write if the key already exists.
        /// </summary>
        NoOverwrite = 0x10,

        /// <summary>
        /// For put: Just reserve space for data, don't copy it. Return a pointer to the reserved space.
        /// </summary>
        ReserveSpace = 0x10000,

        /// <summary>
        /// Data is being appended, don't split full pages.
        /// </summary>
        AppendData = 0x20000,

        /// <summary>
        /// Duplicate data is being appended, don't split full pages.
        /// </summary>
        AppendDuplicateData = 0x40000,
    }
}
