// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.


using System;

namespace Spreads.LMDB
{
    /// <summary>
    /// Options to open LMDB environment
    /// </summary>
    [Flags]
    public enum DbEnvironmentFlags
    {
        /// <summary>
        /// No special options.
        /// </summary>
        None = 0,

        /// <summary>
        /// MDB_FIXEDMAP. use a fixed address for the mmap region. 
        /// This flag must be specified when creating the environment, and is stored persistently in the environment. 
        /// If successful, the memory map will always reside at the same virtual address and pointers used to reference data items in the database will be constant across multiple invocations. 
        /// This option may not always work, depending on how the operating system has allocated memory to shared libraries and other uses. 
        /// The feature is highly experimental.
        /// </summary>
        FixedMap = 0x01,

        /// <summary>
        /// MDB_NOSUBDIR. By default, MDB creates its environment in a directory whose pathname is given in path, and creates its data and lock files under that directory. 
        /// With this option, path is used as-is for the database main data file. 
        /// The database lock file is the path with "-lock" appended.
        /// </summary>
        NoSubDir = 0x4000,

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
        
        /// <summary>
        /// MDB_WRITEMAP Use a writeable memory map unless MDB_RDONLY is set. 
        /// This is faster and uses fewer mallocs, but loses protection from application bugs like wild pointer writes and other bad updates into the database. 
        /// Incompatible with nested transactions.
        /// </summary>
        WriteMap = 0x80000, 

        /// <summary>
        /// MDB_MAPASYNC. When using MDB_WRITEMAP, use asynchronous flushes to disk. 
        /// As with MDB_NOSYNC, a system crash can then corrupt the database or lose the last transactions. 
        /// Calling mdb_env_sync() ensures on-disk database integrity until next commit. 
        /// This flag may be changed at any time using mdb_env_set_flags().
        /// </summary>
        MapAsync = 0x100000,

        /// <summary>
        /// MDB_NOTLS Don't use Thread-Local Storage. Tie reader locktable slots to MDB_txn 
        /// objects instead of to threads. I.e. mdb_txn_reset() keeps the slot reseved for the
        ///  MDB_txn object. A thread may use parallel read-only transactions. A read-only 
        /// transaction may span threads if the user synchronizes its use. Applications that 
        /// multiplex many user threads over individual OS threads need this option. Such an 
        /// application must also serialize the write transactions in an OS thread, since LMDB's
        ///  write locking is unaware of the user threads.
        /// </summary>
        [Obsolete("This option if forced in Spreads.LMDB")]
        NoTls = 0x200000,

        /// <summary>
        /// MDB_NOLOCK Don't do any locking. If concurrent access is anticipated, the caller 
        /// must manage all concurrency itself. For proper operation the caller must enforce
        ///  single-writer semantics, and must ensure that no readers are using old transactions
        ///  while a writer is active. The simplest approach is to use an exclusive lock so
        ///  that no readers may be active at all when a writer begins.
        /// </summary>
        NoLock = 0x400000,

        /// <summary>
        /// MDB_NORDAHEAD Turn off readahead. Most operating systems perform readahead
        ///  on read requests by default. This option turns it off if the OS supports it. 
        /// Turning it off may help random read performance when the DB is larger than RAM 
        /// and system RAM is full. The option is not implemented on Windows.
        /// </summary>
        NoReadAhead = 0x800000,

        /// <summary>
        /// MDB_NOMEMINIT Don't initialize malloc'd memory before writing to unused spaces 
        /// in the data file. By default, memory for pages written to the data file is 
        /// obtained using malloc. While these pages may be reused in subsequent transactions,
        ///  freshly malloc'd pages will be initialized to zeroes before use. This avoids 
        /// persisting leftover data from other code (that used the heap and subsequently 
        /// freed the memory) into the data file. Note that many other system libraries may 
        /// allocate and free memory from the heap for arbitrary uses. E.g., stdio may use 
        /// the heap for file I/O buffers. This initialization step has a modest performance 
        /// cost so some applications may want to disable it using this flag. This option 
        /// can be a problem for applications which handle sensitive data like passwords, 
        /// and it makes memory checkers like Valgrind noisy. This flag is not needed with
        ///  MDB_WRITEMAP, which writes directly to the mmap instead of using malloc for pages.
        ///  The initialization is also skipped if MDB_RESERVE is used; the caller is expected 
        /// to overwrite all of the memory that was reserved in that case. This flag may 
        /// be changed at any time using mdb_env_set_flags().
        /// </summary>
        NoMemInit = 0x1000000
    }


    public enum EnvironmentCopyFlags {
        None = 0,

        /// <summary>
        /// MDB_CP_COMPACT. Compacting copy: Omit free space from copy, and renumber all pages sequentially.
        /// </summary>
        Compact = 0x01
    }
}
