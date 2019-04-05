// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

using Spreads.Buffers;
using Spreads.Collections.Concurrent;
using Spreads.LMDB.Interop;
using Spreads.Serialization;
using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

namespace Spreads.LMDB
{
    /// <summary>
    /// LMDB Environment.
    /// </summary>
    public class LMDBEnvironment : IDisposable
    {
        /// <summary>
        /// Print LMDB errors as Trace.Error
        /// </summary>
        public static bool TraceErrors { get; set; } = false;

        private volatile int _instanceCount;
        private readonly UnixAccessMode _accessMode;
        private readonly bool _disableReadTxnAutoreset;
        private readonly LMDBEnvironmentFlags _openFlags;
        internal EnvironmentHandle _handle;
        private int _maxDbs;
        private int _pageSize;
        private int _overflowPageHeaderSize;

        private readonly BlockingCollection<Delegates> _writeQueue;

        private readonly TaskCompletionSource<object> _writeTaskCompletion = new TaskCompletionSource<object>();
        private readonly CancellationTokenSource _cts;
        private readonly string _directory;
        private bool _isOpen;

        private uint _maxReaders;

        internal LockedObjectPool<TransactionImpl> ReadTxnPool;

        // Useful for testing when simulating multiple processes in a single one
        // and not dealing with LMDB-specific multi-process issues, but instead
        // avoid the breakage from opening LMDB env twice
        // See Caveats: http://www.lmdb.tech/doc/index.html
        // Not thread-safe because it happens once per env at process start/end
        private static ConcurrentDictionary<string, LMDBEnvironment> _openEnvs = new ConcurrentDictionary<string, LMDBEnvironment>();

        /// <summary>
        /// Creates a new instance of Environment.
        /// </summary>
        /// <param name="directory">Relative directory for storing database files.</param>
        /// <param name="openFlags">Database open options.</param>
        /// <param name="accessMode">Unix file access privelegies (optional). Only makes sense on unix operationg systems.</param>
        /// <param name="disableAsync">Disable dedicated writer thread and NO_TLS option.
        /// Fire-and-forget option for write transaction will throw.
        /// .NET async cannot be used in transaction body. True by default. </param>
        /// <param name="disableReadTxnAutoreset">Abort read-only transactions instead of resetting them. Should be true for multiple (a lot of) processes accessing the same env.</param>
        public static LMDBEnvironment Create(string directory = null,
            LMDBEnvironmentFlags openFlags = LMDBEnvironmentFlags.None,
            UnixAccessMode accessMode = UnixAccessMode.Default,
            bool disableAsync = true, bool disableReadTxnAutoreset = false)
        {
#pragma warning disable 618
            if (!disableAsync)
            {
                // we need NoTLS to work well with .NET Tasks, see docs about writers that need a dedicated thread
                openFlags = openFlags | LMDBEnvironmentFlags.NoTls;
            }
#pragma warning restore 618

            // this is machine-local storage for each user.
            if (string.IsNullOrWhiteSpace(directory))
            {
                directory = Config.DbEnvironment.DefaultLocation;
            }
            var env = _openEnvs.GetOrAdd(directory, (dir) => new LMDBEnvironment(dir, openFlags, accessMode, disableAsync, disableReadTxnAutoreset));
            if (env._openFlags != openFlags || env._accessMode != accessMode)
            {
                throw new InvalidOperationException("Environment is already open in this process with different flags and access mode.");
            }
            env._instanceCount++;
            return env;
        }

        private LMDBEnvironment(string directory,
            LMDBEnvironmentFlags openFlags = LMDBEnvironmentFlags.None,
            UnixAccessMode accessMode = UnixAccessMode.Default,
            bool disableWriterThread = false,
            bool disableReadTxnAutoreset = false)
        {
            if (!System.IO.Directory.Exists(directory))
            {
                System.IO.Directory.CreateDirectory(directory);
            }

            NativeMethods.AssertExecute(NativeMethods.mdb_env_create(out var envHandle));
            _handle = envHandle;
            _accessMode = accessMode;
            _disableReadTxnAutoreset = disableReadTxnAutoreset;

            _directory = directory;
            _openFlags = openFlags;

            MaxDatabases = Config.DbEnvironment.DefaultMaxDatabases;

            // Writer Task
            // In the current process writes are serialized via the blocking queue
            // Accross processes, writes are synchronized via WriteTxnGate (TODO!)
            _cts = new CancellationTokenSource();

            if (!disableWriterThread)
            {
                _writeQueue = new BlockingCollection<Delegates>();
                var threadStart = new ThreadStart(() =>
                {
                    while (!_writeQueue.IsCompleted)
                    {
                        try
                        {
                            // BLOCKING
                            var delegates = _writeQueue.Take(_cts.Token);

                            var transactionImpl = TransactionImpl.Create(this, TransactionBeginFlags.ReadWrite);

                            try
                            {
                                // TODO for some methods such as mbd_put we should have a C method
                                // that begins/end txn automatically
                                // we still need to pass call to it here, but we could avoid txn creation
                                // and two P/Invokes

                                var txn = new Transaction(transactionImpl);

                                if (delegates.WriteFunction != null)
                                {
                                    var res = delegates.WriteFunction(txn);
                                    delegates.Tcs?.SetResult(res);
                                }
                                else if (delegates.WriteAction != null)
                                {
                                    delegates.WriteAction(txn);
                                    delegates.Tcs?.SetResult(null);
                                }
                                else
                                {
                                    Environment.FailFast("Wrong writer thread setup");
                                }
                            }
                            catch (Exception e)
                            {
                                delegates.Tcs?.SetException(e);
                                transactionImpl?.Abort();
                            }
                            finally
                            {
                                transactionImpl?.Dispose();
                            }
                        }
                        catch (InvalidOperationException)
                        {
                        }
                    }

                    _writeTaskCompletion.SetResult(null);
                });
                var writeThread = new Thread(threadStart)
                {
                    Name = "LMDB Writer thread",
                    IsBackground = true
                };
                writeThread.Start();
            }
        }

        /// <summary>
        /// Open the environment.
        /// </summary>
        public void Open()
        {
            if (!System.IO.Directory.Exists(_directory))
            {
                System.IO.Directory.CreateDirectory(_directory);
            }

            if (!_isOpen)
            {
                var ptr = NativeMethods.StringToHGlobalUTF8(_directory);
                NativeMethods.AssertExecute(NativeMethods.mdb_env_open(_handle, ptr, _openFlags, _accessMode));
                if (ptr != IntPtr.Zero)
                {
                    Marshal.FreeHGlobal(ptr);
                }
            }

            _isOpen = true;
            var maxPooledReaders = Math.Max(16, Math.Min(Environment.ProcessorCount * 2, MaxReaders - Environment.ProcessorCount * 2));
            var poolSize = _disableReadTxnAutoreset ? 1 : maxPooledReaders;
            ReadTxnPool = new LockedObjectPool<TransactionImpl>(poolSize, () => { return null; }, false);
        }

        /// <summary>
        /// Automatically abort a write transaction on dispose without explicit commit or dispose.
        /// If this property is set to false then explicit abort or commit is required, otherwise
        /// <see cref="InvalidOperationException"/> is thrown.
        /// </summary>
        /// <remarks>
        /// It's not clear if auto abort or auto commit are more intuitive with `using(){}` pattern,
        /// but both make debugging harder and errors hard to find.
        /// Auto abort is better because `using(){}` guarantees (as in `try/finally`) disposal on
        /// an exception and native transaction will be closed without changing existing data.
        /// Commiting after an unhandled exception is obviously wrong.
        /// </remarks>
        public bool AutoAbort { get; set; } = false;

        public object Write(Func<Transaction, object> writeFunction, bool fireAndForget = false)
        {
            if (fireAndForget)
            {
                return WriteAsync(writeFunction, true).Result;
            }
            else
            {
#pragma warning disable 618
                using (var txn = BeginTransaction())
#pragma warning restore 618
                {
                    var result = writeFunction(txn);
                    return result;
                }
            }
        }

        //internal object Write(Func<Transaction, object> writeFunction, bool fireAndForget, bool skipTxnCreate)
        //{
        //    return WriteAsync(writeFunction, fireAndForget, skipTxnCreate).Result;
        //}

        public void Write(Action<Transaction> writeAction, bool fireAndForget = false)
        {
            if (fireAndForget)
            {
                WriteAsync(writeAction, true).Wait();
            }
            else
            {
#pragma warning disable 618
                using (var txn = BeginTransaction())
#pragma warning restore 618
                {
                    writeAction(txn);
                }
            }
        }

        /// <summary>
        /// Queue a write action and spin until it is completed unless fireAndForget is true.
        /// If fireAndForget is true then return immediately.
        /// </summary>
        internal Task<object> WriteAsync(Func<Transaction, object> writeFunction, bool fireAndForget = false)
        {
            TaskCompletionSource<object> tcs;
            if (!_writeQueue.IsAddingCompleted)
            {
                tcs = fireAndForget ? null : new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
                var act = new Delegates
                {
                    WriteFunction = writeFunction,
                    Tcs = tcs
                };

                _writeQueue.Add(act);
            }
            else
            {
                throw new OperationCanceledException();
            }

            if (fireAndForget)
            {
                return Task.FromResult<object>(null);
            }

            return tcs.Task;
        }

        public Task WriteAsync(Action<Transaction> writeAction, bool fireAndForget = false)
        {
            TaskCompletionSource<object> tcs;
            if (!_writeQueue.IsAddingCompleted)
            {
                tcs = fireAndForget ? null : new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
                var act = new Delegates
                {
                    WriteAction = writeAction,
                    Tcs = tcs
                };

                _writeQueue.Add(act);
            }
            else
            {
                throw new OperationCanceledException();
            }

            if (fireAndForget) { return Task.CompletedTask; }

            return tcs.Task;
        }

        /// <summary>
        /// Perform a read transaction.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public T Read<T>(Func<ReadOnlyTransaction, T> readFunc)
        {
            using (var txn = TransactionImpl.Create(this, TransactionBeginFlags.ReadOnly))
            {
                var rotxn = new ReadOnlyTransaction(txn);
                return readFunc(rotxn);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public T Read<T>(Func<ReadOnlyTransaction, object, T> readFunc, object state)
        {
            using (var txn = TransactionImpl.Create(this, TransactionBeginFlags.ReadOnly))
            {
                var rotxn = new ReadOnlyTransaction(txn);
                return readFunc(rotxn, state);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Read(Action<ReadOnlyTransaction> readAction)
        {
            using (var txn = TransactionImpl.Create(this, TransactionBeginFlags.ReadOnly))
            {
                var rotxn = new ReadOnlyTransaction(txn);
                readAction(rotxn);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Read(Action<ReadOnlyTransaction, object> readAction, object state)
        {
            using (var txn = TransactionImpl.Create(this, TransactionBeginFlags.ReadOnly))
            {
                var rotxn = new ReadOnlyTransaction(txn);
                readAction(rotxn, state);
            }
        }

        /// <summary>
        /// A thread can only use one transaction at a time, plus any child transactions. Each transaction belongs to one thread.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Transaction BeginTransaction(TransactionBeginFlags flags = TransactionBeginFlags.ReadWrite)
        {
            if (((int)flags & (int)TransactionBeginFlags.ReadOnly) != 0)
            {
                ThrowShouldUseReadOnlyTxn();
            }
            var impl = TransactionImpl.Create(this, flags);
            var txn = new Transaction(impl);
            return txn;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void ThrowShouldUseReadOnlyTxn()
        {
            throw new InvalidOperationException("Use BeginReadOnlyTransaction for readonly transactions");
        }

        /// <summary>
        /// ReadOnlyTransaction transaction must be disposed ASAP to avoid LMDB environment growth and allocating garbage.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlyTransaction BeginReadOnlyTransaction()
        {
            var txn = TransactionImpl.Create(this, TransactionBeginFlags.ReadOnly);
            var rotxn = new ReadOnlyTransaction(txn);
            return rotxn;
        }

        public Database OpenDatabase(string name, DatabaseConfig config)
        {
#pragma warning disable 618
            using (var txn = BeginTransaction())
            {
#pragma warning restore 618
                try
                {
                    var db = new Database(name, txn._impl, config);
                    txn._impl.Commit();
                    return db;
                }
                catch
                {
                    txn.Abort();
                    throw;
                }
            }
        }

        public void Sync(bool force)
        {
            NativeMethods.AssertExecute(NativeMethods.mdb_env_sync(_handle, force));
        }

        /// <summary>
        /// Close the environment and release the memory map.
        /// Only a single thread may call this function. All transactions, databases, and cursors must already be closed before calling this function.
        /// Attempts to use any such handles after calling this function will cause a SIGSEGV.
        /// The environment handle will be freed and must not be used again after this call.
        /// </summary>
        public void Close()
        {
            Close(false);
        }

        private void Close(bool force)
        {
            _instanceCount--;
            if (_instanceCount < 0)
            {
                throw new InvalidOperationException("Multiple disposal of environment");
            }
            if (_instanceCount == 0 || force)
            {
                if (!force)
                {
                    GC.SuppressFinalize(this);
                }

                lock (ReadTxnPool) // just some internal obj that always exists
                {
                    if (!_isOpen)
                    {
                        return;
                    }

                    _isOpen = false;
                }

                if (_writeQueue != null)
                {
                    _writeQueue.CompleteAdding();
                    // let finish already added write tasks
                    _writeTaskCompletion.Task.Wait();
                    Trace.Assert(_writeQueue.Count == 0, "Write queue must be empty on exit");
                }

                ReadTxnPool.Dispose();

                _cts.Cancel();
                // NB handle dispose does this: NativeMethods.mdb_env_close(_handle);
                _handle.Dispose();

                _openEnvs.TryRemove(_directory, out _);
            }
        }

        public MDB_stat GetStat()
        {
            EnsureOpened();
            NativeMethods.AssertRead(NativeMethods.mdb_env_stat(_handle, out var stat));
            return stat;
        }

        public int ReaderCheck()
        {
            EnsureOpened();
            NativeMethods.AssertRead(NativeMethods.mdb_reader_check(_handle, out var dead));
            return dead;
        }

        /// <summary>
        /// Number of entires in the main database
        /// </summary>
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

        public MDB_envinfo GetEnvInfo()
        {
            EnsureOpened();
            NativeMethods.AssertExecute(NativeMethods.mdb_env_info(_handle, out var info));
            return info;
        }

        /// <summary>
        /// Whether the environment is opened.
        /// </summary>
        public bool IsOpen => _isOpen;

        /// Set the size of the memory map to use for this environment.
        /// The size should be a multiple of the OS page size.
        /// The default is 10485760 bytes.
        /// The size of the memory map is also the maximum size of the database.
        /// The value should be chosen as large as possible, to accommodate future growth of the database.
        /// This function may only be called before the environment is opened.
        /// The size may be changed by closing and reopening the environment.
        /// Any attempt to set a size smaller than the space already consumed by the environment will be silently changed to the current size of the used space.
        public long MapSize
        {
            get
            {
                var info = GetEnvInfo();
                return info.me_mapsize.ToInt64();
            }
            set
            {
                if (_isOpen)
                {
                    // during tests we return same env instance, ignore setting the same size to it
                    if (value == MapSize) { return; }
                    throw new InvalidOperationException("Can't change MapSize of an opened environment");
                }
                NativeMethods.AssertExecute(NativeMethods.mdb_env_set_mapsize(_handle, (IntPtr)value));
            }
        }

        public int PageSize
        {
            get
            {
                if (_pageSize == 0)
                {
                    var stat = GetStat();
                    _pageSize = (int)stat.ms_psize;
                }
                return _pageSize;
            }
        }

        public int OverflowPageHeaderSize
        {
            get
            {
                if (_overflowPageHeaderSize == 0)
                {
                    if (((int)_openFlags & (int)LMDBEnvironmentFlags.WriteMap) != 0)
                    {
                        _overflowPageHeaderSize = GetOverflowPageHeaderSize();
                    }
                    else
                    {
                        _overflowPageHeaderSize = -1;
                    }
                }
                return _overflowPageHeaderSize;
            }
        }

        private unsafe int GetOverflowPageHeaderSize()
        {
            if (((int)_openFlags & (int)LMDBEnvironmentFlags.WriteMap) == 0)
            {
                throw new InvalidOperationException("OverflowPageHeaderSize requires DbEnvironmentFlags.WriteMap flag");
            }
#pragma warning disable 618
            using (var txn = BeginTransaction())
            {
#pragma warning restore 618
                try
                {
                    var db = new Database("temp", txn._impl, new DatabaseConfig(DbFlags.Create));
                    var bufferRef = 0L;
                    var keyPtr = Unsafe.AsPointer(ref bufferRef);
                    var key1 = new DirectBuffer(8, (byte*)keyPtr);
                    var value = new DirectBuffer(PageSize * 10, (byte*)IntPtr.Zero);
                    db.Put(txn, ref key1, ref value, TransactionPutOptions.ReserveSpace);
                    db.Dispose();
                    return checked((int)(((IntPtr)value.Data).ToInt64() % PageSize));
                }
                finally
                {
                    txn.Abort();
                }
            }
        }

        /// <summary>
        /// Last used page of the environment multiplied by its page size.
        /// </summary>
        public long UsedSize
        {
            get
            {
                var info = GetEnvInfo();
                return info.me_last_pgno.ToInt64() * PageSize;
            }
        }

        /// <summary>
        /// Get the maximum number of threads for the environment.
        /// </summary>
        public int MaxReaders
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                if (_isOpen && _maxReaders != 0)
                {
                    return (int)_maxReaders;
                }
                NativeMethods.AssertExecute(NativeMethods.mdb_env_get_maxreaders(_handle, out var readers));
                _maxReaders = readers;
                return (int)readers;
            }
            set
            {
                if (_isOpen)
                {
                    if (value == MaxReaders) { return; }
                    throw new InvalidOperationException("Can't change MaxReaders of opened environment");
                }
                NativeMethods.AssertExecute(NativeMethods.mdb_env_set_maxreaders(_handle, (uint)value));
            }
        }

        public int MaxKeySize => NativeMethods.mdb_env_get_maxkeysize(_handle);

        /// <summary>
        /// Set the maximum number of named databases for the environment.
        /// This function is only needed if multiple databases will be used in the environment.
        /// Simpler applications that use the environment as a single unnamed database can ignore this option.
        /// This function may only be called before the environment is opened.
        /// </summary>
        public int MaxDatabases
        {
            get => _maxDbs;
            set
            {
                if (_isOpen)
                {
                    if (value == _maxDbs) return;
                    throw new InvalidOperationException("Can't change MaxDatabases of opened environment");
                }
                NativeMethods.AssertExecute(NativeMethods.mdb_env_set_maxdbs(_handle, (uint)value));
                _maxDbs = value;
            }
        }

        public unsafe long TouchSpace(int megabytes = 0)
        {
            EnsureOpened();
            int size;
            var used = UsedSize;
            if (megabytes == 0)
            {
                size = (int)((MapSize - used) / 2);
                if (size == 0)
                {
                    return used;
                }
            }
            else
            {
                size = megabytes * 1024 * 1024;
            }
            if (size > MapSize - used)
            {
                size = (int)(Math.Min(MapSize - used, int.MaxValue) * 8 / 10);
            }

            if (size <= 0)
            {
                return used;
            }
            var db = OpenDatabase("__touch_space___", new DatabaseConfig(DbFlags.Create));
            using (var txn = BeginTransaction())
            {
                var key = 0;
                var keyPtr = Unsafe.AsPointer(ref key);
                var key1 = new DirectBuffer(TypeHelper<int>.FixedSize, (byte*)keyPtr);
                DirectBuffer value = new DirectBuffer(size, (byte*)IntPtr.Zero);
                db.Put(txn, ref key1, ref value, TransactionPutOptions.ReserveSpace);
                Unsafe.InitBlockUnaligned(value.Data, 0, (uint)value.Length);
                txn.Commit();
            }
            Sync(true);
            using (var txn = BeginTransaction(TransactionBeginFlags.NoSync))
            {
                // db.Truncate(txn);
                db.Drop(txn);
            }
            db.Dispose();
            return UsedSize;
        }

        public long EntriesCount { get { return GetStat().ms_entries.ToInt64(); } }

        /// <summary>
        /// Directory path to store database files.
        /// </summary>
        public string Directory => _directory;

        [Obsolete("Not a part of LMDB API, for testing only")]
        public int InstanceCount => _instanceCount;

        /// <summary>
        /// Copy an MDB environment to the specified path.
        /// This function may be used to make a backup of an existing environment.
        /// </summary>
        /// <param name="path">The directory in which the copy will reside. This directory must already exist and be writable but must otherwise be empty.</param>
        /// <param name="compact">Omit empty pages when copying.</param>
        public void CopyTo(string path, bool compact = false)
        {
            EnsureOpened();
            var flags = compact ? LMDBEnvironmentCopyFlags.Compact : LMDBEnvironmentCopyFlags.None;
            var ptr = NativeMethods.StringToHGlobalUTF8(path);
            NativeMethods.AssertExecute(NativeMethods.mdb_env_copy2(_handle, ptr, flags));
            if (ptr != IntPtr.Zero)
            {
                Marshal.FreeHGlobal(ptr);
            }
        }

        /// <summary>
        /// Flush the data buffers to disk.
        /// Data is always written to disk when LightningTransaction.Commit is called, but the operating system may keep it buffered.
        /// MDB always flushes the OS buffers upon commit as well, unless the environment was opened with EnvironmentOpenFlags.NoSync or in part EnvironmentOpenFlags.NoMetaSync.
        /// </summary>
        /// <param name="force">If true, force a synchronous flush. Otherwise if the environment has the EnvironmentOpenFlags.NoSync flag set the flushes will be omitted, and with MDB_MAPASYNC they will be asynchronous.</param>
        public void Flush(bool force)
        {
            NativeMethods.AssertExecute(NativeMethods.mdb_env_sync(_handle, force));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void EnsureOpened()
        {
            if (!_isOpen) { ThrowIfNotOpened(); }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void ThrowIfNotOpened()
        {
            throw new InvalidOperationException("Environment should be opened");
        }

        private void Dispose(bool disposing)
        {
            Close(!disposing);
        }

        /// <summary>
        /// Dispose the environment and release the memory map.
        /// Only a single thread may call this function. All transactions, databases, and cursors must already be closed before calling this function.
        /// Attempts to use any such handles after calling this function will cause a SIGSEGV.
        /// The environment handle will be freed and must not be used again after this call.
        /// </summary>
        public void Dispose()
        {
            GC.SuppressFinalize(this);
            Dispose(true);
        }

        ~LMDBEnvironment()
        {
            Dispose(false);
        }

        private struct Delegates
        {
            public Func<Transaction, object> WriteFunction;
            public Action<Transaction> WriteAction;
            public TaskCompletionSource<object> Tcs;
        }
    }
}
