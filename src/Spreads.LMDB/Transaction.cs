// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

using Spreads.Collections.Concurrent;
using Spreads.LMDB.Interop;
using System;
using System.Diagnostics;

//using Spreads.DB.Async;

namespace Spreads.LMDB
{
    /// <summary>
    /// Db transaction.
    /// </summary>
    public class Transaction : IDisposable
    {
        private Environment _environment;

        // on dispose must close write and reset read

        internal IntPtr _writeHandle;

        internal ReadTransactionHandle _readHandle;

        private TransactionState _state;

        #region Lifecycle

        private static readonly ObjectPool<Transaction> TxPool =
            new ObjectPool<Transaction>(() => new Transaction(), System.Environment.ProcessorCount * 16);

        private static readonly ObjectPool<ReadTransactionHandle> ReadHandlePool =
            new ObjectPool<ReadTransactionHandle>(() => new ReadTransactionHandle(), System.Environment.ProcessorCount * 16);

        private Transaction()
        {
            _state = TransactionState.Disposed;
        }

        internal static Transaction Create(Environment environment, TransactionBeginFlags beginFlags)
        {
            environment.EnsureOpened();

            var tx = TxPool.Allocate();

            if (tx._state != TransactionState.Disposed)
            {
                throw new InvalidOperationException("Pooled tx must be in disposed state");
            }

            tx._environment = environment;
            // ReSharper disable once BitwiseOperatorOnEnumWithoutFlags
            var isReadOnly = (beginFlags & TransactionBeginFlags.ReadOnly) == TransactionBeginFlags.ReadOnly;
            if (isReadOnly)
            {
                var rh = ReadHandlePool.Allocate();
                if (rh.IsInvalid)
                {
                    // create new
                    NativeMethods.AssertExecute(
                        NativeMethods.mdb_txn_begin(environment._handle, IntPtr.Zero, beginFlags, out IntPtr handle));
                    rh.SetNewHandle(handle);
                }
                else
                {
                    // renew
                    NativeMethods.AssertExecute(NativeMethods.mdb_txn_renew(rh));
                }
                tx._readHandle = rh;
            }
            else
            {
                NativeMethods.AssertExecute(
                    NativeMethods.mdb_txn_begin(environment._handle, IntPtr.Zero, beginFlags, out IntPtr handle));
                tx._writeHandle = handle;
            }
            tx._state = TransactionState.Active;
            return tx;
        }

        protected void Dispose(bool disposing)
        {
            if (_state == TransactionState.Disposed)
            {
                throw new InvalidOperationException("Transaction is already disposed");
            }
            var isReadOnly = IsReadOnly;
            if (isReadOnly)
            {
                if (disposing)
                {
                    var rh = _readHandle;
                    _readHandle = null;
                    NativeMethods.mdb_txn_reset(rh);
                    ReadHandlePool.Free(rh);
                    // handle will be finalized if do not in pool
                }
                else
                {
                    Trace.TraceWarning("Finalizing read transaction. Dispose it explicitly.");
                    _readHandle.Dispose();
                }
            }
            else
            {
                if (disposing)
                {
                    if (_state == TransactionState.Active)
                    {
                        if (Environment.AutoCommit)
                        {
                            NativeMethods.mdb_txn_commit(_writeHandle);
                        }
                        else
                        {
                            NativeMethods.mdb_txn_abort(_writeHandle);
                        }
                    }
                }
                else
                {
                    if (_state == TransactionState.Active)
                    {
                        Trace.TraceWarning("Finalizing active transaction. Will abort it.");
                        NativeMethods.mdb_txn_abort(_writeHandle);
                    }
                    else
                    {
                        Trace.TraceWarning("Finalizing finished write transaction. Dispose it explicitly.");
                    }
                }
                _writeHandle = IntPtr.Zero;
            }

            _environment = null;
            _state = TransactionState.Disposed;
            if (disposing)
            {
                TxPool.Free(this);
            }
        }

        public void Dispose()
        {
            GC.SuppressFinalize(this);
            Dispose(true);
        }

        ~Transaction()
        {
            Dispose(false);
        }

        #endregion Lifecycle

        /// <summary>
        /// Environment in which the transaction was opened.
        /// </summary>
        public Environment Environment => _environment;

        /// <summary>
        /// Transaction is read-only.
        /// </summary>
        public bool IsReadOnly => _readHandle != null;

        /// <summary>
        /// Current transaction state.
        /// </summary>
        internal TransactionState State => _state;

        /// <summary>
        /// Reset current transaction.
        /// </summary>
        public void Reset()
        {
            if (_state != TransactionState.Active)
            {
                throw new InvalidOperationException("Transaction state is not active for reset");
            }
            if (!IsReadOnly)
            {
                throw new InvalidOperationException("Cannot reset non-readonly transaction");
            }
            Dispose();
        }

        /// <summary>
        /// Commit all the operations of a transaction into the database.
        /// All cursors opened within the transaction will be closed by this call.
        /// The cursors and transaction handle will be freed and must not be used again after this call.
        /// </summary>
        public void Commit()
        {
            if (_state != TransactionState.Active)
            {
                throw new InvalidOperationException("Transaction state is not active for commit");
            }
            if (IsReadOnly)
            {
                throw new InvalidOperationException("Cannot commit readonly transaction");
            }

            NativeMethods.AssertExecute(NativeMethods.mdb_txn_commit(_writeHandle));
            _state = TransactionState.Commited;
        }

        /// <summary>
        /// Abandon all the operations of the transaction instead of saving them.
        /// All cursors opened within the transaction will be closed by this call.
        /// The cursors and transaction handle will be freed and must not be used again after this call.
        /// </summary>
        public void Abort()
        {
            if (_state != TransactionState.Active)
            {
                throw new InvalidOperationException("Transaction state is not active for abort");
            }
            if (IsReadOnly)
            {
                throw new InvalidOperationException("Cannot abort readonly transaction. Use Dispose() method instead.");
            }
            NativeMethods.mdb_txn_abort(_writeHandle);
            _state = TransactionState.Aborted;
        }
    }
}
