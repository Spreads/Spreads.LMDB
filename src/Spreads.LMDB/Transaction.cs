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
    // NB: wrappers are only exposed from inside using(...){...} therefore they are not disposable, we always dispose inner TransactionImpl

    public readonly struct Transaction
    {
        internal readonly TransactionImpl _impl;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Transaction(TransactionImpl txn)
        {
            _impl = txn;
        }

        /// <summary>
        /// Commit all the operations of a transaction into the database.
        /// All cursors opened within the transaction will be closed by this call.
        /// The cursors and transaction handle will be freed and must not be used again after this call.
        /// </summary>
        public void Commit()
        {
            _impl.Commit();
        }

        /// <summary>
        /// Abandon all the operations of the transaction instead of saving them.
        /// All cursors opened within the transaction will be closed by this call.
        /// The cursors and transaction handle will be freed and must not be used again after this call.
        /// </summary>
        public void Abort()
        {
            _impl.Abort();
        }
    }

    public readonly struct ReadOnlyTransaction
    {
        internal readonly TransactionImpl _impl;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ReadOnlyTransaction(TransactionImpl txn)
        {
            if (!txn.IsReadOnly)
            {
                TransactionImpl.ThrowlTransactionIsReadOnly();
            }
            _impl = txn;
        }
    }

    internal class TransactionImpl : IDisposable
    {
        private Environment _environment;

        // on dispose must close write and reset read

        internal IntPtr _writeHandle;

        internal ReadTransactionHandle _readHandle;

        private TransactionState _state;

        #region Lifecycle

        private static readonly ObjectPool<TransactionImpl> TxPool =
            new ObjectPool<TransactionImpl>(() => new TransactionImpl(), System.Environment.ProcessorCount * 16);

        private static readonly ObjectPool<ReadTransactionHandle> ReadHandlePool =
            new ObjectPool<ReadTransactionHandle>(() => new ReadTransactionHandle(), System.Environment.ProcessorCount * 16);

        private TransactionImpl()
        {
            _state = TransactionState.Disposed;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static TransactionImpl Create(Environment environment, TransactionBeginFlags beginFlags)
        {
            environment.EnsureOpened();

            var tx = TxPool.Allocate();

            if (tx._state != TransactionState.Disposed)
            {
                ThrowShoudBeDisposed();
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
                    NativeMethods.AssertExecute(NativeMethods.mdb_txn_renew(rh.Handle));
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
                ThrowlAlreadyDisposed();
            }
            var isReadOnly = IsReadOnly;
            if (isReadOnly)
            {
                if (disposing)
                {
                    var rh = _readHandle;
                    _readHandle = null;
                    NativeMethods.mdb_txn_reset(rh.Handle);
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
                            Trace.TraceWarning("Transaction was not either commited or aborted. Aborting it. Set Environment.AutoCommit to true to commit automatically on transaction end.");
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

        ~TransactionImpl()
        {
            Dispose(false);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void ThrowShoudBeDisposed()
        {
            throw new InvalidOperationException("Pooled tx must be in disposed state");
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void ThrowlAlreadyDisposed()
        {
            throw new InvalidOperationException("Pooled tx must be in disposed state");
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        internal static void ThrowlTransactionIsReadOnly(string message = null)
        {
            message = message is null ? String.Empty : " " + message;
            throw new InvalidOperationException("Transaction is not readonly." + message);
        }

        #endregion Lifecycle

        /// <summary>
        /// Environment in which the transaction was opened.
        /// </summary>
        public Environment Environment => _environment;

        /// <summary>
        /// Transaction is read-only.
        /// </summary>
        public bool IsReadOnly
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _readHandle != null; }
        }

        /// <summary>
        /// Current transaction state.
        /// </summary>
        internal TransactionState State => _state;

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
