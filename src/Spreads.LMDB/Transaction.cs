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
    public readonly struct Transaction : IDisposable
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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Commit()
        {
            _impl.Commit();
        }

        /// <summary>
        /// Abandon all the operations of the transaction instead of saving them.
        /// All cursors opened within the transaction will be closed by this call.
        /// The cursors and transaction handle will be freed and must not be used again after this call.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Abort()
        {
            _impl.Abort();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose()
        {
            _impl.Dispose();
        }
    }

    public readonly struct ReadOnlyTransaction : IDisposable
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose()
        {
            _impl.Dispose();
        }

        // TODO Manual Reset/Renew
    }

    internal class TransactionImpl : IDisposable
    {
        private LMDBEnvironment _lmdbEnvironment;

        // on dispose must close write and reset read

        internal IntPtr _writeHandle;

        internal ReadTransactionHandle _readHandle;

        private TransactionState _state;

        #region Lifecycle

        private static readonly ObjectPool<TransactionImpl> TxPool =
            new ObjectPool<TransactionImpl>(() => new TransactionImpl(), Environment.ProcessorCount * 16);

        private TransactionImpl()
        {
            _state = TransactionState.Disposed;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static TransactionImpl Create(LMDBEnvironment lmdbEnvironment, TransactionBeginFlags beginFlags)
        {
            lmdbEnvironment.EnsureOpened();

            var tx = TxPool.Allocate();

            if (tx._state != TransactionState.Disposed)
            {
                ThrowShoudBeDisposed();
            }

            tx._lmdbEnvironment = lmdbEnvironment;
            // ReSharper disable once BitwiseOperatorOnEnumWithoutFlags
            var isReadOnly = (beginFlags & TransactionBeginFlags.ReadOnly) == TransactionBeginFlags.ReadOnly;
            if (isReadOnly)
            {
                if (!tx._lmdbEnvironment.ReadHandlePool.TryDequeue(out var rh))
                {
                    rh = new ReadTransactionHandle();
                }
                if (rh.IsInvalid)
                {
                    // create new
                    NativeMethods.AssertExecute(
                        NativeMethods.mdb_txn_begin(lmdbEnvironment._handle.Handle, IntPtr.Zero, beginFlags, out IntPtr handle));
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
                    NativeMethods.mdb_txn_begin(lmdbEnvironment._handle.Handle, IntPtr.Zero, beginFlags, out IntPtr handle));
                tx._writeHandle = handle;
            }
            tx._state = TransactionState.Active;
            return tx;
        }

        protected void Dispose(bool disposing)
        {
            var isReadOnly = IsReadOnly;
            if (_state == TransactionState.Disposed)
            {
                if (disposing)
                {
                    ThrowlAlreadyDisposed();
                }
                else
                {
                    // TxPool.Free could have dropped, currently we cannot detect this
                    // See comment in Dispose()
                    if (isReadOnly)
                    {
                        _readHandle.Dispose();
                    }
                    return;
                }
            }

            if (isReadOnly)
            {
                if (disposing)
                {
                    var rh = _readHandle;
                    _readHandle = null;
                    if (_lmdbEnvironment._disableReadTxnAutoreset || _lmdbEnvironment.ReadHandlePool.Count >= _lmdbEnvironment.MaxReaders - Environment.ProcessorCount)
                    {
                        rh.Dispose();
                    }
                    else
                    {                        
                        NativeMethods.mdb_txn_reset(rh.Handle);
                        _lmdbEnvironment.ReadHandlePool.Enqueue(rh);
                    }
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
                        if (LmdbEnvironment.AutoCommit)
                        {
                            NativeMethods.mdb_txn_commit(_writeHandle);
                        }
                        else
                        {
                            NativeMethods.mdb_txn_abort(_writeHandle);
                            // This should not be catchable
                            Environment.FailFast("Transaction was not either commited or aborted. Aborting it. Set Environment.AutoCommit to true to commit automatically on transaction end.");
                        }
                    }
                }
                else
                {
                    if (_state == TransactionState.Active)
                    {
                        NativeMethods.mdb_txn_abort(_writeHandle);
                        Environment.FailFast("Finalizing active transaction. Will abort it. Set Environment.AutoCommit to true to commit automatically on transaction end.");
                    }
                    else
                    {
                        Trace.TraceWarning("Finalizing finished write transaction. Dispose it explicitly.");
                    }
                }
                _writeHandle = IntPtr.Zero;
            }

            _lmdbEnvironment = null;
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
        public LMDBEnvironment LmdbEnvironment => _lmdbEnvironment;

        /// <summary>
        /// Transaction is read-only.
        /// </summary>
        public bool IsReadOnly
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _readHandle != null;
        }

        /// <summary>
        /// Current transaction state.
        /// </summary>
        internal TransactionState State
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _state;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Commit()
        {
            if (_state != TransactionState.Active)
            {
                ThrowTxNotActiveOnCommit();
            }

            if (IsReadOnly)
            {
                ThrowTxReadOnlyOnCommit();
            }

            NativeMethods.AssertExecute(NativeMethods.mdb_txn_commit(_writeHandle));
            _state = TransactionState.Commited;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void ThrowTxNotActiveOnCommit()
        {
            throw new InvalidOperationException("Transaction state is not active for commit");
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void ThrowTxReadOnlyOnCommit()
        {
            throw new InvalidOperationException("Cannot commit readonly transaction");
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
