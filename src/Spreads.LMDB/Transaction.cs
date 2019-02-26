// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

using Spreads.Collections.Concurrent;
using Spreads.LMDB.Interop;
using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.ConstrainedExecution;

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
#if DEBUG
            if (_impl._cursorCount > 0)
            {
                Environment.FailFast("Transaction has outstanding cursors that must be closed before Commit.");
            }
#endif
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
#if DEBUG
            if (_impl._cursorCount > 0)
            {
                Environment.FailFast("Transaction has outstanding cursors that must be closed before Abort.");
            }
#endif
            _impl.Abort();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose()
        {
#if DEBUG
            if (_impl._cursorCount > 0)
            {
                Environment.FailFast("Transaction has outstanding cursors that must be closed before Dispose.");
            }
#endif
            _impl.Dispose();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static implicit operator ReadOnlyTransaction(Transaction value)
        {
            return new ReadOnlyTransaction(value._impl);
        }
    }

    public readonly struct ReadOnlyTransaction : IDisposable
    {
        internal readonly TransactionImpl _impl;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ReadOnlyTransaction(TransactionImpl txn)
        {
            _impl = txn;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose()
        {
#if DEBUG
            if (_impl._cursorCount > 0)
            {
                Environment.FailFast("Transaction has outstanding cursors that must be closed before Dispose.");
            }
#endif
            _impl.Dispose();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Reset()
        {
#if DEBUG
            if (_impl._cursorCount > 0)
            {
                Environment.FailFast("Transaction has outstanding cursors that must be closed before Reset.");
            }
#endif
            _impl.Reset();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Renew()
        {
            _impl.Renew();
        }
    }

    internal class TransactionImpl : CriticalFinalizerObject, IDisposable
    {
        private IntPtr _handle;

        private LMDBEnvironment _lmdbEnvironment;

        private bool _isReadOnly;

        private TransactionState _state;

        #region Lifecycle

#if DEBUG
        public string StackTrace;
        internal int _cursorCount;
#endif

        internal static readonly ObjectPool<TransactionImpl> TxPool =
            new ObjectPool<TransactionImpl>(() => new TransactionImpl(), Environment.ProcessorCount * 16);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private TransactionImpl()
        {
            _state = TransactionState.Disposed;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static TransactionImpl Create(LMDBEnvironment lmdbEnvironment, TransactionBeginFlags beginFlags)
        {
            lmdbEnvironment.EnsureOpened();

            TransactionImpl tx;

            // ReSharper disable once BitwiseOperatorOnEnumWithoutFlags
            var isReadOnly = (beginFlags & TransactionBeginFlags.ReadOnly) == TransactionBeginFlags.ReadOnly;

            if (isReadOnly)
            {
                if ((tx = lmdbEnvironment.ReadTxnPool.Rent()) == null)
                {
                    tx = TxPool.Allocate();
                    tx._isReadOnly = true;
                    if (tx._state != TransactionState.Disposed)
                    {
                        ThrowShoudBeDisposed();
                    }
                }
                else
                {
                    Debug.Assert(tx._isReadOnly);
                    Debug.Assert(tx._state == TransactionState.Reset);
                }

                if (tx.IsInvalid)
                {
                    // create new handle
                    NativeMethods.AssertExecute(NativeMethods.mdb_txn_begin(
                            lmdbEnvironment._handle.Handle,
                            IntPtr.Zero, beginFlags, out var handle));
                    tx.SetNewHandle(handle);
                }
                else
                {
                    tx.Renew();
                }
            }
            else
            {
                tx = TxPool.Allocate();
                tx._isReadOnly = false;
                NativeMethods.AssertExecute(NativeMethods.mdb_txn_begin(
                    lmdbEnvironment._handle.Handle,
                    IntPtr.Zero, beginFlags, out IntPtr handle));
                tx._handle = handle;
                if (tx._state != TransactionState.Disposed)
                {
                    ThrowShoudBeDisposed();
                }
            }

            tx._lmdbEnvironment = lmdbEnvironment;
            tx._state = TransactionState.Active;

#if DEBUG
            tx.StackTrace = Environment.StackTrace;
#endif
            return tx;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void Dispose(bool disposing)
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
                    return;
                }
            }

            _state = TransactionState.Disposed;

            if (isReadOnly)
            {
                if (disposing)
                {
                    NativeMethods.mdb_txn_reset(_handle);
                    _state = TransactionState.Reset;

                    var pooled = _lmdbEnvironment.ReadTxnPool.Return(this);
                    if (pooled)
                    {
                        Debug.Assert(_state == TransactionState.Reset); // set above
                        return;
                    }
                }
                else
                {
#if DEBUG
                    Trace.TraceWarning("Finalizing read transaction. Dispose it explicitly. " + StackTrace);
#endif
                }

                _state = TransactionState.Disposed;
                NativeMethods.mdb_txn_abort(_handle);
                _handle = IntPtr.Zero;
            }
            else
            {
                if (disposing)
                {
                    if (_state == TransactionState.Active)
                    {
                        NativeMethods.mdb_txn_abort(_handle);

                        if (!LmdbEnvironment.AutoAbort)
                        {
                            ThrowDisposingActiveTransaction();
                        }
                    }
                }
                else
                {
                    if (_state == TransactionState.Active)
                    {
                        NativeMethods.mdb_txn_abort(_handle);
                        FailFinalizingActiveTransaction();
                    }
                    else
                    {
#if DEBUG
                        Trace.TraceWarning("Finalizing finished write transaction. Dispose it explicitly. " + StackTrace);
#endif
                    }
                }
                _handle = IntPtr.Zero;
            }

            _lmdbEnvironment = null;

#if DEBUG
            StackTrace = null;
#endif

            if (disposing)
            {
                TxPool.Free(this);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        ~TransactionImpl()
        {
            Dispose(false);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void FailFinalizingActiveTransaction()
        {
            Environment.FailFast(
                "Finalizing active transaction. Will abort it. Set Environment.AutoCommit to true to commit automatically on transaction end.");
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void ThrowDisposingActiveTransaction()
        {
            throw new InvalidOperationException("Transaction was not either commited or aborted. Aborting it. Set Environment.AutoAbort to true to avoid this exception.");
        }

        internal IntPtr Handle
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _handle;
        }

        private bool IsInvalid
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _handle == default;
        }

        internal void SetNewHandle(IntPtr newHandle)
        {
            _handle = newHandle;
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
        internal static void ThrowTransactionIsNotReadOnly()
        {
            throw new InvalidOperationException("Transaction is not readonly.");
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
            get => _isReadOnly;
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

            NativeMethods.AssertExecute(NativeMethods.mdb_txn_commit(_handle));
            _state = TransactionState.Commited;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Abort()
        {
            if (_state != TransactionState.Active)
            {
                ThrowTxNotActiveOnAbort();
            }
            NativeMethods.mdb_txn_abort(_handle);
            _handle = IntPtr.Zero;
            _state = TransactionState.Aborted;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Reset()
        {
            if (_state != TransactionState.Active)
            {
                ThrowTxNotActiveOnReset();
            }
            if (!_isReadOnly)
            {
                ThrowTxNotReadonlyOnReset();
            }
            NativeMethods.mdb_txn_reset(_handle);
            _state = TransactionState.Reset;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Renew()
        {
            if (_state != TransactionState.Reset)
            {
                ThrowTxNotResetOnRenew();
            }
            if (!_isReadOnly)
            {
                ThrowTxNotReadonlyOnRenew();
            }
            NativeMethods.AssertExecute(NativeMethods.mdb_txn_renew(_handle));
            _state = TransactionState.Active;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void ThrowTxNotActiveOnCommit()
        {
            throw new InvalidOperationException("Transaction state is not active on commit");
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void ThrowTxNotActiveOnAbort()
        {
            throw new InvalidOperationException("Transaction state is not active on abort");
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void ThrowTxNotActiveOnReset()
        {
            throw new InvalidOperationException("Transaction state is not active on reset");
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void ThrowTxNotReadonlyOnReset()
        {
            throw new InvalidOperationException("Transaction is not readonly on reset");
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void ThrowTxNotReadonlyOnRenew()
        {
            throw new InvalidOperationException("Transaction is not readonly on renew");
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void ThrowTxNotResetOnRenew()
        {
            throw new InvalidOperationException("Transaction state is not reset on renew");
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void ThrowTxReadOnlyOnCommit()
        {
            throw new InvalidOperationException("Cannot commit readonly transaction");
        }
    }
}
