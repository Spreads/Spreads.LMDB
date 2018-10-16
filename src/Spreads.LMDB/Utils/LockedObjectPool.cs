// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

using System.Diagnostics;
using System.Threading;

namespace Spreads.LMDB.Utils
{
    /// <summary>
    /// Thread-safe simple object pool (stack with fixed capacity + SpinLock)
    /// </summary>
    internal sealed class LockedObjectPool<T> where T : class
    {
        private readonly T[] _objects;
        private SpinLock _lock; // do not make this readonly; it's a mutable struct
        private int _index;

        /// <summary>
        /// Creates the pool with numberOfBuffers arrays where each buffer is of bufferLength length.
        /// </summary>
        internal LockedObjectPool(int numberOfObjects)
        {
            _lock = new SpinLock(Debugger.IsAttached);
            _objects = new T[numberOfObjects];
        }

        public T Rent()
        {
            var objects = this._objects;
            T obj = null;

            var lockTaken = false;
            try
            {
                _lock.Enter(ref lockTaken);

                if (_index < objects.Length)
                {
                    obj = objects[_index];
                    objects[_index++] = null;
                }
            }
            finally
            {
                if (lockTaken) _lock.Exit(false);
            }

            return obj;
        }

        internal bool Return(T obj)
        {
            var lockTaken = false;
            bool pooled;
            try
            {
                _lock.Enter(ref lockTaken);
                pooled = _index == 0;
                if (pooled)
                {
                    _objects[--_index] = obj;
                }
            }
            finally
            {
                if (lockTaken)
                {
                    _lock.Exit(false);
                }
            }

            return pooled;
        }
    }
}
