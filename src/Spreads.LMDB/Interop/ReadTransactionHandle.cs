// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Spreads.LMDB.Interop
{
    internal class ReadTransactionHandle : SafeHandle
    {
        internal ReadTransactionHandle() : base(IntPtr.Zero, ownsHandle: true)
        { }

        public override bool IsInvalid => handle == IntPtr.Zero;

        internal void SetNewHandle(IntPtr handle)
        {
            SetHandle(handle);
        }

        internal IntPtr Handle
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return handle; }
        }

        protected override bool ReleaseHandle()
        {
            NativeMethods.mdb_txn_abort(handle);
            return true;
        }
    }
}