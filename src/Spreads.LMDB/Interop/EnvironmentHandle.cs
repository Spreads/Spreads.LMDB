// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.


using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Spreads.LMDB.Interop
{
    internal class EnvironmentHandle : SafeHandle {
        private EnvironmentHandle()
            : base(IntPtr.Zero, ownsHandle: true) {
            }

        public override bool IsInvalid => handle == IntPtr.Zero;

        internal IntPtr Handle
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return handle; }
        }

        protected override bool ReleaseHandle() {
            NativeMethods.mdb_env_sync(handle, true);
            NativeMethods.mdb_env_close(handle);
            return true;
        }
    }
}