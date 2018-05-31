// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

using System.Runtime.InteropServices;

namespace Spreads.LMDB.Interop
{
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    public delegate int CompareFunction(ref MDB_val left, ref MDB_val right);
}
