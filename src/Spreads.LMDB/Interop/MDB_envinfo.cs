// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

using System.Runtime.InteropServices;
using size_t = System.IntPtr;

// ReSharper disable InconsistentNaming

namespace Spreads.LMDB.Interop
{
    [StructLayout(LayoutKind.Sequential, Pack = 4)]
    public readonly unsafe struct MDB_envinfo
    {
        /// <summary>
        /// Address of map, if fixed
        /// </summary>
        public readonly void* me_mapaddr;

        /// <summary>
        /// Size of the data memory map
        /// </summary>
        public readonly size_t me_mapsize;

        /// <summary>
        /// ID of the last used page
        /// </summary>
        public readonly size_t me_last_pgno;

        /// <summary>
        /// ID of the last committed transaction
        /// </summary>
        public readonly size_t me_last_txnid;

        /// <summary>
        /// max reader slots in the environment
        /// </summary>
        public readonly uint me_maxreaders;

        /// <summary>
        /// max reader slots used in the environment
        /// </summary>
        public readonly uint me_numreaders;
    }
}
