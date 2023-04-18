// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

using System.Runtime.InteropServices;
using size_t = System.IntPtr;

// ReSharper disable InconsistentNaming

namespace Spreads.LMDB.Interop
{
    [StructLayout(LayoutKind.Sequential)]
    public readonly struct MDB_stat
    {
        /// <summary>
        /// Size of a database page. This is currently the same for all databases.
        /// </summary>
        public readonly uint ms_psize;

        /// <summary>
        /// Depth (height) of the B-tree
        /// </summary>
        public readonly uint ms_depth;

        /// <summary>
        /// Number of internal (non-leaf) pages
        /// </summary>
        public readonly size_t ms_branch_pages;

        /// <summary>
        /// Number of leaf pages
        /// </summary>
        public readonly size_t ms_leaf_pages;

        /// <summary>
        /// Number of overflow pages
        /// </summary>
        public readonly size_t ms_overflow_pages;

        /// <summary>
        /// Number of data items
        /// </summary>
        public readonly size_t ms_entries;

        public override string ToString() => $"Stat: ms_psize={ms_psize}, ms_depth={ms_depth}, ms_branch_pages={ms_branch_pages}, ms_leaf_pages={ms_leaf_pages}, ms_overflow_pages={ms_overflow_pages}, ms_entries={ms_entries}";
    }
}
