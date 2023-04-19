// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

// ReSharper disable InconsistentNaming

namespace Spreads.LMDB.Interop
{
    [System.Security.SuppressUnmanagedCodeSecurity]
    internal static partial class NativeMethods
    {
        #region Constants

        /// <summary>
        /// Txn has too many dirty pages
        /// </summary>
        public const int MDB_TXN_FULL = -30788;

        /// <summary>
        /// Environment mapsize reached
        /// </summary>
        public const int MDB_MAP_FULL = -30792;

        /// <summary>
        /// File is not a valid MDB file.
        /// </summary>
        public const int MDB_INVALID = -30793;

        /// <summary>
        /// Environment version mismatch.
        /// </summary>
        public const int MDB_VERSION_MISMATCH = -30794;

        /// <summary>
        /// Update of meta page failed, probably I/O error
        /// </summary>
        public const int MDB_PANIC = -30795;

        /// <summary>
        /// Database contents grew beyond environment mapsize
        /// </summary>
        public const int MDB_MAP_RESIZED = -30785;

        /// <summary>
        /// Environment maxreaders reached
        /// </summary>
        public const int MDB_READERS_FULL = -30790;

        /// <summary>
        /// Environment maxdbs reached
        /// </summary>
        public const int MDB_DBS_FULL = -30791;

        /// <summary>
        /// key/data pair not found (EOF)
        /// </summary>
        public const int MDB_NOTFOUND = -30798;

        // Database Flags

        /// <summary>
        /// Keys are strings to be compared in reverse order, from the end of the strings to the beginning. By default, Keys are treated as strings and compared from beginning to end.
        /// </summary>
        public const int MDB_REVERSEKEY = 0x02;

        /// <summary>
        /// Duplicate keys may be used in the database. (Or, from another perspective, keys may have multiple data items, stored in sorted order.) By default keys must be unique and may have only a single data item.
        /// </summary>
        public const int MDB_DUPSORT = 0x04;

        /// <summary>
        /// Keys are binary integers in native byte order. Setting this option requires all keys to be the same size, typically sizeof(int) or sizeof(size_t).
        /// </summary>
        public const int MDB_INTEGERKEY = 0x08;

        /// <summary>
        /// This flag may only be used in combination with MDB_DUPSORT. This option tells the library that the data items for this database are all the same size, which allows further optimizations in storage and retrieval. When all data items are the same size, the MDB_GET_MULTIPLE and MDB_NEXT_MULTIPLE cursor operations may be used to retrieve multiple items at once.
        /// </summary>
        public const int MDB_DUPFIXED = 0x10;

        /// <summary>
        /// This option specifies that duplicate data items are also integers, and should be sorted as such.
        /// </summary>
        public const int MDB_INTEGERDUP = 0x20;

        /// <summary>
        /// This option specifies that duplicate data items should be compared as strings in reverse order.
        /// </summary>
        public const int MDB_REVERSEDUP = 0x40;

        /// <summary>
        /// Create the named database if it doesn't exist. This option is not allowed in a read-only transaction or a read-only environment.
        /// </summary>
        public const int MDB_CREATE = 0x40000;

        #endregion Constants

        #region Helpers

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int AssertExecute(int res, string methodName = null)
        {
            return AssertHelper(res, true, methodName);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int AssertRead(int res, string methodName = null)
        {
            return AssertHelper(res, res != MDB_NOTFOUND, methodName);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int AssertHelper(int res, bool shouldThrow, string methodName = null)
        {
            if (res != 0 && shouldThrow) { ThrowLMDBEx(res, methodName); }
            return res;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void ThrowLMDBEx(int res, string methodName = null)
        {
            throw new LMDBException(res, methodName);
        }

        public static IntPtr StringToHGlobalUTF8(string s, out int length)
        {
            if (s == null)
            {
                length = 0;
                return IntPtr.Zero;
            }

            var bytes = Encoding.UTF8.GetBytes(s);
            var ptr = Marshal.AllocHGlobal(bytes.Length + 1);
            Marshal.Copy(bytes, 0, ptr, bytes.Length);
            Marshal.WriteByte(ptr, bytes.Length, 0);
            length = bytes.Length;

            return ptr;
        }

        public static IntPtr StringToHGlobalUTF8(string s)
        {
            int temp;
            return StringToHGlobalUTF8(s, out temp);
        }

        #endregion Helpers
    }
}
