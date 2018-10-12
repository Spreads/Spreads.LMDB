// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

using Spreads.Buffers;
using System;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Spreads.Utils.Bootstrap;

// ReSharper disable UnusedMember.Global

namespace Spreads.LMDB.Interop
{
#pragma warning disable IDE1006 // Naming Styles

    [System.Security.SuppressUnmanagedCodeSecurity]
    internal static unsafe partial class NativeMethods
    {
        // NB unmanaged calli is no faster than DllImport, need other reasons to use it.
        // private static readonly bool UseCalli = IntPtr.Size == 8; // LMDB won't work on x86, so this means true

        static NativeMethods()
        {
            // Ensure Bootstrapper is initialized and native libraries are loaded
            Bootstrapper.Instance.Bootstrap<LMDBEnvironment>(
                DbLibraryName,
                null,
                () => { Debug.WriteLine("Native pre-copy"); },
                (lib) =>
                {
                    //mdb_get_ptr = lib.GetFunctionPtr("mdb_get");
                    //mdb_put_ptr = lib.GetFunctionPtr("mdb_put");

                    Debug.WriteLine("Native post-copy");
                },
                () => { Debug.WriteLine("Native dispose"); });
        }

        public const string DbLibraryName = "libspreads_lmdb";

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int mdb_env_create(out EnvironmentHandle env);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern void mdb_env_close(IntPtr env);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern void mdb_env_close(EnvironmentHandle env);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int mdb_env_open(IntPtr env, string path, DbEnvironmentFlags flags, UnixAccessMode mode);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int mdb_env_open(EnvironmentHandle env, string path, DbEnvironmentFlags flags, UnixAccessMode mode);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int mdb_env_open(IntPtr env, string path, DbEnvironmentFlags flags, int mode);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int mdb_env_open(EnvironmentHandle env, string path, DbEnvironmentFlags flags, int mode);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int mdb_env_set_mapsize(IntPtr env, IntPtr size);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int mdb_env_set_mapsize(EnvironmentHandle env, IntPtr size);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int mdb_env_get_maxreaders(IntPtr env, out uint readers);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int mdb_env_get_maxreaders(EnvironmentHandle env, out uint readers);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int mdb_env_get_maxkeysize(IntPtr env);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int mdb_env_get_maxkeysize(EnvironmentHandle env);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int mdb_env_set_maxreaders(IntPtr env, uint readers);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int mdb_env_set_maxreaders(EnvironmentHandle env, uint readers);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int mdb_env_set_maxdbs(IntPtr env, uint dbs);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int mdb_env_set_maxdbs(EnvironmentHandle env, uint dbs);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int mdb_dbi_open(IntPtr txn, string name, DbFlags flags, out uint db);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern void mdb_dbi_close(IntPtr env, uint dbi);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern void mdb_dbi_close(EnvironmentHandle env, uint dbi);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int mdb_drop(IntPtr txn, uint dbi, bool del);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int mdb_txn_begin(IntPtr env, IntPtr parent, TransactionBeginFlags flags, out IntPtr txn);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int mdb_txn_commit(IntPtr txn);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern void mdb_txn_abort(IntPtr txn);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern void mdb_txn_reset(IntPtr txn);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int mdb_txn_renew(IntPtr txn);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern IntPtr mdb_version(out IntPtr major, out IntPtr minor, out IntPtr patch);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern IntPtr mdb_strerror(int err);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int mdb_stat(IntPtr txn, uint dbi, out MDB_stat stat);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int mdb_env_copy(IntPtr env, string path);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int mdb_env_copy2(IntPtr env, string path, EnvironmentCopyFlags copyFlags);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int mdb_env_copy2(EnvironmentHandle env, string path, EnvironmentCopyFlags copyFlags);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int mdb_env_info(IntPtr env, out MDB_envinfo stat);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int mdb_env_info(EnvironmentHandle env, out MDB_envinfo stat);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int mdb_env_stat(IntPtr env, out MDB_stat stat);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int mdb_env_stat(EnvironmentHandle env, out MDB_stat stat);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int mdb_env_sync(IntPtr env, bool force);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int mdb_reader_check(EnvironmentHandle env, out int dead);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int mdb_env_sync(EnvironmentHandle env, bool force);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int mdb_get(void* txn, uint dbi, void* key, void* data);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int mdb_put(IntPtr txn, uint dbi, ref DirectBuffer key, ref DirectBuffer data, TransactionPutOptions flags);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int mdb_del(IntPtr txn, uint dbi, in DirectBuffer key, in DirectBuffer data);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int mdb_del(IntPtr txn, uint dbi, in DirectBuffer key, IntPtr data);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int mdb_cursor_open(IntPtr txn, uint dbi, out IntPtr cursor);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern void mdb_cursor_close(IntPtr cursor);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int mdb_cursor_renew(IntPtr txn, IntPtr cursor);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int mdb_cursor_get(IntPtr cursor, ref DirectBuffer key, ref DirectBuffer data, CursorGetOption op);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int mdb_cursor_put(IntPtr cursor, ref DirectBuffer key, ref DirectBuffer data, CursorPutOptions flags);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int mdb_cursor_put(IntPtr cursor, ref DirectBuffer key, DirectBuffer[] data, CursorPutOptions flags);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int mdb_cursor_del(IntPtr cursor, CursorDeleteOption flags);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int mdb_cursor_count(IntPtr cursor, out UIntPtr countp);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int mdb_set_compare(IntPtr txn, uint dbi, [MarshalAs(UnmanagedType.FunctionPtr)]CompareFunction cmp);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int mdb_set_dupsort(IntPtr txn, uint dbi, [MarshalAs(UnmanagedType.FunctionPtr)]CompareFunction cmp);

        // Spreads extensoins to LMDB

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int sdb_cursor_get_lt(IntPtr cursor, ref DirectBuffer key, out DirectBuffer data);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int sdb_cursor_get_le(IntPtr cursor, ref DirectBuffer key, out DirectBuffer data);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int sdb_cursor_get_eq(IntPtr cursor, ref DirectBuffer key, out DirectBuffer data);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int sdb_cursor_get_ge(IntPtr cursor, ref DirectBuffer key, out DirectBuffer data);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int sdb_cursor_get_gt(IntPtr cursor, ref DirectBuffer key, out DirectBuffer data);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int sdb_cursor_get_lt_dup(IntPtr cursor, ref DirectBuffer key, ref DirectBuffer data);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int sdb_cursor_get_le_dup(IntPtr cursor, ref DirectBuffer key, ref DirectBuffer data);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int sdb_cursor_get_eq_dup(IntPtr cursor, ref DirectBuffer key, ref DirectBuffer data);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int sdb_cursor_get_ge_dup(IntPtr cursor, ref DirectBuffer key, ref DirectBuffer data);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int sdb_cursor_get_gt_dup(IntPtr cursor, ref DirectBuffer key, ref DirectBuffer data);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int sdb_set_dupsort_as_uint128(IntPtr txn, uint dbi);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int sdb_set_dupsort_as_uint96(IntPtr txn, uint dbi);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int sdb_set_dupsort_as_uint80(IntPtr txn, uint dbi);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int sdb_set_dupsort_as_uint64(IntPtr txn, uint dbi);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int sdb_set_dupsort_as_uint64x64(IntPtr txn, uint dbi);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int sdb_set_dupsort_as_uint48(IntPtr txn, uint dbi);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int sdb_set_dupsort_as_uint32(IntPtr txn, uint dbi);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int sdb_set_dupsort_as_uint16(IntPtr txn, uint dbi);

        [DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        public static extern int sdb_put(IntPtr env, uint dbi, ref DirectBuffer key, ref DirectBuffer data, TransactionPutOptions flags);

        // TODO these below have weird native implementation with nindsight (txn/cursor must be null or reset).
        // Test showed that coalescing PInvokes gives little perf gain, so just probably remove these and native stuff.

        //[DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        //public static extern int sdb_find_lt_dup(IntPtr env, uint dbi, ref IntPtr txn, ref IntPtr cursor, ref DirectBuffer key, ref DirectBuffer data);

        //[DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        //public static extern int sdb_find_le_dup(IntPtr env, uint dbi, ref IntPtr txn, ref IntPtr cursor, ref DirectBuffer key, ref DirectBuffer data);

        //[DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        //public static extern int sdb_find_eq_dup(IntPtr env, uint dbi, ref IntPtr txn, ref IntPtr cursor, ref DirectBuffer key, ref DirectBuffer data);

        //[DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        //public static extern int sdb_find_ge_dup(IntPtr env, uint dbi, ref IntPtr txn, ref IntPtr cursor, ref DirectBuffer key, ref DirectBuffer data);

        //[DllImport(DbLibraryName, CallingConvention = CallingConvention.Cdecl)]
        //public static extern int sdb_find_gt_dup(IntPtr env, uint dbi, ref IntPtr txn, ref IntPtr cursor, ref DirectBuffer key, ref DirectBuffer data);

    }

#pragma warning restore IDE1006 // Naming Styles
}