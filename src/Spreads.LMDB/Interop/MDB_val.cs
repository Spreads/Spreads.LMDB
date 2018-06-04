// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Spreads.Serialization;
using size_t = System.IntPtr;

// ReSharper disable InconsistentNaming

namespace Spreads.LMDB.Interop
{
    // [Obsolete("TODO this should be internal and all public methods accept/return Span<byte>")]
    [StructLayout(LayoutKind.Sequential, Pack = 4)]
    public readonly unsafe ref struct MDB_val
    {
        public readonly size_t mv_size;
        public readonly void* mv_data;

        public MDB_val(size_t size, IntPtr data)
        {
            mv_size = size;
            mv_data = (void*)data;
        }

        public MDB_val(Span<byte> span)
        {
            mv_size = (size_t)span.Length;
            mv_data = Unsafe.AsPointer(ref MemoryMarshal.GetReference(span));
        }

        public ReadOnlySpan<byte> Span
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return new ReadOnlySpan<byte>(mv_data, checked((int)mv_size)); }
        }
    }


    public static class MDBValExtensions
    {
        /// <summary>
        /// Memory musy be already pinned.
        /// </summary>
        // [Obsolete("Temp to figute out API")]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe MDB_val AsMDBValUnsafe<T>(this Memory<T> memory)
        {
            var size = TypeHelper<T>.Size;
            if (size <= 0)
            {
                ThrowNotBlittable();
            }

            var byteSize = size * memory.Length;

            var pointer = System.Runtime.CompilerServices.Unsafe.AsPointer(ref MemoryMarshal.GetReference(memory.Span));

            return new MDB_val((size_t)byteSize, (IntPtr)pointer);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void ThrowNotBlittable()
        {
            throw new InvalidOperationException("Type T is not blittable");
        }
    }

    /// <summary>
    /// Non-ref MDB_val only to support mdb_cursor_put with multiple values.
    /// </summary>
    [StructLayout(LayoutKind.Sequential, Pack = 4)]
    internal readonly unsafe struct MDB_val2
    {
        public readonly size_t mv_size;
        public readonly void* mv_data;

        public MDB_val2(size_t size, IntPtr data)
        {
            mv_size = size;
            mv_data = (void*)data;
        }

        public MDB_val2(MDB_val mdbval)
        {
            mv_size = mdbval.mv_size;
            mv_data = mdbval.mv_data;
        }

        public MDB_val AsMDBVal()
        {
            return new MDB_val(mv_size, (IntPtr)mv_data);
        }
    }
}
