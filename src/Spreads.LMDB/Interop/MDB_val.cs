// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using size_t = System.IntPtr;

// ReSharper disable InconsistentNaming

namespace Spreads.LMDB.Interop
{
    [StructLayout(LayoutKind.Sequential, Pack = 4)]
    public readonly unsafe ref struct MDB_val
    {
        public readonly size_t mv_size;
        public readonly void* mv_data;

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

    /// <summary>
    /// Non-ref MDB_val only to support mdb_cursor_put with multiple values.
    /// </summary>
    [StructLayout(LayoutKind.Sequential, Pack = 4)]
    public readonly unsafe struct MDB_val2
    {
        public readonly size_t mv_size;
        public readonly void* mv_data;

        public MDB_val2(Span<byte> span)
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
}
