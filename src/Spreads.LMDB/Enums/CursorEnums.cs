// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.


using System;

namespace Spreads.LMDB
{
    /// <summary>
    /// Cursor operation types
    /// </summary>
    public enum CursorGetOption
    {
        /// <summary>
        /// Position at first key/data item
        /// </summary>
        First,

        /// <summary>
        /// Position at first data item of current key. Only for MDB_DUPSORT
        /// </summary>
        FirstDuplicate,

        /// <summary>
        /// Position at key/data pair. Only for MDB_DUPSORT
        /// </summary>
        GetBoth,

        /// <summary>
        /// position at key, nearest data. Only for MDB_DUPSORT
        /// </summary>
        GetBothRange,

        /// <summary>
        /// Return key/data at current cursor position
        /// </summary>
        GetCurrent,

        /// <summary>
        /// Return all the duplicate data items at the current cursor position. Only for MDB_DUPFIXED
        /// </summary>
        GetMultiple,

        /// <summary>
        /// Position at last key/data item
        /// </summary>
        Last,

        /// <summary>
        /// Position at last data item of current key. Only for MDB_DUPSORT
        /// </summary>
        LastDuplicate,

        /// <summary>
        /// Position at next data item
        /// </summary>
        Next,

        /// <summary>
        /// Position at next data item of current key. Only for MDB_DUPSORT
        /// </summary>
        NextDuplicate,

        /// <summary>
        /// Return all duplicate data items at the next cursor position. Only for MDB_DUPFIXED
        /// </summary>
        NextMultiple,

        /// <summary>
        /// Position at first data item of next key. Only for MDB_DUPSORT
        /// </summary>
        NextNoDuplicate,

        /// <summary>
        /// Position at previous data item
        /// </summary>
        Previous,

        /// <summary>
        /// Position at previous data item of current key. Only for MDB_DUPSORT
        /// </summary>
        PreviousDuplicate,

        /// <summary>
        /// Position at last data item of previous key. Only for MDB_DUPSORT
        /// </summary>
        PreviousNoDuplicate,

        /// <summary>
        /// Position at specified key
        /// </summary>
        Set,

        /// <summary>
        /// Position at specified key, return key + data
        /// </summary>
        SetKey,

        /// <summary>
        /// Position at first key greater than or equal to specified key.
        /// </summary>
        SetRange
    }



    /// <summary>
    /// Special options for cursor put operation.
    /// </summary>
    [Flags]
    public enum CursorPutOptions : uint {
        /// <summary>
        /// No special behavior.
        /// </summary>
        None = 0,

        /// <summary>
        /// Overwrite the current key/data pair
        /// </summary>
        Current = 0x40,

        /// <summary>
        /// Only for MDB_DUPSORT
        /// For put: don't write if the key and data pair already exist.
        /// For mdb_cursor_del: remove all duplicate data items.
        /// </summary>
        NoDuplicateData = 0x20,

        /// <summary>
        /// For put: Don't write if the key already exists.
        /// </summary>
        NoOverwrite = 0x10,

        /// <summary>
        /// For put: Just reserve space for data, don't copy it. Return a pointer to the reserved space.
        /// </summary>
        ReserveSpace = 0x10000,

        /// <summary>
        /// Data is being appended, don't split full pages.
        /// </summary>
        AppendData = 0x20000,

        /// <summary>
        /// Duplicate data is being appended, don't split full pages.
        /// </summary>
        AppendDuplicateData = 0x40000,

        /// <summary>
        /// MDB_MULTIPLE. Store multiple data items in one call. Only for MDB_DUPFIXED.
        /// </summary>
        MultipleData = 0x80000
    }


    /// <summary>
    /// Cursor delete operation options
    /// </summary>
    public enum CursorDeleteOption {
        /// <summary>
        /// No special behavior
        /// </summary>
        None = 0,

        /// <summary>
        /// Only for MDB_DUPSORT
        /// For put: don't write if the key and data pair already exist.
        /// For mdb_cursor_del: remove all duplicate data items.
        /// </summary>
        NoDuplicateData = 0x20
    }


}
