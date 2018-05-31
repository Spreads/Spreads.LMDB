// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.


using System;

namespace Spreads.LMDB
{
    /// <summary>
    /// Unix file access privilegies
    /// </summary>
    [Flags]
    public enum UnixAccessMode : uint
    {
        /// <summary>
        /// S_IRUSR
        /// </summary>
        OwnerRead = 0x0100,

        /// <summary>
        /// S_IWUSR
        /// </summary>
        OwnerWrite = 0x0080, 

        /// <summary>
        /// S_IXUSR
        /// </summary>
        OwnerExec = 0x0040, 

        /// <summary>
        /// S_IRGRP
        /// </summary>
        GroupRead = 0x0020,

        /// <summary>
        /// S_IWGRP
        /// </summary>
        GroupWrite = 0x0010,

        /// <summary>
        /// S_IXGRP
        /// </summary>
        GroupExec = 0x0008, 

        /// <summary>
        /// S_IROTH
        /// </summary>
        OtherRead = 0x0004, 

        /// <summary>
        /// S_IWOTH
        /// </summary>
        OtherWrite = 0x0002, 

        /// <summary>
        /// S_IXOTH
        /// </summary>
        OtherExec = 0x0001,

        /// <summary>
        /// Owner, Group, Other Read/Write
        /// </summary>
        Default = OwnerRead | OwnerWrite | GroupRead | GroupWrite | OtherRead | OtherWrite
    }
}
