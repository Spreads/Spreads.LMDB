// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

using System.Runtime.InteropServices;
using Spreads.LMDB.Interop;

namespace Spreads.LMDB
{
    /// <summary>
    /// Represents lmdb version information.
    /// </summary>
    public static class LMDBVersionInfo
    {
        static LMDBVersionInfo()
        {
            var version = NativeMethods.mdb_version(out var major, out var minor, out var patch);
            Version = Marshal.PtrToStringAnsi(version);
            Major = major.ToInt32();
            Minor = minor.ToInt32();
            Patch = patch.ToInt32();
        }

        /// <summary>
        /// Major version number.
        /// </summary>
        public static int Major { get; }

        /// <summary>
        /// Minor version number.
        /// </summary>
        public static int Minor { get; }

        /// <summary>
        /// Patch version number.
        /// </summary>
        public static int Patch { get; }

        /// <summary>
        /// Version string.
        /// </summary>
        public static string Version { get; }
    }
}
