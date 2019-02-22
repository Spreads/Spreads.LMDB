// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

using Spreads.LMDB.Interop;
using System;
using System.Diagnostics;
using System.Runtime.InteropServices;

namespace Spreads.LMDB
{
    /// <summary>
    /// An exception caused by LMDB operations.
    /// </summary>
    public class LMDBException : Exception
    {
        /// <summary>
        /// LMDB error code.
        /// </summary>
        public int Code { get; }

        private static string GetMessageByCode(int code)
        {
            var ptr = NativeMethods.mdb_strerror(code);
            string message = Marshal.PtrToStringAnsi(ptr);
            if (LMDBEnvironment.TraceErrors)
            {
                Trace.TraceError(message);
            }

            return message;
        }

        internal LMDBException(int code) : base(GetMessageByCode(code))
        {
            Code = code;
        }
    }
}
