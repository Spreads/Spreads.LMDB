// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.


using Spreads.LMDB;
using Spreads.LMDB.Interop;

namespace Spreads.LMDB
{
    public class DatabaseConfig
    {
		public DbFlags OpenFlags { get; }
		public CompareFunction CompareFunction { get; }
	    public CompareFunction DupSortFunction { get; }

	    public DatabaseConfig(DbFlags flags,
            CompareFunction compareFunc = null, 
			CompareFunction dupSortFunc = null)
        {
			OpenFlags = flags;
            CompareFunction = compareFunc;
            DupSortFunction = dupSortFunc;
        }

        
    }
}
