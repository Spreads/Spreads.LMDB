// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

namespace Spreads.LMDB
{
    /// <summary>
    /// Basic setup for the library.
    /// </summary>
    public static class Config
    {
        /// <summary>
        /// Basic environment configuration
        /// </summary>
        public static class DbEnvironment
        {
            /// <summary>
            /// Default MapSize for new environments
            /// </summary>
            public const long LibDefaultMapSize = 10 * 1024 * 1024;

            /// <summary>
            /// Default MaxReaders for new environments
            /// </summary>
            public const int LibDefaultMaxReaders = 126;

            /// <summary>
            /// Default MaxDatabases for new envitonments
            /// </summary>
            public const int LibDefaultMaxDatabases = 1024;

            static DbEnvironment()
            {
                DefaultMapSize = LibDefaultMapSize;
                DefaultMaxReaders = LibDefaultMaxReaders;
                DefaultMaxDatabases = LibDefaultMaxDatabases;
            }

            /// <summary>
            /// Default map size for new environments
            /// </summary>
            public static long DefaultMapSize { get; set; }

            /// <summary>
            /// Default MaxReaders for new environments
            /// </summary>
            public static int DefaultMaxReaders { get; set; }

            /// <summary>
            /// Default MaxDatabases for new environments
            /// </summary>
            public static int DefaultMaxDatabases { get; set; }
        }
    }
}
