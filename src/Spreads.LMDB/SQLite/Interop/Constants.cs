// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;

namespace Microsoft.Data.Sqlite.Interop
{
    internal static class Constants
    {
        // Result Codes

        /// <summary>
        /// Successful result	
        /// </summary>
        public const int SQLITE_OK = 0;
        
        /// <summary>
        /// SQL error or missing database	
        /// </summary>
        public const int SQLITE_ERROR = 1;
        					
        /// <summary>
        /// Internal logic error in SQLite
        /// </summary>
        public const int SQLITE_INTERNAL = 2;
        				
        /// <summary>
        /// Access permission denied
        /// </summary>
        public const int SQLITE_PERM = 3;
        				
        /// <summary>
        /// Callback routine requested an abort	
        /// </summary>
        public const int SQLITE_ABORT = 4;
        				
        /// <summary>
        /// The database file is locked	
        /// </summary>
        public const int SQLITE_BUSY = 5;
        				
        /// <summary>
        /// A table in the database is locked	
        /// </summary>
        public const int SQLITE_LOCKED = 6;


        public const int SQLITE_LOCKED_SHAREDCACHE = 262;
        				
        /// <summary>
        /// A malloc() failed	
        /// </summary>
        public const int SQLITE_NOMEM = 7;
        					
        /// <summary>
        /// Attempt to write a readonly database
        /// </summary>
        public const int SQLITE_READONLY = 8;
        					
        /// <summary>
        /// Operation terminated by sqlite3_interrupt()
        /// </summary>
        public const int SQLITE_INTERRUPT = 9;
        				
        /// <summary>
        /// Some kind of disk I/O error occurred
        /// </summary>
        public const int SQLITE_IOERR = 10;
        				
        /// <summary>
        /// The database disk image is malformed
        /// </summary>
        public const int SQLITE_CORRUPT = 11;
        				
        /// <summary>
        /// Unknown opcode in sqlite3_file_control()
        /// </summary>
        public const int SQLITE_NOTFOUND = 12;
        					
        /// <summary>
        /// Insertion failed because database is full
        /// </summary>
        public const int SQLITE_FULL = 13;
        					
        /// <summary>
        /// Unable to open the database file	
        /// </summary>
        public const int SQLITE_CANTOPEN = 14;
        				
        /// <summary>
        /// Database lock protocol error
        /// </summary>
        public const int SQLITE_PROTOCOL = 15;
        					
        /// <summary>
        /// Database is empty	
        /// </summary>
        public const int SQLITE_EMPTY = 16;
        					
        /// <summary>
        /// The database schema changed	
        /// </summary>
        public const int SQLITE_SCHEMA = 17;
        					
        /// <summary>
        /// String or BLOB exceeds size limit
        /// </summary>
        public const int SQLITE_TOOBIG = 18;
        					
        /// <summary>
        /// Abort due to constraint violation
        /// </summary>
        public const int SQLITE_CONSTRAINT = 19;
        					
        /// <summary>
        /// Data type mismatch	
        /// </summary>
        public const int SQLITE_MISMATCH = 20;
        				
        /// <summary>
        /// Library used incorrectly
        /// </summary>
        public const int SQLITE_MISUSE = 21;
        					
        /// <summary>
        /// Uses OS features not supported on host
        /// </summary>
        public const int SQLITE_NOLFS = 22;
        					
        /// <summary>
        /// Authorization denied	
        /// </summary>
        public const int SQLITE_AUTH = 23;
        					
        /// <summary>
        /// Auxiliary database format error	
        /// </summary>
        public const int SQLITE_FORMAT = 24;
        				
        /// <summary>
        /// 2nd parameter to sqlite3_bind out of range
        /// </summary>
        public const int SQLITE_RANGE = 25;

        /// <summary>
        /// File opened that is not a database file	
        /// </summary>				
        public const int SQLITE_NOTADB = 26;
        					
        /// <summary>
        /// Notifications from sqlite3_log()
        /// </summary>
        public const int SQLITE_NOTICE = 27;
        					
        /// <summary>
        /// Warnings from sqlite3_log()	
        /// </summary>
        public const int SQLITE_WARNING = 28;
        				
        /// <summary>
        /// sqlite3_step() has another row ready
        /// </summary>
        public const int SQLITE_ROW = 100;

        /// <summary>
        /// sqlite3_step() has finished executing	
        /// </summary>
        public const int SQLITE_DONE = 101;



        public const int SQLITE_INTEGER = 1;
        public const int SQLITE_FLOAT = 2;
        public const int SQLITE_TEXT = 3;
        public const int SQLITE_BLOB = 4;
        public const int SQLITE_NULL = 5;

        public const int SQLITE_OPEN_READONLY = 0x00000001;
        public const int SQLITE_OPEN_READWRITE = 0x00000002;
        public const int SQLITE_OPEN_CREATE = 0x00000004;
        public const int SQLITE_OPEN_URI = 0x00000040;
        public const int SQLITE_OPEN_MEMORY = 0x00000080;
        public const int SQLITE_OPEN_SHAREDCACHE = 0x00020000;
        public const int SQLITE_OPEN_PRIVATECACHE = 0x00040000;

        public static readonly IntPtr SQLITE_TRANSIENT = new IntPtr(-1);
        public static readonly IntPtr SQLITE_STATIC = new IntPtr(0);
    }
}
