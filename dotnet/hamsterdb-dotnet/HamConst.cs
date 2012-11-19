/*
 * Copyright (C) 2005-2010 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See file COPYING.GPL2 and COPYING.GPL3 for License information.
 *
 */

namespace Hamster
{
    /// <summary>
    /// Hamsterdb constants - error codes and flags
    /// </summary>
    public sealed class HamConst
    {
        private HamConst() {
        }

        /// <summary>Data Access Mode: random writes</summary>
        public const int HAM_DAM_RANDOM_WRITE               =        1;
        /// <summary>Data Access Mode: sequential inserts</summary>
        public const int HAM_DAM_SEQUENTIAL_INSERT          =        2;

        /// <summary>Operation completed successfully</summary>
        public const int HAM_SUCCESS                        =        0;
        /// <summary>Invalid key size</summary>
        public const int HAM_INV_KEYSIZE                    =       -3;
        /// <summary>Invalid page size (must be 1024 or a multiple of 2048)</summary>
        public const int HAM_INV_PAGESIZE                   =       -4;
        /// <summary>Memory allocation failed - out of memory</summary>
        public const int HAM_OUT_OF_MEMORY                  =       -6;
        /// <summary>Object not initialized</summary>
        public const int HAM_NOT_INITIALIZED                =       -7;
        /// <summary>Invalid function parameter</summary>
        public const int HAM_INV_PARAMETER                  =       -8;
        /// <summary>Invalid file header</summary>
        public const int HAM_INV_FILE_HEADER                =       -9;
        /// <summary>Invalid file version</summary>
        public const int HAM_INV_FILE_VERSION               =      -10;
        /// <summary>Key was not found</summary>
        public const int HAM_KEY_NOT_FOUND                  =      -11;
        /// <summary>Tried to insert a key which already exists</summary>
        public const int HAM_DUPLICATE_KEY                  =      -12;
        /// <summary>Internal Database integrity violated</summary>
        public const int HAM_INTEGRITY_VIOLATED             =      -13;
        /// <summary>Internal hamsterdb error</summary>
        public const int HAM_INTERNAL_ERROR                 =      -14;
        /// <summary>Tried to modify the Database, but the file was opened as read-only</summary>
        public const int HAM_DB_READ_ONLY                   =      -15;
        /// <summary>Database record not found</summary>
        public const int HAM_BLOB_NOT_FOUND                 =      -16;
        /// <summary>Prefix comparison function needs more data</summary>
        public const int HAM_PREFIX_REQUEST_FULLKEY         =      -17;
        /// <summary>Generic file I/O error</summary>
        public const int HAM_IO_ERROR                       =      -18;
        /// <summary>Database cache is full</summary>
        public const int HAM_CACHE_FULL                     =      -19;
        /// <summary>Function is not yet implemented</summary>
        public const int HAM_NOT_IMPLEMENTED                =      -20;
        /// <summary>File not found</summary>
        public const int HAM_FILE_NOT_FOUND                 =      -21;
        /// <summary>Operation would block</summary>
        public const int HAM_WOULD_BLOCK                    =      -22;
        /// <summary>Object was not initialized correctly</summary>
        public const int HAM_NOT_READY                      =      -23;
        /// <summary>Database limits reached</summary>
        public const int HAM_LIMITS_REACHED                 =      -24;
        /// <summary>Object was already initialized</summary>
        public const int HAM_ALREADY_INITIALIZED            =      -27;
        /// <summary>Database needs recovery</summary>
        public const int HAM_NEED_RECOVERY                  =      -28;
        /// <summary>Cursor must be closed prior to Transaction abort/commit</summary>
        public const int HAM_CURSOR_STILL_OPEN              =      -29;
        /// <summary>Record filter or file filter not found</summary>
        public const int HAM_FILTER_NOT_FOUND               =      -30;
        /// <summary>Operation conflicts with another Transaction</summary>
        public const int HAM_TXN_CONFLICT                   =      -31;
        /// <summary>Database cannot be closed because it is modified in a Transaction</summary>
        public const int HAM_TXN_STIL_OPEN                  =      -33;
        /// <summary>Cursor does not point to a valid item</summary>
        public const int HAM_CURSOR_IS_NIL                  =     -100;
        /// <summary>Database not found</summary>
        public const int HAM_DATABASE_NOT_FOUND             =     -200;
        /// <summary>Database name already exists</summary>
        public const int HAM_DATABASE_ALREADY_EXISTS        =     -201;
        /// <summary>Database already open, or: Database handle is already initialized</summary>
        public const int HAM_DATABASE_ALREADY_OPEN          =     -202;
        /// <summary>Environment already open, or: Environment handle is already initialized</summary>
        public const int HAM_ENVIRONMENT_ALREADY_OPEN       =     -203;
        /// <summary>Invalid log file header</summary>
        public const int HAM_LOG_INV_FILE_HEADER            =     -300;

        // Error handling levels
        /// <summary>A debug message</summary>
        public const int HAM_DEBUG_LEVEL_DEBUG          =    0;
        /// <summary>A normal error message</summary>
        public const int HAM_DEBUG_LEVEL_NORMAL         =    1;
        /// <summary>A fatal error message</summary>
        public const int HAM_DEBUG_LEVEL_FATAL          =    3;

        // Transaction constants
        /// <summary>Flag for Transaction.Begin</summary>
        public const int HAM_TXN_READ_ONLY                  =    1;
        /// <summary>Flag for Transaction.Commit</summary>
        public const int HAM_TXN_FORCE_WRITE                =    1;

        // Create/Open flags
        /// <summary>Flag for Database.Open, Database.Create</summary>
        public const int HAM_WRITE_THROUGH                  =    0x001;
        /// <summary>Flag for Database.Open</summary>
        public const int HAM_READ_ONLY                      =    0x004;
        /// <summary>Flag for Database.Create</summary>
        public const int HAM_USE_BTREE                      =    0x010;
        /// <summary>Flag for Database.Create</summary>
        public const int HAM_DISABLE_VAR_KEYLEN             =    0x040;
        /// <summary>Flag for Database.Create</summary>
        public const int HAM_IN_MEMORY_DB                   =    0x080;
        /// <summary>Flag for Database.Open, Database.Create</summary>
        public const int HAM_DISABLE_MMAP                   =    0x200;
        /// <summary>Flag for Database.Open, Database.Create</summary>
        public const int HAM_CACHE_STRICT                   =    0x400;
        /// <summary>Flag for Database.Open, Database.Create</summary>
        public const int HAM_DISABLE_FREELIST_FLUSH         =    0x800;
        /// <summary>Flag for Database.Open, Database.Create</summary>
        public const int HAM_LOCK_EXCLUSIVE                 =   0x1000;
        /// <summary>Flag for Database.Create</summary>
        public const int HAM_RECORD_NUMBER                  =   0x2000;
        /// <summary>Flag for Database.Create</summary>
        public const int HAM_ENABLE_DUPLICATES              =   0x4000;
        /// <summary>Flag for Database.Create</summary>
        public const int HAM_ENABLE_RECOVERY                =   0x8000;
        /// <summary>Flag for Database.Open</summary>
        public const int HAM_AUTO_RECOVERY                  =  0x10000;
        /// <summary>Flag for Database.Create, Database.Open</summary>
        public const int HAM_ENABLE_TRANSACTIONS            =  0x20000;
        /// <summary>Flag for Database.Create, Database.Open</summary>
        public const int HAM_CACHE_UNLIMITED                =  0x40000;

        // Extended parameters
        /// <summary>Parameter name for Database.Open, Database.Create</summary>
        public const int HAM_PARAM_CACHESIZE                =    0x100;
        /// <summary>Parameter name for Database.Create</summary>
        public const int HAM_PARAM_PAGESIZE                 =    0x101;
        /// <summary>Parameter name for Database.Create</summary>
        public const int HAM_PARAM_KEYSIZE                  =    0x102;
        /// <summary>Parameter name for Environment.Create</summary>
        public const int HAM_PARAM_MAX_ENV_DATABASES        =    0x103;
        /// <summary>Parameter name for Database.Create, Database.Open</summary>
        public const int HAM_PARAM_DATA_ACCESS_MODE         =    0x104;

        // Database operations
        /// <summary>Parameter for GetParameters</summary>
        public const int HAM_PARAM_GET_FLAGS            =        0x00000200;
        /// <summary>Parameter for GetParameters</summary>
        public const int HAM_PARAM_GET_FILEMODE         =        0x00000201;
        /// <summary>Parameter for GetParameters</summary>
        public const int HAM_PARAM_GET_FILENAME         =        0x00000202;
        /// <summary>Parameter for GetParameters</summary>
        public const int HAM_PARAM_DBNAME               =        0x00000203;
        /// <summary>Parameter for GetParameters</summary>
        public const int HAM_PARAM_GET_KEYS_PER_PAGE        =        0x00000204;
        /// <summary>Parameter for GetParameters</summary>
        public const int HAM_PARAM_GET_DAM                  =        0x00000205;
        /// <summary>Parameter for GetParameters</summary>
        public const int HAM_PARAM_GET_STATISTICS               =        0x00000206;

        // Database operations
        /// <summary>Flag for Database.Insert, Cursor.Insert</summary>
        public const int HAM_OVERWRITE                      =        0x0001;
        /// <summary>Flag for Database.Insert, Cursor.Insert</summary>
        public const int HAM_DUPLICATE                      =        0x0002;
        /// <summary>Flag for Cursor.Insert</summary>
        public const int HAM_DUPLICATE_INSERT_BEFORE        =        0x0004;
        /// <summary>Flag for Cursor.Insert</summary>
        public const int HAM_DUPLICATE_INSERT_AFTER         =        0x0008;
        /// <summary>Flag for Cursor.Insert</summary>
        public const int HAM_DUPLICATE_INSERT_FIRST         =       0x0010;
        /// <summary>Flag for Cursor.Insert</summary>
        public const int HAM_DUPLICATE_INSERT_LAST          =       0x0020;
        /// <summary>Flag for Database.Find</summary>
        public const int HAM_DIRECT_ACCESS                  =       0x0040;
        /// <summary>Flag for Database.Insert</summary>
        public const int HAM_PARTIAL                        =       0x0080;
        /// <summary>Flag for Database.Insert</summary>
        public const int HAM_HINT_APPEND                    =       0x0080000;
        /// <summary>Flag for Database.Insert</summary>
        public const int HAM_HINT_PREPEND                   =       0x0100000;
        /// <summary>Flag for Database.GetKeyCount</summary>
        public const int HAM_FAST_ESTIMATE                  =       1;
        /// <summary>Flag for Database.Close</summary>
        public const int HAM_AUTO_CLEANUP                   =        1;
        /// <summary>Private flag for testing</summary>
        public const int HAM_DONT_CLEAR_LOG                 =        2;
        /// <summary>Flag for Database.Close</summary>
        public const int HAM_TXN_AUTO_ABORT                 =        4;
        /// <summary>Flag for Database.Close</summary>
        public const int HAM_TXN_AUTO_COMMIT                =        8;

        // Cursor operations
        /// <summary>Flag for Cursor.Move</summary>
        public const int HAM_CURSOR_FIRST                   =        1;
        /// <summary>Flag for Cursor.Move</summary>
        public const int HAM_CURSOR_LAST                    =        2;
        /// <summary>Flag for Cursor.Move</summary>
        public const int HAM_CURSOR_NEXT                    =        4;
        /// <summary>Flag for Cursor.Move</summary>
        public const int HAM_CURSOR_PREVIOUS                =        8;
        /// <summary>Flag for Cursor.Move</summary>
        public const int HAM_SKIP_DUPLICATES                =       16;
        /// <summary>Flag for Cursor.Move</summary>
        public const int HAM_ONLY_DUPLICATES                =       32;

        // Cursor find flags
        /// <summary>Flag for Cursor.Find</summary>
        public const int HAM_FIND_EXACT_MATCH               =        0x4000;
        /// <summary>Flag for Cursor.Find</summary>
        public const int HAM_FIND_LT_MATCH                  =        0x1000;
        /// <summary>Flag for Cursor.Find</summary>
        public const int HAM_FIND_GT_MATCH                  =        0x2000;
        /// <summary>Flag for Cursor.Find</summary>
        public const int HAM_FIND_LEQ_MATCH  = (HAM_FIND_LT_MATCH|HAM_FIND_EXACT_MATCH);
        /// <summary>Flag for Cursor.Find</summary>
        public const int HAM_FIND_GEQ_MATCH  = (HAM_FIND_GT_MATCH|HAM_FIND_EXACT_MATCH);
        /// <summary>Flag for Cursor.Find</summary>
        public const int HAM_FIND_NEAR_MATCH = (HAM_FIND_GEQ_MATCH|HAM_FIND_LEQ_MATCH);
    }
}
