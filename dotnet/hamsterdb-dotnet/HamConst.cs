/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

    /// <summary>Operation completed successfully</summary>
    public const int HAM_SUCCESS                =    0;
    /// <summary>Invalid record size</summary>
    public const int HAM_INV_RECORD_SIZE        =     -2;
    /// <summary>Invalid key size</summary>
    public const int HAM_INV_KEY_SIZE           =     -3;
    /// <summary>Invalid key size</summary>
    public const int HAM_INV_KEYSIZE            =     -3;
    /// <summary>Invalid page size (must be 1024 or a multiple of 2048)</summary>
    public const int HAM_INV_PAGESIZE           =     -4;
    /// <summary>Memory allocation failed - out of memory</summary>
    public const int HAM_OUT_OF_MEMORY          =     -6;
    /// <summary>Invalid function parameter</summary>
    public const int HAM_INV_PARAMETER          =     -8;
    /// <summary>Invalid file header</summary>
    public const int HAM_INV_FILE_HEADER        =     -9;
    /// <summary>Invalid file version</summary>
    public const int HAM_INV_FILE_VERSION       =    -10;
    /// <summary>Key was not found</summary>
    public const int HAM_KEY_NOT_FOUND          =    -11;
    /// <summary>Tried to insert a key which already exists</summary>
    public const int HAM_DUPLICATE_KEY          =    -12;
    /// <summary>Internal Database integrity violated</summary>
    public const int HAM_INTEGRITY_VIOLATED     =    -13;
    /// <summary>Internal hamsterdb error</summary>
    public const int HAM_INTERNAL_ERROR         =    -14;
    /// <summary>Tried to modify the Database, but the file was opened as read-only</summary>
    public const int HAM_WRITE_PROTECTED        =    -15;
    /// <summary>Database record not found</summary>
    public const int HAM_BLOB_NOT_FOUND         =    -16;
    /// <summary>Prefix comparison function needs more data</summary>
    public const int HAM_PREFIX_REQUEST_FULLKEY =    -17;
    /// <summary>Generic file I/O error</summary>
    public const int HAM_IO_ERROR               =    -18;
    /// <summary>Function is not yet implemented</summary>
    public const int HAM_NOT_IMPLEMENTED        =    -20;
    /// <summary>File not found</summary>
    public const int HAM_FILE_NOT_FOUND         =    -21;
    /// <summary>Operation would block</summary>
    public const int HAM_WOULD_BLOCK            =    -22;
    /// <summary>Object was not initialized correctly</summary>
    public const int HAM_NOT_READY              =    -23;
    /// <summary>Database limits reached</summary>
    public const int HAM_LIMITS_REACHED         =    -24;
    /// <summary>Object was already initialized</summary>
    public const int HAM_ALREADY_INITIALIZED    =    -27;
    /// <summary>Database needs recovery</summary>
    public const int HAM_NEED_RECOVERY          =    -28;
    /// <summary>Cursor must be closed prior to Transaction abort/commit</summary>
    public const int HAM_CURSOR_STILL_OPEN      =    -29;
    /// <summary>Operation conflicts with another Transaction</summary>
    public const int HAM_TXN_CONFLICT           =    -31;
    /// <summary>Database cannot be closed because it is modified in a Transaction</summary>
    public const int HAM_TXN_STIL_OPEN          =    -33;
    /// <summary>Cursor does not point to a valid item</summary>
    public const int HAM_CURSOR_IS_NIL          =   -100;
    /// <summary>Database not found</summary>
    public const int HAM_DATABASE_NOT_FOUND     =   -200;
    /// <summary>Database name already exists</summary>
    public const int HAM_DATABASE_ALREADY_EXISTS    =   -201;
    /// <summary>Database already open, or: Database handle is already initialized</summary>
    public const int HAM_DATABASE_ALREADY_OPEN  =   -202;
    /// <summary>Environment already open, or: Environment handle is already initialized</summary>
    public const int HAM_ENVIRONMENT_ALREADY_OPEN   =   -203;
    /// <summary>Invalid log file header</summary>
    public const int HAM_LOG_INV_FILE_HEADER    =   -300;

    // Error handling levels
    /// <summary>A debug message</summary>
    public const int HAM_DEBUG_LEVEL_DEBUG      =  0;
    /// <summary>A normal error message</summary>
    public const int HAM_DEBUG_LEVEL_NORMAL     =  1;
    /// <summary>A fatal error message</summary>
    public const int HAM_DEBUG_LEVEL_FATAL      =  3;

    // Transaction constants
    /// <summary>Flag for Transaction.Begin</summary>
    public const int HAM_TXN_READ_ONLY          =  1;
    /// <summary>Flag for Transaction.Commit</summary>
    public const int HAM_TXN_FORCE_WRITE        =  1;

    // Create/Open flags
    /// <summary>Flag for Database.Open, Database.Create</summary>
    public const int HAM_ENABLE_FSYNC           =  0x001;
    /// <summary>Flag for Database.Open</summary>
    public const int HAM_READ_ONLY              =  0x004;
    /// <summary>Flag for Database.Create</summary>
    public const int HAM_IN_MEMORY              =  0x00080;
    /// <summary>Flag for Database.Open, Database.Create</summary>
    public const int HAM_DISABLE_MMAP           =  0x00200;
    /// <summary>Flag for Database.Open, Database.Create</summary>
    public const int HAM_DISABLE_FREELIST_FLUSH =  0x00800;
    /// <summary>Flag for Database.Create</summary>
    public const int HAM_RECORD_NUMBER          =  0x02000;
    /// <summary>Flag for Database.Create</summary>
    public const int HAM_ENABLE_DUPLICATE_KEYS  =  0x04000;
    /// <summary>Flag for Database.Create</summary>
    public const int HAM_ENABLE_RECOVERY        =  0x08000;
    /// <summary>Flag for Database.Open</summary>
    public const int HAM_AUTO_RECOVERY          =  0x10000;
    /// <summary>Flag for Database.Create, Database.Open</summary>
    public const int HAM_ENABLE_TRANSACTIONS    =  0x20000;
    /// <summary>Flag for Database.Create, Database.Open</summary>
    public const int HAM_CACHE_UNLIMITED        =  0x40000;
    /// <summary>Flag for Environment.Create, Environment.Open</summary>
    public const int HAM_FLUSH_WHEN_COMMITTED   =  0x01000000;

    // Extended parameters
    /// <summary>Parameter name for Database.Open, Database.Create</summary>
    public const int HAM_PARAM_CACHESIZE        =  0x00100;
    /// <summary>Parameter name for Database.Create</summary>
    public const int HAM_PARAM_PAGESIZE         =  0x00101;
    /// <summary>Parameter name for Database.Create</summary>
    public const int HAM_PARAM_KEY_SIZE         =  0x00102;
    /// <summary>Parameter name for Database.Create</summary>
    public const int HAM_PARAM_KEYSIZE          =  0x00102;
    /// <summary>Parameter name for Environment.Create</summary>
    public const int HAM_PARAM_MAX_DATABASES    =  0x00103;
    /// <summary>Parameter name for Database.Create</summary>
    public const int HAM_PARAM_KEY_TYPE         =  0x00104;
    /// <summary>Parameter name for Environment.Open, Environment.Create</summary>
    public const int HAM_PARAM_NETWORK_TIMEOUT_SEC = 0x00000107;
    /// <summary>Parameter name for Database.Create</summary>
    public const int HAM_PARAM_RECORD_SIZE      =  0x00108;

    // Database operations
    /// <summary>Parameter for GetParameters</summary>
    public const int HAM_PARAM_FLAGS                = 0x00000200;
    /// <summary>Parameter for GetParameters</summary>
    public const int HAM_PARAM_FILEMODE             = 0x00000201;
    /// <summary>Parameter for GetParameters</summary>
    public const int HAM_PARAM_FILENAME             = 0x00000202;
    /// <summary>Parameter for GetParameters</summary>
    public const int HAM_PARAM_DATABASE_NAME        = 0x00000203;
    /// <summary>Parameter for GetParameters</summary>
    public const int HAM_PARAM_MAX_KEYS_PER_PAGE    = 0x00000204;
    /// <summary>Value for HAM_PARAM_KEY_SIZE</summary>
    public const int HAM_KEY_SIZE_UNLIMITED         = 0xffff;
    /// <summary>Value for HAM_PARAM_RECORD_SIZE</summary>
    public const long HAM_RECORD_SIZE_UNLIMITED     = 0xffffffff;

    /// <summary>hamsterdb pro: Value for Environment.Create,
    /// Environment.Open</summary>
    public const int HAM_PARAM_JOURNAL_COMPRESSION  = 0x1000;
    /// <summary>hamsterdb pro: Value for Database.Create,
    /// Database.Open</summary>
    public const int HAM_PARAM_RECORD_COMPRESSION   = 0x1001;
    /// <summary>hamsterdb pro: Value for Database.Create,
    /// Database.Open</summary>
    public const int HAM_PARAM_KEY_COMPRESSION      = 0x1002;
    /// <summary>hamsterdb pro: "null" compression</summary>
    public const int HAM_COMPRESSION_NONE                 =      0;
    /// <summary>hamsterdb pro: zlib compression</summary>
    public const int HAM_COMPRESSION_ZLIB                 =      1;
    /// <summary>hamsterdb pro: snappy compression</summary>
    public const int HAM_COMPRESSION_SNAPPY               =      2;
    /// <summary>hamsterdb pro: lzf compression</summary>
    public const int HAM_COMPRESSION_LZF                  =      3;
    /// <summary>hamsterdb pro: lzop compression</summary>
    public const int HAM_COMPRESSION_LZOP                 =      4;

    // Database operations
    /// <summary>Flag for Database.Insert, Cursor.Insert</summary>
    public const int HAM_OVERWRITE                  =    0x0001;
    /// <summary>Flag for Database.Insert, Cursor.Insert</summary>
    public const int HAM_DUPLICATE                  =    0x0002;
    /// <summary>Flag for Cursor.Insert</summary>
    public const int HAM_DUPLICATE_INSERT_BEFORE    =    0x0004;
    /// <summary>Flag for Cursor.Insert</summary>
    public const int HAM_DUPLICATE_INSERT_AFTER     =    0x0008;
    /// <summary>Flag for Cursor.Insert</summary>
    public const int HAM_DUPLICATE_INSERT_FIRST     =    0x0010;
    /// <summary>Flag for Cursor.Insert</summary>
    public const int HAM_DUPLICATE_INSERT_LAST      =    0x0020;
    /// <summary>Flag for Database.Find</summary>
    public const int HAM_DIRECT_ACCESS              =    0x0040;
    /// <summary>Flag for Database.Insert</summary>
    public const int HAM_PARTIAL            =     0x0080;
    /// <summary>Flag for Database.Insert</summary>
    public const int HAM_HINT_APPEND          =     0x0080000;
    /// <summary>Flag for Database.Insert</summary>
    public const int HAM_HINT_PREPEND           =     0x0100000;
    /// <summary>Flag for Database.Close</summary>
    public const int HAM_AUTO_CLEANUP           =    1;
    /// <summary>Private flag for testing</summary>
    public const int HAM_DONT_CLEAR_LOG         =    2;
    /// <summary>Flag for Database.Close</summary>
    public const int HAM_TXN_AUTO_ABORT         =    4;
    /// <summary>Flag for Database.Close</summary>
    public const int HAM_TXN_AUTO_COMMIT        =    8;

    // Cursor operations
    /// <summary>Flag for Cursor.Move</summary>
    public const int HAM_CURSOR_FIRST           =    1;
    /// <summary>Flag for Cursor.Move</summary>
    public const int HAM_CURSOR_LAST          =    2;
    /// <summary>Flag for Cursor.Move</summary>
    public const int HAM_CURSOR_NEXT          =    4;
    /// <summary>Flag for Cursor.Move</summary>
    public const int HAM_CURSOR_PREVIOUS        =    8;
    /// <summary>Flag for Cursor.Move</summary>
    public const int HAM_SKIP_DUPLICATES        =     16;
    /// <summary>Flag for Cursor.Move</summary>
    public const int HAM_ONLY_DUPLICATES        =     32;

    // Cursor find flags
    /// <summary>Flag for Cursor.Find</summary>
    public const int HAM_FIND_EXACT_MATCH         =    0x4000;
    /// <summary>Flag for Cursor.Find</summary>
    public const int HAM_FIND_LT_MATCH          =    0x1000;
    /// <summary>Flag for Cursor.Find</summary>
    public const int HAM_FIND_GT_MATCH          =    0x2000;
    /// <summary>Flag for Cursor.Find</summary>
    public const int HAM_FIND_LEQ_MATCH  = (HAM_FIND_LT_MATCH|HAM_FIND_EXACT_MATCH);
    /// <summary>Flag for Cursor.Find</summary>
    public const int HAM_FIND_GEQ_MATCH  = (HAM_FIND_GT_MATCH|HAM_FIND_EXACT_MATCH);
    /// <summary>Flag for Cursor.Find</summary>
    public const int HAM_FIND_NEAR_MATCH = (HAM_FIND_GEQ_MATCH|HAM_FIND_LEQ_MATCH);

    /// <summary>A binary blob without type; sorted by memcmp</summary>
    public const int HAM_TYPE_BINARY            =         0;
    /// <summary>A binary blob without type; sorted by callback function</summary>
    public const int HAM_TYPE_CUSTOM            =         1;
    /// <summary>An unsigned 8-bit integer</summary>
    public const int HAM_TYPE_UINT8             =         3;
    /// <summary>An unsigned 16-bit integer</summary>
    public const int HAM_TYPE_UINT16            =         5;
    /// <summary>An unsigned 32-bit integer</summary>
    public const int HAM_TYPE_UINT32            =         7;
    /// <summary>An unsigned 64-bit integer</summary>
    public const int HAM_TYPE_UINT64            =         9;
    /// <summary>An 32-bit float</summary>
    public const int HAM_TYPE_REAL32            =        11;
    /// <summary>An 64-bit double</summary>
    public const int HAM_TYPE_REAL64            =        12;
  }
}
