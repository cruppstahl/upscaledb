/**
 * Copyright (C) 2005-2012 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See file COPYING.GPL2 and COPYING.GPL3 for License information.
 *
 */

package de.crupp.hamsterdb;

public class Const {

    /** Operation completed successfully */
    public final static int HAM_SUCCESS                         =        0;

    /** Invalid key size */
    public final static int HAM_INV_KEYSIZE                     =       -3;

    /** Invalid page size (must be a multiple of 1024) */
    public final static int HAM_INV_PAGESIZE                    =       -4;

    /** Memory allocation failed - out of memory */
    public final static int HAM_OUT_OF_MEMORY                   =       -6;

    /** Object not initialized */
    public final static int HAM_NOT_INITIALIZED                 =       -7;

    /** Invalid function parameter */
    public final static int HAM_INV_PARAMETER                   =       -8;

    /** Invalid file header */
    public final static int HAM_INV_FILE_HEADER                 =       -9;

    /** Invalid file version */
    public final static int HAM_INV_FILE_VERSION                =      -10;

    /** Key was not found */
    public final static int HAM_KEY_NOT_FOUND                   =      -11;

    /** Tried to insert a key which already exists */
    public final static int HAM_DUPLICATE_KEY                   =      -12;

    /** Internal Database integrity violated */
    public final static int HAM_INTEGRITY_VIOLATED              =      -13;

    /** Internal hamsterdb error */
    public final static int HAM_INTERNAL_ERROR                  =      -14;

    /** Tried to modify the Database, but the file was opened as read-only */
    public final static int HAM_WRITE_PROTECTED                 =      -15;

    /** Database record not found */
    public final static int HAM_BLOB_NOT_FOUND                  =      -16;

    /** Prefix comparison function needs more data */
    public final static int HAM_PREFIX_REQUEST_FULLKEY          =      -17;

    /** Generic file I/O error */
    public final static int HAM_IO_ERROR                        =      -18;

    /** Database cache is full */
    public final static int HAM_CACHE_FULL                      =      -19;

    /** Function is not yet implemented */
    public final static int HAM_NOT_IMPLEMENTED                 =      -20;

    /** File not found */
    public final static int HAM_FILE_NOT_FOUND                  =      -21;

    /** Operation would block */
    public final static int HAM_WOULD_BLOCK                     =      -22;

    /** Object was not initialized correctly */
    public final static int HAM_NOT_READY                       =      -23;

    /** Database limits reached */
    public final static int HAM_LIMITS_REACHED                  =      -24;

    /** AES encryption key is wrong */
    public final static int HAM_ACCESS_DENIED                   =      -25;

    /** Object was already initialized */
    public final static int HAM_ALREADY_INITIALIZED             =      -27;

    /** Database needs recovery */
    public final static int HAM_NEED_RECOVERY                   =      -28;

    /** Cursor must be closed prior to Txn abort/commit */
    public final static int HAM_CURSOR_STILL_OPEN               =      -29;

    /** Record or file filter not found */
    public final static int HAM_FILTER_NOT_FOUND                =      -30;

    /** Operation conflicts with another Transaction */
    public final static int HAM_TXN_CONFLICT                    =      -31;

    /** Database cannot be closed because it is modified in a Transaction */
    public final static int HAM_TXN_STILL_OPEN                  =      -33;

    /** Cursor does not point to a valid item */
    public final static int HAM_CURSOR_IS_NIL                   =     -100;

    /** Database not found */
    public final static int HAM_DATABASE_NOT_FOUND              =     -200;

    /** Database name already exists */
    public final static int HAM_DATABASE_ALREADY_EXISTS         =     -201;

    /** Database already open */
    public final static int HAM_DATABASE_ALREADY_OPEN           =     -202;

    /** Environment already open */
    public final static int HAM_ENVIRONMENT_ALREADY_OPEN        =     -202;

    /** Invalid log file header */
    public final static int HAM_LOG_INV_FILE_HEADER             =     -300;

    /** Remote I/O error/Network error */
    public final static int HAM_NETWORK_ERROR                   =     -400;

    /** Debug message severity - debug */
    public final static int HAM_DEBUG_LEVEL_DEBUG               =        0;

    /** Debug message severity - normal */
    public final static int HAM_DEBUG_LEVEL_NORMAL              =        1;

    /** Debug message severity - fatal */
    public final static int HAM_DEBUG_LEVEL_FATAL               =        3;

    /** Flag for Transaction.begin() */
    public final static int HAM_TXN_READ_ONLY                   =        1;

    /** Flag for Transaction.commit() */
    public final static int HAM_TXN_FORCE_WRITE                 =        1;

    /** Flag for Database.open(), Database.create() */
    public final static int HAM_ENABLE_FSYNC                    =    0x001;

    /** Flag for Database.open() */
    public final static int HAM_READ_ONLY                       =    0x004;

    /** Flag for Database.create() */
    public final static int HAM_DISABLE_VAR_KEYLEN              =    0x040;

    /** Flag for Database.create() */
    public final static int HAM_IN_MEMORY_DB                    =    0x080;

    /** Flag for Database.open(), Database.create() */
    public final static int HAM_DISABLE_MMAP                    =    0x200;

    /** Flag for Database.open(), Database.create() */
    public final static int HAM_CACHE_STRICT                    =    0x400;

    /** Flag for Database.open(), Database.create() */
    public final static int HAM_DISABLE_FREELIST_FLUSH          =    0x800;

    /** Flag for Database.open(), Database.create() */
    public final static int HAM_LOCK_EXCLUSIVE                  =   0x1000;

    /** Flag for Database.create() */
    public final static int HAM_RECORD_NUMBER                   =   0x2000;

    /** Flag for Database.create() */
    public final static int HAM_ENABLE_DUPLICATES               =   0x4000;

    /** Flag for Database.create(), Database.open() */
    public final static int HAM_ENABLE_RECOVERY                 =   0x8000;

    /** Flag for Database.open() */
    public final static int HAM_AUTO_RECOVERY                   =  0x10000;

    /** Flag for Database.create, Database.open(), ... */
    public final static int HAM_ENABLE_TRANSACTIONS             =  0x20000;

    /** Flag for Database.create, Database.open(), ... */
    public final static int HAM_CACHE_UNLIMITED                 =  0x40000;

    /** Parameter name for Database.open(), Database.create() */
    public final static int HAM_PARAM_CACHESIZE                 =    0x100;

    /** Parameter name for Database.create() */
    public final static int HAM_PARAM_PAGESIZE                  =    0x101;

    /** Parameter name for Database.create() */
    public final static int HAM_PARAM_KEYSIZE                   =    0x102;

    /** Parameter name for Environment.create() */
    public final static int HAM_PARAM_MAX_DATABASES             =    0x103;

    /** Parameter name for Environment.getParameters(),... */
    public final static int HAM_PARAM_FLAGS                     =    0x200;

    /** Parameter name for Environment.getParameters(),... */
    public final static int HAM_PARAM_FILEMODE                  =    0x201;

    /** Parameter name for Environment.getParameters(),... */
    public final static int HAM_PARAM_FILENAME                  =    0x202;

    /** Parameter name for Database.getParameters() */
    public final static int HAM_PARAM_DATABASE_NAME             =    0x203;

    /** Parameter name for Database.getParameters() */
    public final static int HAM_PARAM_MAX_KEYS_PER_PAGE         =    0x204;

    /** Flag for Database.insert(), Cursor.insert() */
    public final static int HAM_OVERWRITE                       =        1;

    /** Flag for Database.insert(), Cursor.insert() */
    public final static int HAM_DUPLICATE                       =        2;

    /** Flag for Cursor.insert() */
    public final static int HAM_DUPLICATE_INSERT_BEFORE         =        4;

    /** Flag for Cursor.insert() */
    public final static int HAM_DUPLICATE_INSERT_AFTER          =        8;

    /** Flag for Cursor.insert() */
    public final static int HAM_DUPLICATE_INSERT_FIRST          =       16;

    /** Flag for Cursor.insert() */
    public final static int HAM_DUPLICATE_INSERT_LAST           =       32;

    /** Flag for Database.find(), Cursor.move() */
    public final static int HAM_DIRECT_ACCESS                   =     0x40;

    /** Flag for Database.find(), Cursor.insert(), Cursor.move()... */
    public final static int HAM_PARTIAL                         =     0x80;

    /** Flag for Database.insert(), ... */
    public final static int HAM_HINT_APPEND                     =     0x80000;

    /** Flag for Database.insert(), ... */
    public final static int HAM_HINT_PREPEND                    =     0x100000;

    /** Flag for Database.close(), Environment.close() */
    public final static int HAM_AUTO_CLEANUP                    =        1;

    /* (Private) Flag for Database.close() */
    public final static int HAM_DONT_CLEAR_LOG                  =        2;

    /* Flag for Database.close() */
    public final static int HAM_TXN_AUTO_ABORT                  =        4;

    /* Flag for Database.close() */
    public final static int HAM_TXN_AUTO_COMMIT                 =        8;

    /** Flag for Cursor.move() */
    public final static int HAM_CURSOR_FIRST                    =        1;

    /** Flag for Cursor.move() */
    public final static int HAM_CURSOR_LAST                     =        2;

    /** Flag for Cursor.move() */
    public final static int HAM_CURSOR_NEXT                     =        4;

    /** Flag for Cursor.move() */
    public final static int HAM_CURSOR_PREVIOUS                 =        8;

    /** Flag for Cursor.move() */
    public final static int HAM_SKIP_DUPLICATES                 =       16;

    /** Flag for Cursor.move() */
    public final static int HAM_ONLY_DUPLICATES                 =       32;

    /** Flag for Database.find() */
    public final static int HAM_FIND_EXACT_MATCH                =   0x4000;

    /** Flag for Database.find() */
    public final static int HAM_FIND_LT_MATCH                   =   0x1000;

    /** Flag for Database.find() */
    public final static int HAM_FIND_GT_MATCH                   =   0x2000;

    /** Flag for Database.find() */
    public final static int HAM_FIND_LEQ_MATCH                  =
                HAM_FIND_LT_MATCH|HAM_FIND_EXACT_MATCH;

    /** Flag for Database.find() */
    public final static int HAM_FIND_GEQ_MATCH                  =
                HAM_FIND_GT_MATCH|HAM_FIND_EXACT_MATCH;

    /** Flag for Database.find() */
    public final static int HAM_FIND_NEAR_MATCH                 =
                HAM_FIND_GEQ_MATCH|HAM_FIND_LEQ_MATCH;

}
