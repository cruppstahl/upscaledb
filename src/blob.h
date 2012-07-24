/*
 * Copyright (C) 2005-2012 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

/**
 * @brief functions for reading/writing/allocating blobs (memory chunks of
 * arbitrary size)
 *
 */

#ifndef HAM_BLOB_H__
#define HAM_BLOB_H__

#include "internal_fwd_decl.h"
#include "endianswap.h"

#include "packstart.h"

/**
 * a blob structure (blob_t)
 *
 * every blob has a blob_t header; it holds flags and some other 
 * administrative information
 */
typedef HAM_PACK_0 struct HAM_PACK_1 blob_t
{
    /**
     * the blob ID - which is the absolute address/offset of this 
     * blob_t structure in the file
     */
    ham_u64_t _blobid;

    /**
     * the allocated size of the blob; this is the size, which is used
     * by the blob and it's header and maybe additional padding
     */
    ham_u64_t _allocated_size;

    /** the size of the blob */
    ham_u64_t _size;

    /** additional flags */
    ham_u32_t _flags;
} HAM_PACK_2 blob_t;

#include "packstop.h"

/** get the blob ID (blob start address) of a blob_t */
#define blob_get_self(b)               (ham_db2h_offset((b)->_blobid))

/** set the blob ID (blob start address) of a blob_t */
#define blob_set_self(b, s)            (b)->_blobid=ham_h2db_offset(s)

/** get the allocated size of a blob_t */
#define blob_get_alloc_size(b)         (ham_db2h64((b)->_allocated_size))

/** set the allocated size of a blob_t */
#define blob_set_alloc_size(b, s)      (b)->_allocated_size=ham_h2db64(s)

/** get the size of a blob_t */
#define blob_get_size(b)               (ham_db2h64((b)->_size))

/** get the size of a blob_t */
#define blob_set_size(b, s)            (b)->_size=ham_h2db64(s)

/** get flags of a blob_t */
#define blob_get_flags(b)              (ham_db2h32((b)->_flags))

/** set flags of a blob_t */
#define blob_set_flags(b, f)           (b)->_flags=ham_h2db32(f)


#if defined(_MSC_VER) && defined(_CRTDBG_MAP_ALLOC)
#  undef alloc
#  undef free
#  undef realloc
#  undef calloc
#endif

/**
 * The BlobManager manages blobs (not a surprise)
 */
class BlobManager
{
  public:
    BlobManager(Environment *env)
      : m_env(env) {
    }

    /**
     * allocate/create a blob
     * returns the blob-id (the start address of the blob header) in @a blobid
     */
    ham_status_t allocate(Database *db, ham_record_t *record, ham_u32_t flags,
                    ham_offset_t *blobid);

    /**
     * reads a blob and stores the data in @a record
     * flags: either 0 or HAM_DIRECT_ACCESS
     */
    ham_status_t read(Database *db, Transaction *txn, ham_offset_t blobid, 
                    ham_record_t *record, ham_u32_t flags);

    /**
     * retrieves a blob size
     */
    ham_status_t get_datasize(Database *db, ham_offset_t blobid,
                    ham_offset_t *size);

    /**
     * overwrite an existing blob
     *
     * will return an error if the blob does not exist
     * returns the blob-id (the start address of the blob header) in @a blobid
     */
    ham_status_t overwrite(Database *db, ham_offset_t old_blobid, 
                    ham_record_t *record, ham_u32_t flags,
                    ham_offset_t *new_blobid);

    /**
     * delete an existing blob
     */
    ham_status_t free(Database *db, ham_offset_t blobid, ham_u32_t flags);

  private:
    friend class DuplicateManager;

    /**
     * write a series of data chunks to storage at file offset 'addr'.
     * 
     * The chunks are assumed to be stored in sequential order, adjacent
     * to each other, i.e. as one long data strip.
     * 
     * Writing is performed on a per-page basis, where special conditions
     * will decide whether or not the write operation is performed
     * through the page cache or directly to device; such is determined 
     * on a per-page basis.
     */
    ham_status_t write_chunks(Page *page, ham_offset_t addr, bool allocated,
                    bool freshly_created, ham_u8_t **chunk_data,
                    ham_size_t *chunk_size, ham_size_t chunks);

    /**
     * same as above, but for reading chunks from the file
     */
    ham_status_t read_chunk(Page *page, Page **fpage, ham_offset_t addr,
                    Database *db, ham_u8_t *data, ham_size_t size);

    /* 
     * if the blob is small enough (or if logging is enabled) then go through
     * the cache. otherwise use direct I/O
     */
    bool blob_from_cache(ham_size_t size);

    /** the Environment which created this BlobManager */   
    Environment *m_env;
};

#endif /* HAM_BLOB_H__ */
