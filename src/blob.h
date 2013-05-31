/*
 * Copyright (C) 2005-2013 Christoph Rupp (chris@crupp.de).
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
#include "error.h"
#include "page.h"

namespace hamsterdb {

#include "packstart.h"

/**
 * A blob header structure
 *
 * This header is prepended to the blob's payload. It holds the blob size and
 * the blob's address (which is not required but useful for error checking.)
 */
HAM_PACK_0 class HAM_PACK_1 PBlobHeader
{
  public:
    PBlobHeader() {
      memset(this, 0, sizeof(PBlobHeader));
    }

    static PBlobHeader *from_page(Page *page, ham_u64_t address) {
      ham_size_t readstart = (ham_size_t)(address - page->get_self());
      return (PBlobHeader *)&page->get_raw_payload()[readstart];
    }

    /** Returns the absolute address of the blob */
    ham_u64_t get_self() const {
      return (ham_db2h_offset(m_blobid));
    }

    /** Sets the absolute address of the blob */
    void set_self(ham_u64_t id) {
      m_blobid = ham_h2db_offset(id);
    }

    /** Returns the payload size of the blob */
    ham_u64_t get_size() const {
      return (ham_db2h_offset(m_size));
    }

    /** Sets the payload size of the blob */
    void set_size(ham_u64_t size) {
      m_size = ham_h2db_offset(size);
    }

    /** get the allocated size of the blob (includes padding) */
    ham_u64_t get_alloc_size() const {
      return (ham_db2h_offset(m_allocated_size));
    }

    /** set the allocated size of a blob (includes padding) */
    void set_alloc_size(ham_u64_t size) {
      m_allocated_size = ham_h2db64(size);
    }

  private:
    /**
     * The blob ID - which is the absolute address/offset of this
     * structure in the file
     */
    ham_u64_t m_blobid;

    /**
     * the allocated size of the blob; this is the size, which is used
     * by the blob and it's header and maybe additional padding
     */
    ham_u64_t m_allocated_size;

    /** The size of the blob (excluding the header) */
    ham_u64_t m_size;

    /* flags are currently unused, but removing them would break file
     * compatibility; they will be removed with the next file format
     * update */
    ham_u32_t unused_flags;
} HAM_PACK_2;

#include "packstop.h"


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
                    ham_u64_t *blobid);

    /**
     * reads a blob and stores the data in @a record
     * flags: either 0 or HAM_DIRECT_ACCESS
     */
    ham_status_t read(Database *db, Transaction *txn, ham_u64_t blobid,
                    ham_record_t *record, ham_u32_t flags);

    /**
     * retrieves a blob size
     */
    ham_status_t get_datasize(Database *db, ham_u64_t blobid,
                    ham_u64_t *size);

    /**
     * overwrite an existing blob
     *
     * will return an error if the blob does not exist
     * returns the blob-id (the start address of the blob header) in @a blobid
     */
    ham_status_t overwrite(Database *db, ham_u64_t old_blobid,
                    ham_record_t *record, ham_u32_t flags,
                    ham_u64_t *new_blobid);

    /**
     * delete an existing blob
     */
    ham_status_t free(Database *db, ham_u64_t blobid, ham_u32_t flags);

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
    ham_status_t write_chunks(Database *db, Page *page, ham_u64_t addr,
                    bool allocated, bool freshly_created,
                    ham_u8_t **chunk_data, ham_size_t *chunk_size,
                    ham_size_t chunks);

    /**
     * same as above, but for reading chunks from the file
     */
    ham_status_t read_chunk(Page *page, Page **fpage, ham_u64_t addr,
                    Database *db, ham_u8_t *data, ham_size_t size);

    /*
     * if the blob is small enough (or if logging is enabled) then go through
     * the cache. otherwise use direct I/O
     */
    bool blob_from_cache(ham_size_t size);

    /** the Environment which created this BlobManager */
    Environment *m_env;
};

} // namespace hamsterdb

#endif /* HAM_BLOB_H__ */
