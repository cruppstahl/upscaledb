/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
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

#ifndef HAM_BLOB_MANAGER_H__
#define HAM_BLOB_MANAGER_H__

#include <ham/hamsterdb_int.h>
#include "page.h"

namespace hamsterdb {

class ByteArray;
class LocalDatabase;
class LocalEnvironment;

#include "packstart.h"

// A blob header structure
//
// This header is prepended to the blob's payload. It holds the blob size and
// the blob's address (which is not required but useful for error checking.)
HAM_PACK_0 class HAM_PACK_1 PBlobHeader
{
  public:
    PBlobHeader() {
      memset(this, 0, sizeof(PBlobHeader));
    }

    // Returns a PBlobHeader from a file address
    static PBlobHeader *from_page(Page *page, ham_u64_t address) {
      ham_u32_t readstart = (ham_u32_t)(address - page->get_address());
      return (PBlobHeader *)&page->get_raw_payload()[readstart];
    }

    // Returns the absolute address of the blob
    ham_u64_t get_self() const {
      return (ham_db2h_offset(m_blobid));
    }

    // Sets the absolute address of the blob
    void set_self(ham_u64_t id) {
      m_blobid = ham_h2db_offset(id);
    }

    // Returns the payload size of the blob
    ham_u64_t get_size() const {
      return (ham_db2h_offset(m_size));
    }

    // Sets the payload size of the blob
    void set_size(ham_u64_t size) {
      m_size = ham_h2db_offset(size);
    }

    // Returns the allocated size of the blob (includes padding)
    ham_u64_t get_alloc_size() const {
      return (ham_db2h_offset(m_allocated_size));
    }

    // Sets the allocated size of a blob (includes padding)
    void set_alloc_size(ham_u64_t size) {
      m_allocated_size = ham_h2db64(size);
    }

  private:
    // Flags are currently unused, but might be required later (i.e. for
    // compression
    ham_u32_t unused_flags;

    // The blob ID - which is the absolute address/offset of this
    //* structure in the file
    ham_u64_t m_blobid;

    // The allocated size of the blob; this is the size, which is used
    // by the blob and it's header and maybe additional padding
    ham_u64_t m_allocated_size;

    // The "real" size of the blob (excluding the header)
    ham_u64_t m_size;
} HAM_PACK_2;

#include "packstop.h"


#if defined(_MSC_VER) && defined(_CRTDBG_MAP_ALLOC)
#  undef free
#endif

// The BlobManager manages blobs (not a surprise)
//
// This is an abstract baseclass, derived for In-Memory- and Disk-based
// Environments.
class BlobManager
{
  public:
    BlobManager(LocalEnvironment *env)
      : m_env(env), m_blob_total_allocated(0), m_blob_total_read(0),
        m_blob_direct_read(0), m_blob_direct_written(0),
        m_blob_direct_allocated(0) {
    }

    virtual ~BlobManager() { }

    // Allocates/create a new blob.
    // This function returns the blob-id (the start address of the blob
    // header)
    virtual ham_u64_t allocate(LocalDatabase *db, ham_record_t *record,
                    ham_u32_t flags) = 0;

    // Reads a blob and stores the data in @a record.
    // @ref flags: either 0 or HAM_DIRECT_ACCESS
    virtual void read(LocalDatabase *db, ham_u64_t blob_id,
                    ham_record_t *record, ham_u32_t flags,
                    ByteArray *arena) = 0;

    // Retrieves the size of a blob
    virtual ham_u64_t get_datasize(LocalDatabase *db, ham_u64_t blob_id) = 0;

    // Overwrites an existing blob
    //
    // Will return an error if the blob does not exist. Returns the blob-id
    // (the start address of the blob header)
    virtual ham_u64_t overwrite(LocalDatabase *db, ham_u64_t old_blob_id,
                    ham_record_t *record, ham_u32_t flags) = 0;

    // Deletes an existing blob
    virtual void free(LocalDatabase *db, ham_u64_t blob_id,
                    Page *page = 0, ham_u32_t flags = 0) = 0;

    // Fills in the current metrics
    void get_metrics(ham_env_metrics_t *metrics) const {
      metrics->blob_total_allocated = m_blob_total_allocated;
      metrics->blob_total_read = m_blob_total_read;
      metrics->blob_direct_read = m_blob_direct_read;
      metrics->blob_direct_written = m_blob_direct_written;
      metrics->blob_direct_allocated = m_blob_direct_allocated;
    }

  protected:
    // The Environment which created this BlobManager
    LocalEnvironment *m_env;

    // Usage tracking - number of blobs allocated
    ham_u64_t m_blob_total_allocated;

    // Usage tracking - number of blobs read
    ham_u64_t m_blob_total_read;

    // Usage tracking - number of direct I/O reads
    ham_u64_t m_blob_direct_read;

    // Usage tracking - number of direct I/O writes
    ham_u64_t m_blob_direct_written;

    // Usage tracking - number of direct I/O allocations
    ham_u64_t m_blob_direct_allocated;
};

} // namespace hamsterdb

#endif /* HAM_BLOB_MANAGER_H__ */
