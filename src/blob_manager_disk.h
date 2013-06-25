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

#ifndef HAM_BLOB_MANAGER_DISK_H__
#define HAM_BLOB_MANAGER_DISK_H__

#include "blob_manager.h"
#include "env.h"

namespace hamsterdb {

/*
 * A BlobManager for disk-based databases
 */
class DiskBlobManager : public BlobManager
{
  public:
    DiskBlobManager(Environment *env)
      : BlobManager(env) {
    }

    // allocate/create a blob
    // returns the blob-id (the start address of the blob header) in |blobid|
    ham_status_t allocate(LocalDatabase *db, ham_record_t *record,
                    ham_u32_t flags, ham_u64_t *blobid);

    // reads a blob and stores the data in |record|. The pointer |record.data|
    // is backed by the |arena|, unless |HAM_RECORD_USER_ALLOC| is set.
    // flags: either 0 or HAM_DIRECT_ACCESS
    ham_status_t read(LocalDatabase *db, ham_u64_t blobid,
                    ham_record_t *record, ham_u32_t flags,
                    ByteArray *arena);

    // retrieves the size of a blob
    ham_status_t get_datasize(LocalDatabase *db, ham_u64_t blobid,
                    ham_u64_t *size);

    // overwrite an existing blob
    //
    // will return an error if the blob does not exist
    // returns the blob-id (the start address of the blob header) in |blobid|
    ham_status_t overwrite(LocalDatabase *db, ham_u64_t old_blobid,
                    ham_record_t *record, ham_u32_t flags,
                    ham_u64_t *new_blobid);

    // delete an existing blob
    ham_status_t free(LocalDatabase *db, ham_u64_t blobid,
                    Page *page = 0, ham_u32_t flags = 0);

  private:
    friend class DuplicateManager;

    // write a series of data chunks to storage at file offset 'addr'.
    //
    // The chunks are assumed to be stored in sequential order, adjacent
    // to each other, i.e. as one long data strip.
    //
    // Writing is performed on a per-page basis, where special conditions
    // will decide whether or not the write operation is performed
    // through the page cache or directly to device; such is determined
    // on a per-page basis.
    ham_status_t write_chunks(LocalDatabase *db, Page *page, ham_u64_t addr,
                    bool allocated, bool freshly_created,
                    ham_u8_t **chunk_data, ham_size_t *chunk_size,
                    ham_size_t chunks);

    // same as above, but for reading chunks from the file
    ham_status_t read_chunk(Page *page, Page **fpage, ham_u64_t addr,
                    LocalDatabase *db, ham_u8_t *data, ham_size_t size);

    // if the blob is small enough (or if logging is enabled) then go through
    // the cache. otherwise use direct I/O
    bool blob_from_cache(ham_size_t size) {
      if (m_env->get_log())
        return (size < (m_env->get_usable_pagesize()));
      return (size < (ham_size_t)(m_env->get_pagesize() >> 3));
    }
};

} // namespace hamsterdb

#endif /* HAM_BLOB_MANAGER_DISK_H__ */
