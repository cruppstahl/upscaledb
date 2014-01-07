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

#ifndef HAM_BLOB_MANAGER_INMEM_H__
#define HAM_BLOB_MANAGER_INMEM_H__

#include "db.h"
#include "blob_manager.h"

namespace hamsterdb {

/**
 * A BlobManager for in-memory blobs
 */
class InMemoryBlobManager : public BlobManager {
  public:
    InMemoryBlobManager(LocalEnvironment *env)
      : BlobManager(env) {
    }

    // Allocates/create a new blob
    // This function returns the blob-id (the start address of the blob
    // header)
    ham_u64_t allocate(LocalDatabase *db, ham_record_t *record,
                    ham_u32_t flags);

    // Reads a blob and stores the data in |record|
    // |flags|: either 0 or HAM_DIRECT_ACCESS
    void read(LocalDatabase *db, ham_u64_t blobid,
                    ham_record_t *record, ham_u32_t flags,
                    ByteArray *arena);

    // Retrieves the size of a blob
    ham_u64_t get_datasize(LocalDatabase *db, ham_u64_t blobid) {
      PBlobHeader *blob_header = (PBlobHeader *)U64_TO_PTR(blobid);
      return ((ham_u32_t)blob_header->get_size());
    }

    // Overwrites an existing blob
    //
    // Will return an error if the blob does not exist. Returns the blob-id
    // (the start address of the blob header) 
    ham_u64_t overwrite(LocalDatabase *db, ham_u64_t old_blobid,
                    ham_record_t *record, ham_u32_t flags);

    // Deletes an existing blob
    void free(LocalDatabase *db, ham_u64_t blobid,
                    Page *page = 0, ham_u32_t flags = 0) {
      Memory::release((void *)U64_TO_PTR(blobid));
    }
};

} // namespace hamsterdb

#endif /* HAM_BLOB_MANAGER_INMEM_H__ */
