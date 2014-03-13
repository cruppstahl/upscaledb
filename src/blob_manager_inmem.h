/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 * All Rights Reserved.
 *
 * NOTICE: All information contained herein is, and remains the property
 * of Christoph Rupp and his suppliers, if any. The intellectual and
 * technical concepts contained herein are proprietary to Christoph Rupp
 * and his suppliers and may be covered by Patents, patents in process,
 * and are protected by trade secret or copyright law. Dissemination of
 * this information or reproduction of this material is strictly forbidden
 * unless prior written permission is obtained from Christoph Rupp.
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

  protected:
    // Allocates/create a new blob
    // This function returns the blob-id (the start address of the blob
    // header)
    virtual ham_u64_t do_allocate(LocalDatabase *db, ham_record_t *record,
                    ham_u32_t flags);

    // Reads a blob and stores the data in |record|
    // |flags|: either 0 or HAM_DIRECT_ACCESS
    virtual void do_read(LocalDatabase *db, ham_u64_t blobid,
                    ham_record_t *record, ham_u32_t flags,
                    ByteArray *arena);

    // Retrieves the size of a blob
    virtual ham_u64_t do_get_blob_size(LocalDatabase *db, ham_u64_t blobid) {
      PBlobHeader *blob_header = (PBlobHeader *)U64_TO_PTR(blobid);
      return ((ham_u32_t)blob_header->get_size());
    }

    // Overwrites an existing blob
    //
    // Will return an error if the blob does not exist. Returns the blob-id
    // (the start address of the blob header) 
    virtual ham_u64_t do_overwrite(LocalDatabase *db, ham_u64_t old_blobid,
                    ham_record_t *record, ham_u32_t flags);

    // Deletes an existing blob
    virtual void do_erase(LocalDatabase *db, ham_u64_t blobid,
                    Page *page = 0, ham_u32_t flags = 0) {
      Memory::release((void *)U64_TO_PTR(blobid));
    }
};

} // namespace hamsterdb

#endif /* HAM_BLOB_MANAGER_INMEM_H__ */
