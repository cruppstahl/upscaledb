/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
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

#ifndef HAM_BLOB_MANAGER_INMEM_H
#define HAM_BLOB_MANAGER_INMEM_H

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!
#include "3blob_manager/blob_manager.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

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
    virtual uint64_t do_allocate(Context *context, ham_record_t *record,
                    uint32_t flags);

    // Reads a blob and stores the data in |record|
    // |flags|: either 0 or HAM_DIRECT_ACCESS
    virtual void do_read(Context *context, uint64_t blobid,
                    ham_record_t *record, uint32_t flags,
                    ByteArray *arena);

    // Retrieves the size of a blob
    virtual uint64_t do_get_blob_size(Context *context, uint64_t blobid) {
      PBlobHeader *blob_header = (PBlobHeader *)U64_TO_PTR(blobid);
      return ((uint32_t)blob_header->get_size());
    }

    // Overwrites an existing blob
    //
    // Will return an error if the blob does not exist. Returns the blob-id
    // (the start address of the blob header) 
    virtual uint64_t do_overwrite(Context *context, uint64_t old_blobid,
                    ham_record_t *record, uint32_t flags);

    // Deletes an existing blob
    virtual void do_erase(Context *context, uint64_t blobid,
                    Page *page = 0, uint32_t flags = 0) {
      Memory::release((void *)U64_TO_PTR(blobid));
    }
};

} // namespace hamsterdb

#endif /* HAM_BLOB_MANAGER_INMEM_H */
