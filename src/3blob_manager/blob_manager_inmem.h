/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
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
    virtual uint64_t do_allocate(LocalDatabase *db, ham_record_t *record,
                    uint32_t flags);

    // Reads a blob and stores the data in |record|
    // |flags|: either 0 or HAM_DIRECT_ACCESS
    virtual void do_read(LocalDatabase *db, uint64_t blobid,
                    ham_record_t *record, uint32_t flags,
                    ByteArray *arena);

    // Retrieves the size of a blob
    virtual uint64_t do_get_blob_size(LocalDatabase *db, uint64_t blobid) {
      PBlobHeader *blob_header = (PBlobHeader *)U64_TO_PTR(blobid);
      return ((uint32_t)blob_header->get_size());
    }

    // Overwrites an existing blob
    //
    // Will return an error if the blob does not exist. Returns the blob-id
    // (the start address of the blob header) 
    virtual uint64_t do_overwrite(LocalDatabase *db, uint64_t old_blobid,
                    ham_record_t *record, uint32_t flags);

    // Deletes an existing blob
    virtual void do_erase(LocalDatabase *db, uint64_t blobid,
                    Page *page = 0, uint32_t flags = 0) {
      Memory::release((void *)U64_TO_PTR(blobid));
    }
};

} // namespace hamsterdb

#endif /* HAM_BLOB_MANAGER_INMEM_H */
