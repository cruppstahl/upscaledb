/*
 * Copyright (C) 2005-2017 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * See the file COPYING for License information.
 */

#ifndef UPS_BLOB_MANAGER_INMEM_H
#define UPS_BLOB_MANAGER_INMEM_H

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!
#include "3blob_manager/blob_manager.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

/**
 * A BlobManager for in-memory blobs
 */
struct InMemoryBlobManager : public BlobManager {
  InMemoryBlobManager(const EnvConfig *config,
                  PageManager *page_manager, Device *device)
    : BlobManager(config, page_manager, device) {
  }

  // Allocates/create a new blob
  // This function returns the blob-id (the start address of the blob
  // header)
  virtual uint64_t allocate(Context *context, ups_record_t *record,
                  uint32_t flags);

  // Reads a blob and stores the data in |record|
  // |flags|: either 0 or UPS_DIRECT_ACCESS
  virtual void read(Context *context, uint64_t blobid, ups_record_t *record,
                  uint32_t flags, ByteArray *arena);

  // Retrieves the size of a blob
  virtual uint32_t blob_size(Context *context, uint64_t blobid) {
    PBlobHeader *blob_header = (PBlobHeader *)blobid;
    return blob_header->size;
  }

  // Overwrites an existing blob
  //
  // Will return an error if the blob does not exist. Returns the blob-id
  // (the start address of the blob header) 
  virtual uint64_t overwrite(Context *context, uint64_t old_blobid,
                  ups_record_t *record, uint32_t flags);

  // Overwrites regions of an existing blob
  //
  // Will return an error if the blob does not exist. Returns the blob-id
  // (the start address of the blob header)
  virtual uint64_t overwrite_regions(Context *context, uint64_t old_blob_id,
                  ups_record_t *record, uint32_t flags,
                  Region *regions, size_t num_regions);

  // Deletes an existing blob
  virtual void erase(Context *context, uint64_t blobid, Page *page = 0,
                  uint32_t flags = 0) {
    Memory::release((void *)blobid);
  }
};

} // namespace upscaledb

#endif /* UPS_BLOB_MANAGER_INMEM_H */
