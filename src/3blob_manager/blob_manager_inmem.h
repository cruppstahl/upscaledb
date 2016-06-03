/*
 * Copyright (C) 2005-2016 Christoph Rupp (chris@crupp.de).
 * All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
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
