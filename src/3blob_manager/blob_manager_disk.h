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

#ifndef UPS_BLOB_MANAGER_DISK_H
#define UPS_BLOB_MANAGER_DISK_H

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!
#include "3blob_manager/blob_manager.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

#include "1base/packstart.h"

/*
 * The header of a blob page
 *
 * Contains a fixed length freelist and a couter for the number of free
 * bytes
 */
UPS_PACK_0 struct UPS_PACK_1 PBlobPageHeader
{
  enum {
    // Freelist entries
    kFreelistLength = 32
  };

  void initialize() {
    ::memset(this, 0, sizeof(PBlobPageHeader));
  }

  // Returns a PBlobPageHeader from a page
  static PBlobPageHeader *from_page(Page *page) {
    return (PBlobPageHeader *)&page->payload()[0];
  }

  // Number of "regular" pages for this blob; used for blobs exceeding
  // a page size
  uint32_t num_pages;

  // Number of free bytes in this page
  uint32_t free_bytes;

  // The freelist - offset/size pairs in this page
  struct FreelistEntry {
    uint32_t offset;
    uint32_t size;
  } freelist[kFreelistLength];
} UPS_PACK_2;

#include "1base/packstop.h"


/*
 * A BlobManager for disk-based databases
 */
struct DiskBlobManager : public BlobManager
{
  enum {
    // Overhead per page
    kPageOverhead = Page::kSizeofPersistentHeader + sizeof(PBlobPageHeader)
  };

  DiskBlobManager(const EnvConfig *config,
                  PageManager *page_manager, Device *device)
    : BlobManager(config, page_manager, device) {
  }

  // allocate/create a blob
  // returns the blob-id (the start address of the blob header)
  virtual uint64_t allocate(Context *context, ups_record_t *record,
                  uint32_t flags);

  // reads a blob and stores the data in |record|. The pointer |record.data|
  // is backed by the |arena|, unless |UPS_RECORD_USER_ALLOC| is set.
  // flags: either 0 or UPS_DIRECT_ACCESS
  virtual void read(Context *context, uint64_t blobid, ups_record_t *record,
                  uint32_t flags, ByteArray *arena);

  // retrieves the size of a blob
  virtual uint32_t blob_size(Context *context, uint64_t blobid);

  // overwrite an existing blob
  //
  // will return an error if the blob does not exist
  // returns the blob-id (the start address of the blob header) in |blobid|
  virtual uint64_t overwrite(Context *context, uint64_t old_blobid,
                  ups_record_t *record, uint32_t flags);

  // Overwrites regions of an existing blob
  //
  // Will return an error if the blob does not exist. Returns the blob-id
  // (the start address of the blob header)
  virtual uint64_t overwrite_regions(Context *context, uint64_t old_blob_id,
                  ups_record_t *record, uint32_t flags,
                  Region *regions, size_t num_regions);

  // delete an existing blob
  virtual void erase(Context *context, uint64_t blobid,
                  Page *page = 0, uint32_t flags = 0);
};

} // namespace upscaledb

#endif /* UPS_BLOB_MANAGER_DISK_H */
