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

/**
 * @brief functions for reading/writing/allocating blobs (memory chunks of
 * arbitrary size)
 *
 */

#ifndef UPS_BLOB_MANAGER_H
#define UPS_BLOB_MANAGER_H

#include "0root/root.h"

#include "ups/upscaledb_int.h"

// Always verify that a file of level N does not include headers > N!
#include "1base/dynamic_array.h"
#include "2page/page.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

struct Device;
struct PageManager;
struct EnvConfig;
struct Context;

#include "1base/packstart.h"

// A blob header structure
//
// This header is prepended to the blob's payload. It holds the blob size and
// the blob's address (which is not required but useful for error checking.)
UPS_PACK_0 struct UPS_PACK_1 PBlobHeader {
  enum {
    // Blob is compressed
    kIsCompressed = 1
  };

  PBlobHeader() {
    ::memset(this, 0, sizeof(PBlobHeader));
  }

  // Returns a PBlobHeader from a file address
  static PBlobHeader *fropage(Page *page, uint64_t address) {
  uint32_t readstart = (uint32_t)(address - page->address());
  return (PBlobHeader *)&page->raw_payload()[readstart];
  }

  // The blob id - which is the absolute address/offset of this
  // structure in the file
  uint64_t blob_id;

  // Flags; store compression information
  uint32_t flags;

  // The allocated size of the blob; this is the size, which is used
  // by the blob and it's header and maybe additional padding
  uint32_t allocated_size;

  // The size of the blob from the user's point of view (excluding the header)
  uint32_t size;

} UPS_PACK_2;

#include "1base/packstop.h"

// The BlobManager manages blobs (not a surprise)
//
// This is an abstract baseclass, derived for In-Memory- and Disk-based
// Environments.
struct BlobManager {
  // A parameter for overwrite_regions()
  struct Region {
    Region() {
    }

    Region(uint32_t offset_, uint32_t size_)
      : offset(offset_), size(size_) {
    }

    uint32_t offset;
    uint32_t size;
  };

  // Flags for allocate(); make sure that they do not conflict with
  // the flags for ups_db_insert()
  enum {
    // Do not compress the blob, even if compression is enabled
    kDisableCompression = 0x10000000
  };

  BlobManager(const EnvConfig *config_, PageManager *page_manager_,
                  Device *device_)
    : config(config_), page_manager(page_manager_), device(device_),
      metric_before_compression(0), metric_after_compression(0),
      metric_total_allocated(0), metric_total_read(0) {
  }

  virtual ~BlobManager() { }

  // Allocates/create a new blob.
  // This function returns the blob-id (the start address of the blob
  // header)
  //
  // |flags| can be kDisableCompression // TODO replace with bool value?
  virtual uint64_t allocate(Context *context, ups_record_t *record,
                  uint32_t flags) = 0;

  // Reads a blob and stores the data in @a record.
  // @ref flags: either 0 or UPS_DIRECT_ACCESS
  virtual void read(Context *context, uint64_t blob_id, ups_record_t *record,
                  uint32_t flags, ByteArray *arena) = 0;

  // Retrieves the size of a blob
  virtual uint32_t blob_size(Context *context, uint64_t blob_id) = 0;

  // Overwrites an existing blob
  //
  // Will return an error if the blob does not exist. Returns the blob-id
  // (the start address of the blob header)
  virtual uint64_t overwrite(Context *context, uint64_t old_blob_id,
                  ups_record_t *record, uint32_t flags) = 0;

  // Overwrites regions of an existing blob
  //
  // Will return an error if the blob does not exist. Returns the blob-id
  // (the start address of the blob header)
  virtual uint64_t overwrite_regions(Context *context, uint64_t old_blob_id,
                  ups_record_t *record, uint32_t flags,
                  Region *regions, size_t num_regions) = 0;

  // Deletes an existing blob
  virtual void erase(Context *context, uint64_t blob_id, Page *page = 0,
                  uint32_t flags = 0) = 0;

  // Fills in the current metrics
  void fill_metrics(ups_env_metrics_t *metrics) const {
    metrics->blob_total_allocated = metric_total_allocated;
    metrics->blob_total_read = metric_total_read;
    metrics->record_bytes_before_compression = metric_before_compression;
    metrics->record_bytes_after_compression = metric_after_compression;
  }

  // The configuration of the Environment
  const EnvConfig *config;

  // The active page manager - required to allocate and fetch pages
  PageManager *page_manager;

  // The device - sometimes it's accessed directly
  Device *device;

  // Usage tracking - number of bytes before compression
  uint64_t metric_before_compression;

  // Usage tracking - number of bytes after compression
  uint64_t metric_after_compression;

  // Usage tracking - number of blobs allocated
  uint64_t metric_total_allocated;

  // Usage tracking - number of blobs read
  uint64_t metric_total_read;
};

} // namespace upscaledb

#endif /* UPS_BLOB_MANAGER_H */
