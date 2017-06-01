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

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!
#include "1base/dynamic_array.h"
#include "2device/device_inmem.h"
#include "2compressor/compressor.h"
#include "3blob_manager/blob_manager_inmem.h"
#include "4context/context.h"
#include "4db/db_local.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

using namespace upscaledb;

uint64_t
InMemoryBlobManager::allocate(Context *context, ups_record_t *record,
                uint32_t flags)
{
  metric_total_allocated++;

  void *record_data = record->data;
  uint32_t record_size = record->size;
  uint32_t original_size = record->size;

  // compression enabled? then try to compress the data
  Compressor *compressor = context->db->record_compressor.get();
  if (compressor) {
    metric_before_compression += record_size;
    uint32_t len = compressor->compress((uint8_t *)record->data,
                        record->size);
    if (len < record->size) {
      record_data = compressor->arena.data();
      record_size = len;
    }
    metric_after_compression += record_size;
  }

  // in-memory-database: the blobid is actually a pointer to the memory
  // buffer, in which the blob (with the blob-header) is stored
  uint8_t *p = (uint8_t *)device->alloc(record_size + sizeof(PBlobHeader));

  // initialize the header
  PBlobHeader *blob_header = (PBlobHeader *)p;
  blob_header->blob_id = (uint64_t)p;
  blob_header->flags = original_size != record_size
                            ? PBlobHeader::kIsCompressed
                            : 0;
  blob_header->allocated_size = record_size + sizeof(PBlobHeader);
  blob_header->size = original_size;

  // now write the blob data into the allocated memory
  ::memcpy(p + sizeof(PBlobHeader), record_data, record_size);
  return (uint64_t)p;
}

void
InMemoryBlobManager::read(Context *context, uint64_t blobid,
                ups_record_t *record, uint32_t flags,
                ByteArray *arena)
{
  metric_total_read++;

  // the blobid is actually a pointer to the memory buffer in which the
  // blob is stored
  PBlobHeader *blob_header = (PBlobHeader *)blobid;
  uint8_t *blob_data = (uint8_t *)blobid + sizeof(PBlobHeader);
  uint32_t blob_size = (uint32_t)blob_header->size;

  record->size = blob_size;

  // empty blob?
  if (unlikely(blob_size == 0)) {
    record->data = 0;
    record->size = 0;
    return;
  }

  // is the record compressed? if yes then decompress directly in the
  // caller's memory arena to avoid additional memcpys
  if (ISSET(blob_header->flags, PBlobHeader::kIsCompressed)) {
    Compressor *compressor = context->db->record_compressor.get();
    compressor->decompress(blob_data,
                  blob_header->allocated_size - sizeof(PBlobHeader),
                  blob_size, arena);
    record->data = arena->data();
    return;
  }

  // no compression
  if (NOTSET(record->flags, UPS_RECORD_USER_ALLOC)) {
    arena->resize(blob_size);
    record->data = arena->data();
  }
  // and copy the data
  ::memcpy(record->data, blob_data, blob_size);
}

uint64_t
InMemoryBlobManager::overwrite(Context *context, uint64_t old_blobid,
                ups_record_t *record, uint32_t flags)
{
  // This routine basically ignores compression. It is very unlikely that
  // the record size remains identical after the payload was compressed.
  //
  // As a consequence, the existing record is only overwritten if the
  // uncompressed record would fit in. Otherwise a new record is allocated,
  // and this one then is compressed.

  PBlobHeader *phdr = (PBlobHeader *)old_blobid;

  // If the new blob is as large as the old one then just overwrite the
  // data
  if (phdr->allocated_size == record->size + sizeof(PBlobHeader)) {
    uint8_t *p = (uint8_t *)phdr;
    ::memmove(p + sizeof(PBlobHeader), record->data, record->size);
    phdr->flags = 0; // disable compression, just in case
    return (uint64_t)phdr;
  }

  // Otherwise free the old blob and allocate a new one
  uint64_t new_blobid = allocate(context, record, flags);
  InMemoryDevice *imd = (InMemoryDevice *)device;
  imd->release(phdr, (size_t)phdr->allocated_size);
  return new_blobid;
}

uint64_t
InMemoryBlobManager::overwrite_regions(Context *context, uint64_t old_blob_id,
                  ups_record_t *record, uint32_t flags,
                  Region *regions, size_t num_regions)
{
  (void)regions;
  (void)num_regions;
  return overwrite(context, old_blob_id, record, flags);
}
