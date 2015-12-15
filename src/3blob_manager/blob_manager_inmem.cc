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
InMemoryBlobManager::do_allocate(Context *context, ups_record_t *record,
                uint32_t flags)
{
  void *record_data = record->data;
  uint32_t record_size = record->size;
  uint32_t original_size = record->size;

  // compression enabled? then try to compress the data
  Compressor *compressor = context->db->get_record_compressor();
  if (compressor) {
    m_metric_before_compression += record_size;
    uint32_t len = compressor->compress((uint8_t *)record->data,
                        record->size);
    if (len < record->size) {
      record_data = (void *)compressor->get_output_data();
      record_size = len;
    }
    m_metric_after_compression += record_size;
  }
  // in-memory-database: the blobid is actually a pointer to the memory
  // buffer, in which the blob (with the blob-header) is stored
  uint8_t *p = (uint8_t *)m_device->alloc(record_size + sizeof(PBlobHeader));

  // initialize the header
  PBlobHeader *blob_header = (PBlobHeader *)p;
  memset(blob_header, 0, sizeof(*blob_header));
  blob_header->blob_id = (uint64_t)p;
  blob_header->allocated_size = record_size + sizeof(PBlobHeader);
  blob_header->size = original_size;
  blob_header->flags = (original_size != record_size ? kIsCompressed : 0);

  // do we have gaps? if yes, fill them with zeroes
  //
  // UPS_PARTIAL is not allowed in combination with compression,
  // therefore we do not have to check compression stuff in here.
  if (flags & UPS_PARTIAL) {
    uint8_t *s = p + sizeof(PBlobHeader);
    if (record->partial_offset)
      memset(s, 0, record->partial_offset);
    memcpy(s + record->partial_offset, record->data, record->partial_size);
    if (record->partial_offset + record->partial_size < record->size)
      memset(s + record->partial_offset + record->partial_size, 0,
              record->size - (record->partial_offset + record->partial_size));
  }
  else {
    memcpy(p + sizeof(PBlobHeader), record_data, record_size);
  }

  return ((uint64_t)p);
}

void
InMemoryBlobManager::do_read(Context *context, uint64_t blobid,
                ups_record_t *record, uint32_t flags,
                ByteArray *arena)
{
  // in-memory-database: the blobid is actually a pointer to the memory
  // buffer, in which the blob is stored
  PBlobHeader *blob_header = (PBlobHeader *)blobid;
  uint8_t *data = (uint8_t *)(blobid) + sizeof(PBlobHeader);

  // when the database is closing, the header is already deleted
  if (!blob_header) {
    record->size = 0;
    return;
  }

  uint32_t blobsize = (uint32_t)blob_header->size;
  record->size = blobsize;

  if (flags & UPS_PARTIAL) {
    if (record->partial_offset > blobsize) {
      ups_trace(("partial offset is greater than the total record size"));
      throw Exception(UPS_INV_PARAMETER);
    }
    if (record->partial_offset + record->partial_size > blobsize)
      record->partial_size = blobsize = blobsize - record->partial_offset;
    else
      blobsize = record->partial_size;
  }

  // empty blob?
  if (!blobsize) {
    record->data = 0;
    record->size = 0;
  }
  else {
    if (flags & UPS_PARTIAL)
      data += record->partial_offset;

    // is the record compressed? if yes then decompress directly in the
    // caller's memory arena to avoid additional memcpys
    if (blob_header->flags & kIsCompressed) {
      Compressor *compressor = context->db->get_record_compressor();
      if (!compressor)
        throw Exception(UPS_NOT_READY);

      if (!(record->flags & UPS_RECORD_USER_ALLOC)) {
        compressor->decompress(data,
                      blob_header->allocated_size - sizeof(PBlobHeader),
                      blobsize, arena);
        data = (uint8_t *)arena->get_ptr();
      }
      else {
        compressor->decompress(data,
                      blob_header->allocated_size - sizeof(PBlobHeader),
                      blobsize);
        data = (uint8_t *)compressor->get_output_data();
      }
      record->data = data;
    }
    else { // no compression
    if ((flags & UPS_DIRECT_ACCESS)
          && !(record->flags & UPS_RECORD_USER_ALLOC)) {
        record->data = data;
    }
    else {
      // resize buffer if necessary
      if (!(record->flags & UPS_RECORD_USER_ALLOC)) {
          arena->resize(blobsize);
          record->data = arena->get_ptr();
      }
      // and copy the data
        memcpy(record->data, data, blobsize);
      }
    }
  }
}

uint64_t
InMemoryBlobManager::do_overwrite(Context *context, uint64_t old_blobid,
                ups_record_t *record, uint32_t flags)
{
  // This routine basically ignores compression. It is very unlikely that
  // the record size remains identical after the payload was compressed.
  //
  // As a consequence, the existing record is only overwritten if the
  // uncompressed record would fit in. Otherwise a new record is allocated,
  // and this one then is compressed.

  // Free the old blob, allocate a new blob (but if both sizes are equal,
  // just overwrite the data)
  PBlobHeader *phdr = (PBlobHeader *)old_blobid;

  if (phdr->allocated_size == record->size + sizeof(PBlobHeader)) {
    uint8_t *p = (uint8_t *)phdr;
    if (flags & UPS_PARTIAL) {
      ::memmove(p + sizeof(PBlobHeader) + record->partial_offset,
              record->data, record->partial_size);
    }
    else {
      ::memmove(p + sizeof(PBlobHeader), record->data, record->size);
    }
    phdr->flags = 0; // disable compression, just in case
    return ((uint64_t)phdr);
  }
  else {
    uint64_t new_blobid = allocate(context, record, flags);

    InMemoryDevice *dev = (InMemoryDevice *)m_device;
    dev->release(phdr, (size_t)phdr->allocated_size);
    return (new_blobid);
  }
}

