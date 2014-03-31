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

#include "config.h"
#include "mem.h"
#include "db_local.h"
#include "compressor.h"

#include "blob_manager_inmem.h"

using namespace hamsterdb;


ham_u64_t
InMemoryBlobManager::do_allocate(LocalDatabase *db, ham_record_t *record,
            ham_u32_t flags)
{
  void *record_data = record->data;
  ham_u32_t record_size = record->size;
  ham_u32_t original_size = record->size;

  // compression enabled? then try to compress the data
  Compressor *compressor = db->get_record_compressor();
  if (compressor) {
    m_metric_before_compression += record_size;
    ham_u32_t len = compressor->compress((ham_u8_t *)record->data,
                        record->size);
    if (len < record->size) {
      record_data = (void *)compressor->get_output_data();
      record_size = len;
    }
    m_metric_after_compression += record_size;
  }

  // in-memory-database: the blobid is actually a pointer to the memory
  // buffer, in which the blob (with the blob-header) is stored
  ham_u8_t *p = Memory::allocate<ham_u8_t>(record_size + sizeof(PBlobHeader));

  // initialize the header
  PBlobHeader *blob_header = (PBlobHeader *)p;
  memset(blob_header, 0, sizeof(*blob_header));
  blob_header->set_self((ham_u64_t)PTR_TO_U64(p));
  blob_header->set_alloc_size(record_size + sizeof(PBlobHeader));
  blob_header->set_size(original_size);
  blob_header->set_flags(original_size != record_size ? kIsCompressed : 0);

  // do we have gaps? if yes, fill them with zeroes
  //
  // HAM_PARTIAL is not allowed in combination with compression,
  // therefore we do not have to check compression stuff in here.
  if (flags & HAM_PARTIAL) {
    ham_u8_t *s = p + sizeof(PBlobHeader);
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

  return ((ham_u64_t)PTR_TO_U64(p));
}

void
InMemoryBlobManager::do_read(LocalDatabase *db, ham_u64_t blobid,
                    ham_record_t *record, ham_u32_t flags,
                    ByteArray *arena)
{
  // in-memory-database: the blobid is actually a pointer to the memory
  // buffer, in which the blob is stored
  PBlobHeader *blob_header = (PBlobHeader *)U64_TO_PTR(blobid);

  // when the database is closing, the header is already deleted
  if (!blob_header) {
    record->size = 0;
    return;
  }

  ham_u32_t blobsize = (ham_u32_t)blob_header->get_size();
  record->size = blobsize;

  if (flags & HAM_PARTIAL) {
    if (record->partial_offset > blobsize) {
      ham_trace(("partial offset is greater than the total record size"));
      throw Exception(HAM_INV_PARAMETER);
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
    ham_u8_t *data = (ham_u8_t *)(U64_TO_PTR(blobid)) + sizeof(PBlobHeader);
    if (flags & HAM_PARTIAL)
      data += record->partial_offset;

    // is the record compressed? if yes then decompress directly in the
    // caller's memory arena to avoid additional memcpys
    if (blob_header->get_flags() & kIsCompressed) {
      Compressor *compressor = db->get_record_compressor();
      if (!compressor)
        throw Exception(HAM_NOT_READY);

      if (!(record->flags & HAM_RECORD_USER_ALLOC)) {
        compressor->decompress(data,
                      blob_header->get_alloc_size() - sizeof(PBlobHeader),
                      blobsize, arena);
        data = (ham_u8_t *)arena->get_ptr();
      }
      else {
        compressor->decompress(data,
                      blob_header->get_alloc_size() - sizeof(PBlobHeader),
                      blobsize);
        data = (ham_u8_t *)compressor->get_output_data();
      }
      record->data = data;
    }
    else { // no compression
      if ((flags & HAM_DIRECT_ACCESS)
            && !(record->flags & HAM_RECORD_USER_ALLOC)) {
        record->data = data;
      }
      else {
        // resize buffer if necessary
        if (!(record->flags & HAM_RECORD_USER_ALLOC)) {
            arena->resize(blobsize);
            record->data = arena->get_ptr();
        }
        // and copy the data
        memcpy(record->data, data, blobsize);
      }
    }
  }
}

ham_u64_t
InMemoryBlobManager::do_overwrite(LocalDatabase *db, ham_u64_t old_blobid,
                    ham_record_t *record, ham_u32_t flags)
{
  // This routine basically ignores compression. The likelyhood, that a
  // compressed buffer has an identical size as the record that's overwritten,
  // is very small. In most cases this check will be false, and then
  // the record would be compressed again in do_allocate().
  //
  // As a consequence, the existing record is only overwritten if the
  // uncompressed record would fit in. Otherwise a new record is allocated,
  // and this one then is compressed.

  // Free the old blob, allocate a new blob (but if both sizes are equal,
  // just overwrite the data)
  PBlobHeader *phdr = (PBlobHeader *)U64_TO_PTR(old_blobid);

  if (phdr->get_alloc_size() == sizeof(PBlobHeader) + record->size) {
    ham_u8_t *p = (ham_u8_t *)phdr;
    if (flags & HAM_PARTIAL) {
      memmove(p + sizeof(PBlobHeader) + record->partial_offset,
              record->data, record->partial_size);
    }
    else
      memmove(p + sizeof(PBlobHeader), record->data, record->size);
    phdr->set_flags(0); // disable compression, just in case
    return ((ham_u64_t)PTR_TO_U64(phdr));
  }
  else {
    ham_u64_t new_blobid = allocate(db, record, flags);
    Memory::release(phdr);
    return (new_blobid);
  }
}

