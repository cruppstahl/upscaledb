/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 *
 */

#include "config.h"
#include "mem.h"
#include "env_local.h"

#include "blob_manager_inmem.h"

using namespace hamsterdb;


ham_u64_t
InMemoryBlobManager::allocate(LocalDatabase *db, ham_record_t *record,
            ham_u32_t flags)
{
  m_blob_total_allocated++;

  // PARTIAL WRITE
  //
  // if offset+partial_size equals the full record size, then we won't
  // have any gaps. In this case we just write the full record and ignore
  // the partial parameters.
  if (flags & HAM_PARTIAL) {
    if (record->partial_offset == 0 && record->partial_size == record->size)
      flags &= ~HAM_PARTIAL;
  }

  // in-memory-database: the blobid is actually a pointer to the memory
  // buffer, in which the blob (with the blob-header) is stored
  ham_u8_t *p = Memory::allocate<ham_u8_t>(record->size + sizeof(PBlobHeader));

  // initialize the header
  PBlobHeader *blob_header = (PBlobHeader *)p;
  memset(blob_header, 0, sizeof(*blob_header));
  blob_header->set_self((ham_u64_t)PTR_TO_U64(p));
  blob_header->set_alloc_size(record->size + sizeof(PBlobHeader));
  blob_header->set_size(record->size);

  // do we have gaps? if yes, fill them with zeroes
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
    memcpy(p + sizeof(PBlobHeader), record->data, record->size);
  }

  return ((ham_u64_t)PTR_TO_U64(p));
}

void
InMemoryBlobManager::read(LocalDatabase *db, ham_u64_t blobid,
                    ham_record_t *record, ham_u32_t flags,
                    ByteArray *arena)
{
  m_blob_total_read++;

  // in-memory-database: the blobid is actually a pointer to the memory
  // buffer, in which the blob is stored
  PBlobHeader *blob_header = (PBlobHeader *)U64_TO_PTR(blobid);
  ham_u8_t *data = (ham_u8_t *)(U64_TO_PTR(blobid)) + sizeof(PBlobHeader);

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
    ham_u8_t *d = data;
    if (flags & HAM_PARTIAL)
      d += record->partial_offset;

    if ((flags & HAM_DIRECT_ACCESS)
          && !(record->flags & HAM_RECORD_USER_ALLOC)) {
      record->data = d;
    }
    else {
      // resize buffer if necessary
      if (!(record->flags & HAM_RECORD_USER_ALLOC)) {
          arena->resize(blobsize);
          record->data = arena->get_ptr();
      }
      // and copy the data
      memcpy(record->data, d, blobsize);
    }
  }
}

ham_u64_t
InMemoryBlobManager::overwrite(LocalDatabase *db, ham_u64_t old_blobid,
                    ham_record_t *record, ham_u32_t flags)
{
  // PARTIAL WRITE
  //
  // if offset+partial_size equals the full record size, then we won't
  // have any gaps. In this case we just write the full record and ignore
  // the partial parameters.
  if (flags & HAM_PARTIAL) {
    if (record->partial_offset == 0 && record->partial_size == record->size)
      flags &= ~HAM_PARTIAL;
  }

  // free the old blob, allocate a new blob (but if both sizes are equal,
  // just overwrite the data)
  PBlobHeader *phdr = (PBlobHeader *)U64_TO_PTR(old_blobid);

  if (phdr->get_size() == record->size) {
    ham_u8_t *p = (ham_u8_t *)phdr;
    if (flags & HAM_PARTIAL) {
      memmove(p + sizeof(PBlobHeader) + record->partial_offset,
              record->data, record->partial_size);
    }
    else {
      memmove(p + sizeof(PBlobHeader), record->data, record->size);
    }
    return ((ham_u64_t)PTR_TO_U64(phdr));
  }
  else {
    ham_u64_t new_blobid = m_env->get_blob_manager()->allocate(db, record,
            flags);

    Memory::release(phdr);
    return (new_blobid);
  }
}

