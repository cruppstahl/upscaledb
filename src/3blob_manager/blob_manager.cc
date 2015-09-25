/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
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
#include "3blob_manager/blob_manager.h"
#include "4context/context.h"
#include "4db/db_local.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

using namespace hamsterdb;

uint64_t
BlobManager::allocate(Context *context, ups_record_t *record,
            uint32_t flags)
{
  // PARTIAL WRITE

  if (flags & UPS_PARTIAL) {
    // Partial updates are not allowed if the records are compressed
    if (context->db && context->db->get_record_compressor()) {
      ups_trace(("Partial operations are not allowed if records "
                              "are compressed"));
      throw Exception(UPS_INV_PARAMETER);
    }
    // if offset + partial_size equals the full record size then there won't
    // be any gaps. In this case we just write the full record and ignore
    // the partial parameters.
    if (record->partial_offset == 0 && record->partial_size == record->size)
      flags &= ~UPS_PARTIAL;
  }

  m_metric_total_allocated++;

  return (do_allocate(context, record, flags));
}

void
BlobManager::read(Context *context, uint64_t blobid, ups_record_t *record,
                uint32_t flags, ByteArray *arena)
{
  // PARTIAL READ
  if (flags & UPS_PARTIAL) {
    // Partial updates are not allowed if the records are compressed
    if (context->db && context->db->get_record_compressor()) {
      ups_trace(("Partial operations are not allowed if records "
                              "are compressed"));
      throw Exception(UPS_INV_PARAMETER);
    }
  }
  m_metric_total_read++;

  return (do_read(context, blobid, record, flags, arena));
}

uint64_t
BlobManager::overwrite(Context *context, uint64_t old_blobid,
                ups_record_t *record, uint32_t flags)
{
  // PARTIAL WRITE
  if (flags & UPS_PARTIAL) {
    // Partial updates are not allowed if the records are compressed
    if (context->db && context->db->get_record_compressor()) {
      ups_trace(("Partial operations are not allowed if records "
                              "are compressed"));
      throw Exception(UPS_INV_PARAMETER);
    }
    // if offset+partial_size equals the full record size, then we won't
    // have any gaps. In this case we just write the full record and ignore
    // the partial parameters.
    if (record->partial_offset == 0 && record->partial_size == record->size)
      flags &= ~UPS_PARTIAL;
  }

  return (do_overwrite(context, old_blobid, record, flags));
}

uint64_t
BlobManager::get_blob_size(Context *context, uint64_t blob_id)
{
  return (do_get_blob_size(context, blob_id));
}

void
BlobManager::erase(Context *context, uint64_t blob_id, Page *page,
                uint32_t flags)
{
  return (do_erase(context, blob_id, page, flags));
}

