/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
 * All Rights Reserved.
 *
 * NOTICE: All information contained herein is, and remains the property
 * of Christoph Rupp and his suppliers, if any. The intellectual and
 * technical concepts contained herein are proprietary to Christoph Rupp
 * and his suppliers and may be covered by Patents, patents in process,
 * and are protected by trade secret or copyright law. Dissemination of
 * this information or reproduction of this material is strictly forbidden
 * unless prior written permission is obtained from Christoph Rupp.
 */

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!
#include "3blob_manager/blob_manager.h"
#include "4db/db_local.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

using namespace hamsterdb;

uint64_t
BlobManager::allocate(Context *context, ham_record_t *record,
            uint32_t flags)
{
  // PARTIAL WRITE
  if (flags & HAM_PARTIAL) {
    // Partial updates are not allowed if the records are compressed
    if (db->get_record_compressor()) {
      ham_trace(("Partial operations are not allowed if records "
                              "are compressed"));
      throw Exception(HAM_INV_PARAMETER);
    }
    // if offset + partial_size equals the full record size then there won't
    // be any gaps. In this case we just write the full record and ignore
    // the partial parameters.
    if (record->partial_offset == 0 && record->partial_size == record->size)
      flags &= ~HAM_PARTIAL;
  }

  m_metric_total_allocated++;

  return (do_allocate(context, record, flags));
}

void
BlobManager::read(Context *context, uint64_t blobid, ham_record_t *record,
                uint32_t flags, ByteArray *arena)
{
  // PARTIAL READ
  if (flags & HAM_PARTIAL) {
    // Partial updates are not allowed if the records are compressed
    if (db->get_record_compressor()) {
      ham_trace(("Partial operations are not allowed if records "
                              "are compressed"));
      throw Exception(HAM_INV_PARAMETER);
    }
  }

  m_metric_total_read++;

  return (do_read(context, blobid, record, flags, arena));
}

uint64_t
BlobManager::overwrite(Context *context, uint64_t old_blobid,
                ham_record_t *record, uint32_t flags)
{
  // PARTIAL WRITE
  if (flags & HAM_PARTIAL) {
    // Partial updates are not allowed if the records are compressed
    if (db->get_record_compressor()) {
      ham_trace(("Partial operations are not allowed if records "
                              "are compressed"));
      throw Exception(HAM_INV_PARAMETER);
    }
    // if offset+partial_size equals the full record size, then we won't
    // have any gaps. In this case we just write the full record and ignore
    // the partial parameters.
    if (record->partial_offset == 0 && record->partial_size == record->size)
      flags &= ~HAM_PARTIAL;
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

