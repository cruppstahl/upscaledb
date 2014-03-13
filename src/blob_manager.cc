/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
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

#include "config.h"
#include "blob_manager.h"

using namespace hamsterdb;


ham_u64_t
BlobManager::allocate(LocalDatabase *db, ham_record_t *record,
            ham_u32_t flags)
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

  m_metric_total_allocated++;

  return (do_allocate(db, record, flags));
}

void
BlobManager::read(LocalDatabase *db, ham_u64_t blobid,
                    ham_record_t *record, ham_u32_t flags,
                    ByteArray *arena)
{
  m_metric_total_read++;

  return (do_read(db, blobid, record, flags, arena));
}

ham_u64_t
BlobManager::overwrite(LocalDatabase *db, ham_u64_t old_blobid,
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

  return (do_overwrite(db, old_blobid, record, flags));
}

ham_u64_t
BlobManager::get_blob_size(LocalDatabase *db, ham_u64_t blob_id)
{
  return (do_get_blob_size(db, blob_id));
}

void
BlobManager::erase(LocalDatabase *db, ham_u64_t blob_id, Page *page,
                    ham_u32_t flags)
{
  return (do_erase(db, blob_id, page, flags));
}

