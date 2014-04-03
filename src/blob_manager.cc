/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

