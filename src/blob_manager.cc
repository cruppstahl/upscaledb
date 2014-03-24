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

  return (do_allocate(db, record, flags));
}

void
BlobManager::read(LocalDatabase *db, ham_u64_t blobid,
                    ham_record_t *record, ham_u32_t flags,
                    ByteArray *arena)
{
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

