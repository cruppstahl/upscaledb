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
#include "4db/histogram.h"
#include "4db/db_local.h"
#include "4cursor/cursor.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

bool
Histogram::test_if_lower(Txn *txn, ups_key_t *key)
{
  // if key was not initialized: always return false
  if (unlikely(lower.size == 0))
    return false;
  return db->btree_index->compare_keys(key, &lower) < 0;
}

bool
Histogram::test_and_update_if_lower(Txn *txn, ups_key_t *key)
{
  // initialize the key, if required
  if (unlikely(lower.size == 0)) {
    Cursor *cursor = db->cursor_create(txn, 0);
    ups_status_t st = db->cursor_move(cursor, &lower, 0, UPS_CURSOR_FIRST);
    delete cursor;
    if (unlikely(st != 0))
      return false;
    lower_arena.copy((uint8_t *)lower.data, lower.size);
    lower.data = (void *)lower_arena.data();
  }

  // if the new key is lower than |lower|: update the cached key
  if (test_if_lower(txn, key)) {
    lower_arena.copy((uint8_t *)key->data, key->size);
    lower.data = (void *)lower_arena.data();
    lower.size = key->size;
    return true;
  }

  return false;
}

bool
Histogram::test_if_greater(Txn *txn, ups_key_t *key)
{
  // if key was not initialized: always return false
  if (unlikely(upper.size == 0))
    return false;
  return db->btree_index->compare_keys(key, &upper) > 0;
}

bool
Histogram::test_and_update_if_greater(Txn *txn, ups_key_t *key)
{
  // initialize the key, if required
  if (unlikely(upper.size == 0)) {
    Cursor *cursor = db->cursor_create(txn, 0);
    ups_status_t st = db->cursor_move(cursor, &upper, 0, UPS_CURSOR_LAST);
    delete cursor;
    if (unlikely(st != 0))
      return false;
    upper_arena.copy((uint8_t *)upper.data, upper.size);
    upper.data = (void *)upper_arena.data();
  }

  // if the new key is greater than |upper|: update the cached key
  if (test_if_greater(txn, key)) {
    upper_arena.copy((uint8_t *)key->data, key->size);
    upper.data = (void *)upper_arena.data();
    upper.size = key->size;
    return true;
  }

  return false;
}

void
Histogram::reset_if_equal(ups_key_t *key)
{
  if (unlikely(lower.size > 0
                && db->btree_index->compare_keys(&lower, key) == 0))
    ::memset(&lower, 0, sizeof(lower));
  if (unlikely(upper.size > 0
                && db->btree_index->compare_keys(&upper, key) == 0))
    ::memset(&upper, 0, sizeof(upper));
}

} // namespace upscaledb

