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

