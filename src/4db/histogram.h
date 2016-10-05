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

#ifndef UPS_HISTOGRAM_H
#define UPS_HISTOGRAM_H

#include "0root/root.h"

#include <stdlib.h>

// Always verify that a file of level N does not include headers > N!
#include "1base/dynamic_array.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

struct Txn;
struct LocalDb;

/*
 * struct Histogram caches the lowest and greatest key in a database. It's used
 * by the LocalDb to discover whether a newly inserted key is outside of the
 * database, and therefore definitely does not yet exist (this saves a
 * btree lookup when inserting the key.
 *
 * This "histogram" is an indication and not necessarily true. It is updated
 * when a Transaction is running, but the update is not reverted when the
 * Transaction is aborted. It can therefore incorrect results, which are
 * however not problematic for the caller (it can return "The key maybe already
 * exists" although it doesn't, but never the other way around).
 */
struct Histogram {
  Histogram(LocalDb *db_)
    : db(db_) {
    ::memset(&lower, 0, sizeof(lower));
    ::memset(&upper, 0, sizeof(upper));
  }

  // compares key to the lower key, returns true if it's lower
  bool test_if_lower(Txn *txn, ups_key_t *key);

  // compares key to the lower key, returns true if it's lower, and updates
  // the cached key
  bool test_and_update_if_lower(Txn *txn, ups_key_t *key);

  // compares key to the upper key, returns true if it's greater
  bool test_if_greater(Txn *txn, ups_key_t *key);

  // compares key to the upper key, returns true if it's greater, and updates
  // the cached key
  bool test_and_update_if_greater(Txn *txn, ups_key_t *key);

  // resets the stored key(s) if it's equal to |key|. Used when deleting
  // keys
  void reset_if_equal(ups_key_t *key);

  // the database (used to fetch and compare keys)
  LocalDb *db;

  // memory arena which backs the 'lower' key
  ByteArray lower_arena;

  // memory arena which backs the 'upper' key
  ByteArray upper_arena;

  // the lower boundary key
  ups_key_t lower;

  // the upper boundary key
  ups_key_t upper;
};

} // namespace upscaledb

#endif // UPS_HISTOGRAM_H


