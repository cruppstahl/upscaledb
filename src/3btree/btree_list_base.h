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

/*
 * Base class for KeyLists
 */

#ifndef UPS_BTREE_LIST_BASE_H
#define UPS_BTREE_LIST_BASE_H

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

struct BaseList {
  BaseList(LocalDb *db_, PBtreeNode *node_)
    : db(db_), node(node_), range_size(0) {
  }

  // Checks the integrity of this node. Throws an exception if there is a
  // violation.
  void check_integrity(Context *context, size_t node_count) const {
  }

  // Rearranges the list
  void vacuumize(size_t node_count, bool force) const {
  }

  // The Database
  LocalDb *db;

  // The node which stores this list
  PBtreeNode *node;

  // The size of the range (in bytes)
  uint32_t range_size;
};

} // namespace upscaledb

#endif // UPS_BTREE_LIST_BASE_H
