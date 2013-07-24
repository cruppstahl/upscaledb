/*
 * Copyright (C) 2005-2013 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

#ifndef HAM_BTREE_ENUM_H__
#define HAM_BTREE_ENUM_H__

#include "btree_index.h"

namespace hamsterdb {

struct PBtreeKey;
struct PBtreeNode;

class BtreeVisitor {
  public:
    enum {
      // Stops the traversal of the current page, continues to next page
      kSkipPage     = 1
    };

    // Virtual destructor - can be overwritten!
    virtual ~BtreeVisitor() {
    }

    // Return true if this visitor should also visit internal nodes, not
    // just the leafs
    virtual bool visit_internal_nodes() const {
      return (false);
    }

    // Called when the traversal algorithm "visits" a key
    virtual ham_status_t item(PBtreeNode *node, PBtreeKey *key) {
      return (0);
    }
};

} // namespace hamsterdb

#endif /* HAM_BTREE_ENUM_H__ */
