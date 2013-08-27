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

#ifndef HAM_BTREE_NODE_FACTORY_H__
#define HAM_BTREE_NODE_FACTORY_H__

#include "db.h"
#include "page.h"
#include "btree_node_proxy.h"

namespace hamsterdb {

//
// A BtreeNodeFactory creates BtreeNodeProxy objects depending on the
// Database configuration
//
struct BtreeNodeFactory
{
  static BtreeNodeProxy *get(Page *page) {
    if (page->get_node_proxy())
      return (page->get_node_proxy());

    LocalDatabase *db = page->get_db();
    ham_u32_t dbflags = db->get_rt_flags();

    BtreeNodeProxy *proxy = 0;

    // Callback function provided by user?
    if (db->get_compare_func()) {
        proxy = new BtreeNodeProxyImpl<LegacyNodeLayout,
                        CallbackCompare>(page);
    }
    // Record number database
    else if (dbflags & HAM_RECORD_NUMBER) {
      proxy = new BtreeNodeProxyImpl<LegacyNodeLayout,
                      RecordNumberCompare>(page);
    }
    // Variable keys with non-constant size (not extended)
    // This is the default!
    //   - HAM_PARAM_KEYSIZE to specify the size limit
    else if ((dbflags & HAM_DISABLE_VARIABLE_KEYS) == 0
            && (dbflags & HAM_ENABLE_EXTENDED_KEYS) == 0) {
      proxy = new BtreeNodeProxyImpl<LegacyNodeLayout,
                      VariableSizeCompare>(page);
    }
    // Fixed keys with constant size (not extended)
    //   - HAM_DISABLE_VARIABLE_KEYS
    //   - HAM_PARAM_KEYSIZE to specify the constant size
    else if (dbflags & HAM_DISABLE_VARIABLE_KEYS
            && (dbflags & HAM_ENABLE_EXTENDED_KEYS) == 0) {
      proxy = new BtreeNodeProxyImpl<LegacyNodeLayout,
                      FixedSizeCompare>(page);
    }
    // Fixed keys with constant size (extended)
    //   - HAM_DISABLE_VARIABLE_KEYS
    //   - HAM_ENABLE_EXTENDED_KEYS
    else if (dbflags & HAM_DISABLE_VARIABLE_KEYS
            && dbflags & HAM_ENABLE_EXTENDED_KEYS) {
      proxy = new BtreeNodeProxyImpl<LegacyNodeLayout,
                      FixedSizeCompare>(page);
    }
    // Variable keys with non-constant size (extended)
    //   - HAM_ENABLE_EXTENDED_KEYS
    else if (dbflags & HAM_ENABLE_EXTENDED_KEYS) {
      proxy = new BtreeNodeProxyImpl<LegacyNodeLayout,
                      VariableSizeCompare>(page);
    }
    else
      ham_assert(!"shouldn't be here");

    page->set_node_proxy(proxy);
    return (proxy);
  }

  static int compare(LocalDatabase *db, const ham_key_t *lhs,
                  const ham_key_t *rhs) {
    ham_u32_t dbflags = db->get_rt_flags();

    // Callback-function provided by user?
    if (db->get_compare_func()) {
      CallbackCompare cmp(db);
      return (cmp(lhs->data, lhs->size, rhs->data, rhs->size));
    }

    // Record number database
    if (dbflags & HAM_RECORD_NUMBER) {
      RecordNumberCompare cmp(db);
      return (cmp(lhs->data, lhs->size, rhs->data, rhs->size));
    }
    // Variable keys of any size
    if ((dbflags & HAM_DISABLE_VARIABLE_KEYS) == 0) {
      VariableSizeCompare cmp(db);
      return (cmp(lhs->data, lhs->size, rhs->data, rhs->size));
    }
    // Fixed keys with constant size
    else {
      FixedSizeCompare cmp(db);
      return (cmp(lhs->data, lhs->size, rhs->data, rhs->size));
    }

    // Everything else is handled by the variable-sized memcmp
    VariableSizeCompare cmp(db);
    return (cmp(lhs->data, lhs->size, rhs->data, rhs->size));
  }
};

} // namespace hamsterdb

#endif /* HAM_BTREE_NODE_FACTORY_H__ */
