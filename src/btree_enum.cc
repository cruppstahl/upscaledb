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

/**
 * @brief btree enumeration
 *
 */

#include "config.h"

#include <string.h>
#include <stdio.h>

#include "btree.h"
#include "db.h"
#include "env.h"
#include "device.h"
#include "error.h"
#include "mem.h"
#include "page.h"
#include "btree_node.h"

namespace hamsterdb {

class BtreeEnumAction
{
  public:
    BtreeEnumAction(BtreeIndex *btree, ham_enumerate_cb_t cb, void *context)
      : m_btree(btree), m_cb(cb), m_context(context) {
      ham_assert(m_btree->get_rootpage() != 0);
      ham_assert(m_cb != 0);
    }

    ham_status_t run() {
      Page *page;
      ham_u32_t level = 0;
      Database *db = m_btree->get_db();
      ham_status_t cb_st = HAM_ENUM_CONTINUE;

      /* get the root page of the tree */
      ham_status_t st = db->fetch_page(&page, m_btree->get_rootpage());
      if (st)
        return (st);

      /* while we found a page... */
      while (page) {
        PBtreeNode *node = PBtreeNode::from_page(page);
        ham_u64_t ptr_left = node->get_ptr_left();
        ham_size_t count = node->get_count();

        st = m_cb(HAM_ENUM_EVENT_DESCEND, (void *)&level,
                (void *)&count, m_context);
        if (st != HAM_ENUM_CONTINUE)
          return (st);

        /* enumerate the page and all its siblings */
        cb_st = enumerate_level(page, level,
                        (cb_st == HAM_ENUM_DO_NOT_DESCEND));
        if (cb_st == HAM_ENUM_STOP || cb_st < 0 /* error */)
          break;

        /* follow the pointer to the smallest child */
        if (ptr_left) {
          st = db->fetch_page(&page, ptr_left);
          if (st)
            return (st);
        }
        else
          page = 0;

        ++level;
      }

      return (cb_st < 0 ? cb_st : HAM_SUCCESS);
    }

  private:
    /**
     * enumerate a whole level in the tree - start with "page" and traverse
     * the linked list of all the siblings
     */
    ham_status_t enumerate_level(Page *page, ham_u32_t level, bool recursive) {
      ham_status_t st;
      ham_status_t cb_st = HAM_ENUM_CONTINUE;

      while (page) {
        /* enumerate the page */
        cb_st = enumerate_page(page, level);
        if (cb_st == HAM_ENUM_STOP || cb_st < 0 /* error */)
          break;

        /* get the right sibling */
        PBtreeNode *node = PBtreeNode::from_page(page);
        if (node->get_right()) {
          st = m_btree->get_db()->fetch_page(&page, node->get_right());
          if (st)
            return (st);
        }
        else
          break;
      }
      return (cb_st);
    }

    /** enumerate a single page */
    ham_status_t enumerate_page(Page *page, ham_u32_t level) {
      Database *db = page->get_db();
      PBtreeNode *node = PBtreeNode::from_page(page);
      ham_status_t cb_st;
      ham_status_t cb_st2;

      bool is_leaf;
      if (node->get_ptr_left())
        is_leaf = false;
      else
        is_leaf = true;

      ham_size_t count = node->get_count();

      cb_st = m_cb(HAM_ENUM_EVENT_PAGE_START, (void *)page, &is_leaf, m_context);
      if (cb_st == HAM_ENUM_STOP || cb_st < 0 /* error */)
        return (cb_st);

      for (ham_size_t i = 0; (i < count) && (cb_st != HAM_ENUM_DO_NOT_DESCEND);
          i++) {
        PBtreeKey *bte = node->get_key(db, i);
        cb_st = m_cb(HAM_ENUM_EVENT_ITEM, (void *)bte, (void *)&count, m_context);
        if (cb_st == HAM_ENUM_STOP || cb_st < 0 /* error */)
          break;
      }

      cb_st2 = m_cb(HAM_ENUM_EVENT_PAGE_STOP, (void *)page, &is_leaf, m_context);

      if (cb_st < 0 /* error */)
        return (cb_st);
      else if (cb_st == HAM_ENUM_STOP)
        return (HAM_ENUM_STOP);
      else
        return (cb_st2);
    }

    BtreeIndex *m_btree;
    ham_enumerate_cb_t m_cb;
    void *m_context;
};

ham_status_t
BtreeIndex::enumerate(ham_enumerate_cb_t cb, void *context)
{
  BtreeEnumAction bea(this, cb, context);
  return (bea.run());
}

} // namespace hamsterdb

