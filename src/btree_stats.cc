/*
 * Copyright (C) 2005-2013 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 *
 * Original author: Ger Hobbelt
 */

#include "config.h"

#include <string.h>
#include <stdio.h>

#include "page.h"
#include "btree_stats.h"
#include "btree_index.h"
#include "btree_node_factory.h"

namespace hamsterdb {

BtreeStatistics::BtreeStatistics()
  : m_append_count(0), m_prepend_count(0)
{
  memset(&m_last_leaf_pages[0], 0, sizeof(m_last_leaf_pages));
  memset(&m_last_leaf_count[0], 0, sizeof(m_last_leaf_count));
}

void
BtreeStatistics::find_succeeded(Page *page)
{
  ham_u64_t old = m_last_leaf_pages[kOperationFind];
  if (old != page->get_address()) {
    m_last_leaf_pages[kOperationFind] = 0;
    m_last_leaf_count[kOperationFind] = 0;
  }
  else
    m_last_leaf_count[kOperationFind]++;
}

void
BtreeStatistics::find_failed()
{
  m_last_leaf_pages[kOperationFind] = 0;
  m_last_leaf_count[kOperationFind] = 0;
}

void
BtreeStatistics::insert_succeeded(Page *page, ham_u16_t slot)
{
  ham_u64_t old = m_last_leaf_pages[kOperationInsert];
  if (old != page->get_address()) {
    m_last_leaf_pages[kOperationInsert] = page->get_address();
    m_last_leaf_count[kOperationInsert] = 0;
  }
  else
    m_last_leaf_count[kOperationInsert]++;

  BtreeNodeProxy *node = BtreeNodeFactory::get(page);
  ham_assert(node->is_leaf());
  
  if (!node->get_right() && slot == node->get_count() - 1)
    m_append_count++;
  else
    m_append_count = 0;

  if (!node->get_left() && slot == 0)
    m_prepend_count++;
  else
    m_prepend_count = 0;
}

void
BtreeStatistics::insert_failed()
{
  m_last_leaf_pages[kOperationInsert] = 0;
  m_last_leaf_count[kOperationInsert] = 0;
  m_append_count = 0;
  m_prepend_count = 0;
}

void
BtreeStatistics::erase_succeeded(Page *page)
{
  ham_u64_t old = m_last_leaf_pages[kOperationErase];
  if (old != page->get_address()) {
    m_last_leaf_pages[kOperationErase] = page->get_address();
    m_last_leaf_count[kOperationErase] = 0;
  }
  else
    m_last_leaf_count[kOperationErase]++;
}

void
BtreeStatistics::erase_failed()
{
  m_last_leaf_pages[kOperationErase] = 0;
  m_last_leaf_count[kOperationErase] = 0;
}

void
BtreeStatistics::reset_page(Page *page)
{
  for (int i = 0; i < kOperationMax; i++) {
    m_last_leaf_pages[i] = 0;
    m_last_leaf_count[i] = 0;
  }
}

BtreeStatistics::FindHints
BtreeStatistics::get_find_hints(ham_u32_t flags)
{
  BtreeStatistics::FindHints hints = {flags, flags, 0, false};

  /* if the last 5 lookups hit the same page: reuse that page */
  if (m_last_leaf_count[kOperationFind] >= 5) {
    hints.try_fast_track = true;
    hints.leaf_page_addr = m_last_leaf_pages[kOperationFind];
  }

  return (hints);
}

BtreeStatistics::InsertHints
BtreeStatistics::get_insert_hints(ham_u32_t flags)
{
  InsertHints hints = {flags, flags, 0, 0, 0, 0, 0};

  /* if the previous insert-operation replaced the upper bound (or
   * lower bound) key then it was actually an append (or prepend) operation.
   * in this case there's some probability that the next operation is also
   * appending/prepending.
   */
  if (m_append_count > 0)
    hints.flags |= HAM_HINT_APPEND;
  else if (m_prepend_count > 0)
    hints.flags |= HAM_HINT_PREPEND;

  hints.append_count = m_append_count;
  hints.prepend_count = m_prepend_count;

  /* if the last 5 inserts hit the same page: reuse that page */
  if (m_last_leaf_count[kOperationInsert] >= 5)
    hints.leaf_page_addr = m_last_leaf_pages[kOperationInsert];

  return (hints);
}

} // namespace hamsterdb
