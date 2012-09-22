/*
 * Copyright (C) 2005-2012 Christoph Rupp (chris@crupp.de).
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

#include "statistics.h"
#include "btree.h"
#include "btree_cursor.h"
#include "cursor.h"
#include "cache.h"
#include "db.h"
#include "endianswap.h"
#include "env.h"
#include "error.h"
#include "freelist_statistics.h"
#include "mem.h"
#include "page.h"
#include "btree_stats.h"
#include "util.h"
#include "btree_node.h"

namespace ham {


BtreeStatistics::BtreeStatistics(Database *db)
  : m_db(db)
{
}

void
BtreeStatistics::find_succeeded(Page *page)
{
  ham_offset_t old = m_perf_data.last_leaf_pages[HAM_OPERATION_STATS_FIND];
  if (old != page->get_self()) {
    m_perf_data.last_leaf_pages[HAM_OPERATION_STATS_FIND] = 0;
    m_perf_data.last_leaf_count[HAM_OPERATION_STATS_FIND] = 0;
  }
  else
    m_perf_data.last_leaf_count[HAM_OPERATION_STATS_FIND]++;
}

void
BtreeStatistics::find_failed()
{
  m_perf_data.last_leaf_pages[HAM_OPERATION_STATS_FIND] = 0;
  m_perf_data.last_leaf_count[HAM_OPERATION_STATS_FIND] = 0;
}

void
BtreeStatistics::insert_succeeded(Page *page, ham_u16_t slot)
{
  ham_offset_t old = m_perf_data.last_leaf_pages[HAM_OPERATION_STATS_INSERT];
  if (old != page->get_self()) {
    m_perf_data.last_leaf_pages[HAM_OPERATION_STATS_INSERT] = 0;
    m_perf_data.last_leaf_count[HAM_OPERATION_STATS_INSERT] = 0;
  }
  else
    m_perf_data.last_leaf_count[HAM_OPERATION_STATS_INSERT]++;

  BtreeNode *node = BtreeNode::from_page(page);
  ham_assert(node->is_leaf());
  
  if (!node->get_right() && slot == node->get_count() - 1)
    m_perf_data.append_count++;
  else
    m_perf_data.append_count = 0;

  if (!node->get_left() && slot == 0)
    m_perf_data.prepend_count++;
  else
    m_perf_data.prepend_count = 0;
}

void
BtreeStatistics::insert_failed()
{
  m_perf_data.last_leaf_pages[HAM_OPERATION_STATS_INSERT] = 0;
  m_perf_data.last_leaf_count[HAM_OPERATION_STATS_INSERT] = 0;
  m_perf_data.append_count = 0;
  m_perf_data.prepend_count = 0;
}

void
BtreeStatistics::erase_succeeded(Page *page)
{
  ham_offset_t old = m_perf_data.last_leaf_pages[HAM_OPERATION_STATS_ERASE];
  if (old != page->get_self()) {
    m_perf_data.last_leaf_pages[HAM_OPERATION_STATS_ERASE] = 0;
    m_perf_data.last_leaf_count[HAM_OPERATION_STATS_ERASE] = 0;
  }
  else
    m_perf_data.last_leaf_count[HAM_OPERATION_STATS_ERASE]++;
}

void
BtreeStatistics::erase_failed()
{
  m_perf_data.last_leaf_pages[HAM_OPERATION_STATS_ERASE] = 0;
  m_perf_data.last_leaf_count[HAM_OPERATION_STATS_ERASE] = 0;
}

void
BtreeStatistics::reset_page(Page *page)
{
  for (int i = 0; i <= HAM_OPERATION_STATS_MAX; i++) {
    m_perf_data.last_leaf_pages[i] = 0;
    m_perf_data.last_leaf_count[i] = 0;
  }
}

BtreeStatistics::FindHints
BtreeStatistics::get_find_hints(ham_u32_t flags)
{
  BtreeStatistics::FindHints hints = {flags, flags, 0, false};

  /* if the last 5 lookups hit the same page: reuse that page */
  if (m_perf_data.last_leaf_count[HAM_OPERATION_STATS_FIND] >= 5) {
    hints.try_fast_track = true;
    hints.leaf_page_addr = m_perf_data.last_leaf_pages[HAM_OPERATION_STATS_FIND];
  }

  return (hints);
}

BtreeStatistics::InsertHints
BtreeStatistics::get_insert_hints(ham_u32_t flags)
{
  InsertHints hints = {flags, flags, 0, false, 0};

  /* if the previous insert-operation replaced the upper bound (or
   * lower bound) key then it was actually an append (or prepend) operation.
   * in this case there's some probability that the next operation is also
   * appending/prepending.
   */
  if (m_perf_data.append_count > 0)
    hints.flags |= HAM_HINT_APPEND;
  else if (m_perf_data.prepend_count > 0)
    hints.flags |= HAM_HINT_PREPEND;

  hints.append_count = m_perf_data.append_count;
  hints.prepend_count = m_perf_data.prepend_count;

  /* if the last 5 inserts hit the same page: reuse that page */
  if (m_perf_data.last_leaf_count[HAM_OPERATION_STATS_INSERT] >= 5)
    hints.leaf_page_addr = m_perf_data.last_leaf_pages[HAM_OPERATION_STATS_INSERT];

  return (hints);
}

} // namespace ham
