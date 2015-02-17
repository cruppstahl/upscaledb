/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "0root/root.h"

#include <string.h>
#include <stdio.h>

// Always verify that a file of level N does not include headers > N!
#include "2page/page.h"
#include "3btree/btree_stats.h"
#include "3btree/btree_index.h"
#include "3btree/btree_node_proxy.h"
#include "4db/db_local.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

BtreeStatistics::BtreeStatistics()
  : m_append_count(0), m_prepend_count(0)
{
  memset(&m_last_leaf_pages[0], 0, sizeof(m_last_leaf_pages));
  memset(&m_last_leaf_count[0], 0, sizeof(m_last_leaf_count));
  memset(&m_keylist_range_size[0], 0, sizeof(m_keylist_range_size));
  memset(&m_keylist_capacities[0], 0, sizeof(m_keylist_capacities));
}

void
BtreeStatistics::find_succeeded(Page *page)
{
  uint64_t old = m_last_leaf_pages[kOperationFind];
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
BtreeStatistics::insert_succeeded(Page *page, uint16_t slot)
{
  uint64_t old = m_last_leaf_pages[kOperationInsert];
  if (old != page->get_address()) {
    m_last_leaf_pages[kOperationInsert] = page->get_address();
    m_last_leaf_count[kOperationInsert] = 0;
  }
  else
    m_last_leaf_count[kOperationInsert]++;

  BtreeNodeProxy *node;
  node = page->get_db()->btree_index()->get_node_from_page(page);
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
  uint64_t old = m_last_leaf_pages[kOperationErase];
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
BtreeStatistics::get_find_hints(uint32_t flags)
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
BtreeStatistics::get_insert_hints(uint32_t flags)
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

#define AVG(m)  m._instances ? (m._total / m._instances) : 0

void
BtreeStatistics::finalize_metrics(btree_metrics_t *metrics)
{
  metrics->keys_per_page.avg = AVG(metrics->keys_per_page);
  metrics->keylist_ranges.avg = AVG(metrics->keylist_ranges);
  metrics->recordlist_ranges.avg = AVG(metrics->recordlist_ranges);
  metrics->keylist_index.avg = AVG(metrics->keylist_index);
  metrics->recordlist_index.avg = AVG(metrics->recordlist_index);
  metrics->keylist_unused.avg = AVG(metrics->keylist_unused);
  metrics->recordlist_unused.avg = AVG(metrics->recordlist_unused);
  metrics->keylist_blocks_per_page.avg = AVG(metrics->keylist_blocks_per_page);
  metrics->keylist_block_sizes.avg = AVG(metrics->keylist_block_sizes);
}

} // namespace hamsterdb
