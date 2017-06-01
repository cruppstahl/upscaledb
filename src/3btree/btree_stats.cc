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

#include <string.h>
#include <stdio.h>

// Always verify that a file of level N does not include headers > N!
#include "2page/page.h"
#include "3btree/btree_stats.h"
#include "3btree/btree_index.h"
#include "3btree/btree_node_proxy.h"
#include "4db/db_local.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

BtreeStatistics::BtreeStatistics()
{
  ::memset(&state, 0, sizeof(state));
}

void
BtreeStatistics::find_succeeded(Page *page)
{
  if (state.last_leaf_pages[kOperationFind] != page->address()) {
    state.last_leaf_pages[kOperationFind] = page->address();
    state.last_leaf_count[kOperationFind] = 0;
  }
  else
    state.last_leaf_count[kOperationFind]++;
}

void
BtreeStatistics::find_failed()
{
  state.last_leaf_pages[kOperationFind] = 0;
  state.last_leaf_count[kOperationFind] = 0;
}

void
BtreeStatistics::insert_succeeded(Page *page, uint16_t slot)
{
  if (state.last_leaf_pages[kOperationInsert] != page->address()) {
    state.last_leaf_pages[kOperationInsert] = page->address();
    state.last_leaf_count[kOperationInsert] = 0;
  }
  else
    state.last_leaf_count[kOperationInsert]++;

  BtreeNodeProxy *node = page->db()->btree_index->get_node_from_page(page);
  assert(node->is_leaf());
  
  if (!node->right_sibling() && slot == node->length() - 1)
    state.append_count++;
  else
    state.append_count = 0;

  if (!node->left_sibling() && slot == 0)
    state.prepend_count++;
  else
    state.prepend_count = 0;
}

void
BtreeStatistics::insert_failed()
{
  state.last_leaf_pages[kOperationInsert] = 0;
  state.last_leaf_count[kOperationInsert] = 0;
  state.append_count = 0;
  state.prepend_count = 0;
}

void
BtreeStatistics::erase_succeeded(Page *page)
{
  if (state.last_leaf_pages[kOperationErase] != page->address()) {
    state.last_leaf_pages[kOperationErase] = page->address();
    state.last_leaf_count[kOperationErase] = 0;
  }
  else
    state.last_leaf_count[kOperationErase]++;
}

void
BtreeStatistics::erase_failed()
{
  state.last_leaf_pages[kOperationErase] = 0;
  state.last_leaf_count[kOperationErase] = 0;
}

BtreeStatistics::FindHints
BtreeStatistics::find_hints(uint32_t flags)
{
  BtreeStatistics::FindHints hints = {flags, flags, 0, false};

  /* if the last 5 lookups hit the same page: reuse that page */
  if (state.last_leaf_count[kOperationFind] >= 5) {
    hints.try_fast_track = true;
    hints.leaf_page_addr = state.last_leaf_pages[kOperationFind];
  }

  return hints;
}

BtreeStatistics::InsertHints
BtreeStatistics::insert_hints(uint32_t flags)
{
  InsertHints hints = {flags, flags, 0, 0, 0, 0, 0};

  /* if the previous insert-operation replaced the upper bound (or
   * lower bound) key then it was actually an append (or prepend) operation.
   * in this case there's some probability that the next operation is also
   * appending/prepending.
   */
  if (state.append_count > 0)
    hints.flags |= UPS_HINT_APPEND;
  else if (state.prepend_count > 0)
    hints.flags |= UPS_HINT_PREPEND;

  hints.append_count = state.append_count;
  hints.prepend_count = state.prepend_count;

  /* if the last 5 inserts hit the same page: reuse that page */
  if (state.last_leaf_count[kOperationInsert] >= 5)
    hints.leaf_page_addr = state.last_leaf_pages[kOperationInsert];

  return hints;
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

} // namespace upscaledb
