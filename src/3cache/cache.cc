/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
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

#include <vector>

#include "ham/hamsterdb_int.h"

// Always verify that a file of level N does not include headers > N!
#include "3cache/cache.h"
#include "4context/context.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

// Calculates the hash of a page address
static inline size_t
calc_hash(uint64_t value)
{
  return ((size_t)(value % CacheState::kBucketSize));
}

void
Cache::del_unlocked(Page *page)
{
  ham_assert(page->get_address() != 0);
  size_t hash = calc_hash(page->get_address());
  /* remove the page from the cache buckets */
  m_state.buckets[hash].del(page);

  /* remove it from the list of all cached pages */
  if (m_state.totallist.del(page) && page->is_allocated())
    m_state.alloc_elements--;
}

void
Cache::fill_metrics(ham_env_metrics_t *metrics) const
{
  metrics->cache_hits = m_state.cache_hits;
  metrics->cache_misses = m_state.cache_misses;
}

Page *
Cache::get(uint64_t address)
{
  size_t hash = calc_hash(address);

  ScopedSpinlock lock(m_state.mutex);

  Page *page = m_state.buckets[hash].get(address);;
  if (!page) {
    m_state.cache_misses++;
    return (0);
  }

  // Now re-insert the page at the head of the "totallist", and
  // thus move far away from the tail. The pages at the tail are highest
  // candidates to be deleted when the cache is purged.
  m_state.totallist.del(page);
  m_state.totallist.put(page);
  m_state.cache_hits++;
  return (page);
}

void
Cache::put(Page *page)
{
  size_t hash = calc_hash(page->get_address());
  ham_assert(page->get_data());

  ScopedSpinlock lock(m_state.mutex);

  /* First remove the page from the cache, if it's already cached
   *
   * Then re-insert the page at the head of the list. The tail will
   * point to the least recently used page.
   */
  m_state.totallist.del(page);
  m_state.totallist.put(page);

  if (page->is_allocated())
    m_state.alloc_elements++;
  m_state.buckets[hash].put(page);
}

void
Cache::del(Page *page)
{
  ScopedSpinlock lock(m_state.mutex);
  del_unlocked(page);
}

size_t
Cache::current_elements()
{
  ScopedSpinlock lock(m_state.mutex);
  return (m_state.totallist.size());
}

size_t
Cache::capacity() const
{
  return (m_state.capacity_bytes);
}

size_t
Cache::allocated_elements() const
{
  return (m_state.alloc_elements);
}

} // namespace hamsterdb
