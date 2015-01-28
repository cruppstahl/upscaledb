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

/*
 * Implementation of the Cache Manager
 *
 * @exception_safe: nothrow
 * @thread_safe: yes
 */

#ifndef HAM_CACHE_IMPL_H
#define HAM_CACHE_IMPL_H

#include "0root/root.h"

#include <vector>

#include "ham/hamsterdb_int.h"

// Always verify that a file of level N does not include headers > N!
#include "3cache/cache_state.h"
#include "4context/context.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {
namespace CacheImpl {

void
del_unlocked(CacheState &state, Page *page);

// Returns true if the page can be purged: page must use allocated
// memory instead of an mmapped pointer; page must not be in use (= in
// a changeset) and not have cursors attached
struct PurgeSelector
{
  PurgeSelector(Context *context)
  : m_context(context) {
  }

  bool operator()(Page *page) {
  if (page->is_allocated() == false)
    return (false);
  if (m_context->changeset.has(page))
    return (false);
  if (page->cursor_list() != 0)
    return (false);
  return (true);
  }

  Context *m_context;
};

template<typename Purger>
struct PurgeIfSelector
{
  PurgeIfSelector(CacheState &state, Purger &purger)
    : m_state(state), m_purger(purger) {
  }

  void prepare(size_t size) {
  }

  bool operator()(Page *page) {
    if (m_purger(page)) {
      del_unlocked(m_state, page);
      delete page;
    }
    // don't remove page from list; it was already removed above
    return (false);
  }

  CacheState &m_state;
  Purger &m_purger;
};

// Calculates the hash of a page address
inline size_t
calc_hash(uint64_t value)
{
  return ((size_t)(value % CacheState::kBucketSize));
}

inline void
fill_metrics(const CacheState &state, ham_env_metrics_t *metrics) {
  metrics->cache_hits = state.cache_hits;
  metrics->cache_misses = state.cache_misses;
}

inline Page *
get(CacheState &state, uint64_t address, uint32_t flags = 0)
{
  size_t hash = CacheImpl::calc_hash(address);

  ScopedSpinlock lock(state.mutex);

  Page *page = state.buckets[hash].get(address);;
  if (!page) {
    state.cache_misses++;
    return (0);
  }

  // Now re-insert the page at the head of the "totallist", and
  // thus move far away from the tail. The pages at the tail are highest
  // candidates to be deleted when the cache is purged.
  state.totallist.del(page);
  state.totallist.put(page);
  state.cache_hits++;
  return (page);
}

inline void
put(CacheState &state, Page *page)
{
  size_t hash = calc_hash(page->get_address());
  ham_assert(page->get_data());

  ScopedSpinlock lock(state.mutex);

  /* First remove the page from the cache, if it's already cached
   *
   * Then re-insert the page at the head of the list. The tail will
   * point to the least recently used page.
   */
  state.totallist.del(page);
  state.totallist.put(page);

  if (page->is_allocated())
    state.alloc_elements++;
  state.buckets[hash].put(page);
}

inline void
del_unlocked(CacheState &state, Page *page)
{
  ham_assert(page->get_address() != 0);
  size_t hash = calc_hash(page->get_address());
  /* remove the page from the cache buckets */
  state.buckets[hash].del(page);

  /* remove it from the list of all cached pages */
  if (state.totallist.del(page) && page->is_allocated())
    state.alloc_elements--;
}

inline void
del(CacheState &state, Page *page)
{
  ScopedSpinlock lock(state.mutex);
  del_unlocked(state, page);
}

template<typename Purger>
inline void
purge(CacheState &state, Context *context, Purger &purger)
{
  size_t limit = state.totallist.size()
            - (state.capacity_bytes / state.page_size_bytes);
  if (limit < CacheState::kPurgeAtLeast)
    limit = CacheState::kPurgeAtLeast;

  PurgeSelector selector(context);

  for (size_t i = 0; i < limit; i++) {
    // The lock is permanently re-acquired so other threads don't starve
    ScopedSpinlock lock(state.mutex);
    Page *page = state.totallist.find_first_reverse(selector);
    if (!page)
      break;
    del_unlocked(state, page);
    purger(page);  // TODO also in locked context?
  }
}

template<typename Purger>
inline void
purge_if(CacheState &state, Purger &purger)
{
  PurgeIfSelector<Purger> selector(state, purger);

  ScopedSpinlock lock(state.mutex);
  state.totallist.extract(selector);
}

inline size_t
capacity(const CacheState &state)
{
  return (state.capacity_bytes);
}

inline size_t
current_elements(const CacheState &state)
{
  return (state.totallist.size());
}

inline size_t
allocated_elements(const CacheState &state)
{
  return (state.alloc_elements);
}

}} // namespace hamsterdb::CacheImpl

#endif /* HAM_CACHE_IMPL_H */
