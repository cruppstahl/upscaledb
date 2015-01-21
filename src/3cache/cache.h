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
 * The Cache Manager
 *
 * Stores pages in a non-intrusive hash table (each Page instance keeps
 * next/previous pointers for the overflow bucket). Can efficiently purge
 * unused pages, because all pages are also stored in a (non-intrusive)
 * linked list, and whenever a page is accessed it is removed and re-inserted
 * at the head. The tail therefore points to the page which was not used
 * in a long time, and is the primary candidate for purging.
 *
 * @exception_safe: nothrow
 * @thread_safe: yes
 */

#ifndef HAM_CACHE_H
#define HAM_CACHE_H

#include "0root/root.h"

#include <vector>

#include "ham/hamsterdb_int.h"

// Always verify that a file of level N does not include headers > N!
#include "3cache/cache_state.h"
#include "3cache/cache_impl.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

struct Cache
{
  // The default constructor
  Cache(CacheState state)
    : m_state(state) {
  }

  // Fills in the current metrics
  void fill_metrics(ham_env_metrics_t *metrics) const {
    CacheImpl::fill_metrics(m_state, metrics);
  }

  // Retrieves a page from the cache, also removes the page from the cache
  // and re-inserts it at the front. Returns null if the page was not cached.
  Page *get(uint64_t address, uint32_t flags = 0) {
    return (CacheImpl::get(m_state, address, flags));
  }

  // Stores a page in the cache
  void put(Page *page) {
    CacheImpl::put(m_state, page);
  }

  // Removes a page from the cache
  void del(Page *page) {
    CacheImpl::del(m_state, page);
  }

  // Purges the cache. Whenever |PurgeSelector()| returns true, the page is
  // forwarded to the |purger()|.
  //
  // Tries to purge at least 20 pages. In benchmarks this has proven to
  // be a good limit.
  template<typename Purger>
  void purge(Purger &purger) {
    CacheImpl::purge<Purger>(m_state, purger);
  }

  // Visits all pages in the "totallist". If |cb| returns true then the
  // page is removed and deleted. This is used by the Environment
  // to flush (and delete) pages.
  template<typename Purger>
  void purge_if(Purger &purger) {
    CacheImpl::purge_if<Purger>(m_state, purger);
  }

  // Returns the capacity (in bytes)
  size_t capacity() const {
    return (CacheImpl::capacity(m_state));
  }

  // Returns the number of currently cached elements
  size_t current_elements() const {
    return (CacheImpl::current_elements(m_state));
  }

  // Returns the number of currently cached elements (excluding those that
  // are mmapped)
  size_t allocated_elements() const {
    return (CacheImpl::allocated_elements(m_state));
  }

  // The mutable state
  CacheState m_state;
};

} // namespace hamsterdb

#endif /* HAM_CACHE_H */
