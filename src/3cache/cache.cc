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

// Always verify that a file of level N does not include headers > N!
#include "3cache/cache.h"
#include "4env/env_local.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

struct PageCollectionPurgeIfCallback
{
  PageCollectionPurgeIfCallback(Cache *cache, Cache::PurgeIfCallback cb,
                  LocalEnvironment *env, LocalDatabase *db, uint32_t flags)
    : m_cache(cache), m_cb(cb), m_env(env), m_db(db), m_flags(flags) {
  }

  void prepare(size_t size) {
  }

  bool operator()(Page *page) {
    if (m_cb(page, m_env, m_db, m_flags)) {
      m_cache->remove_page_unlocked(page);
      delete page;
    }
    return (false); // don't remove page from list; it was already removed above
  }

  Cache *m_cache;
  Cache::PurgeIfCallback m_cb;
  LocalEnvironment *m_env;
  LocalDatabase *m_db;
  uint32_t m_flags;
};

void
Cache::purge_if(PurgeIfCallback cb, LocalEnvironment *env, LocalDatabase *db,
                    uint32_t flags)
{
  PageCollectionPurgeIfCallback visitor(this, cb, env, db, flags);

  ScopedSpinlock lock(m_mutex);
  m_totallist.extract(visitor);
}

struct PageCollectionPurgeCallback
{
  PageCollectionPurgeCallback(LocalEnvironment *env)
    : m_env(env) {
  }

  // Returns true if the page can be purged: page must use allocated
  // memory instead of an mmapped pointer; page must not be in use (= in
  // a changeset) and not have cursors attached
  bool operator()(Page *page) {
    if (page->is_allocated() == false)
      return (false);
    if (m_env->get_changeset().contains(page))
      return (false);
    if (page->get_cursor_list() != 0)
      return (false);
    return (true);
  }

  LocalEnvironment *m_env;
};

void
Cache::purge(Cache::PurgeCallback cb, PageManager *pm)
{
  const size_t kPurgeAtLeast = 20;

  // By default this is capped to |kPurgeAtLeast| pages to avoid I/O spikes.
  // In benchmarks this has proven to be a good limit.
  // TODO TODO TODO
  // This comment is wrong. The code purges *AT LEAST* 20 pages, not *MAX*
  // 20 pages!
  size_t max_pages = get_capacity() / m_env->get_page_size();
  if (max_pages == 0)
    max_pages = 1;
  size_t limit = get_current_elements() - max_pages;
  if (limit < kPurgeAtLeast)
    limit = kPurgeAtLeast;

  PageCollectionPurgeCallback visitor(m_env);
  for (size_t i = 0; i < limit; i++) {
    // The lock is permanently re-acquired so other threads don't starve
    ScopedSpinlock lock(m_mutex);
    Page *page = m_totallist.find_first_reverse(visitor);
    if (!page)
      break;
    remove_page_unlocked(page);
    cb(page, m_env->get_page_manager()); // TODO also in locked context?
  }
}

} // namespace hamsterdb
