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

// Always verify that a file of level N does not include headers > N!
#include "1errorinducer/errorinducer.h"
#include "2device/device.h"
#include "2page/page.h"
#include "3changeset/changeset.h"
#include "3journal/journal.h"
#include "3page_manager/page_manager.h"
#include "4db/db.h"
#include "4env/env_local.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

/* a unittest hook for Changeset::flush() */
void (*g_CHANGESET_POST_LOG_HOOK)(void);

namespace Impl {

static Page *
get(ChangesetState &state, uint64_t address)
{
  return (state.collection.get(address));
}

static void
del(ChangesetState &state, Page *page)
{
  page->mutex().unlock();
  state.collection.del(page);
}

static bool
has(const ChangesetState &state, Page *page)
{
  return (state.collection.has(page));
}

static int counter = 0;
static void
put(ChangesetState &state, Page *page)
{
  if (!has(state, page)) {
    if (page->get_address() == 66560)
      counter++;
    page->mutex().lock();
  }
  state.collection.put(page);
}

static bool
is_empty(const ChangesetState &state)
{
  return (state.collection.is_empty());
}

struct UnlockPage
{
  void operator()(Page *page) {
    page->mutex().try_lock();
    page->mutex().unlock();
  }
};

static void
clear(ChangesetState &state)
{
  UnlockPage unlocker;
  state.collection.for_each(unlocker);

  state.collection.clear();
}

struct PageCollectionVisitor
{
  PageCollectionVisitor()
    : num_pages(0), pages(0) {
  }

  ~PageCollectionVisitor() {
    Memory::release(pages);
  }

  void prepare(size_t size) {
    pages = Memory::allocate<Page *>(sizeof(Page *) * size);
  }

  bool operator()(Page *page) {
    if (page->is_dirty() == true) {
      pages[num_pages] = page;
      ++num_pages;
    }
    // |page| is now removed from the Changeset
    page->mutex().unlock();
    return (true);
  }

  int num_pages;
  Page **pages;
};

static void
flush(ChangesetState &state, uint64_t lsn)
{
  // now flush all modified pages to disk
  if (state.collection.is_empty())
    return;
  
  HAM_INDUCE_ERROR(ErrorInducer::kChangesetFlush);

  // Fetch the pages, ignoring all pages that are not dirty
  PageCollectionVisitor visitor;
  state.collection.extract(visitor);

  // TODO sort by address (really?)

  if (visitor.num_pages == 0)
    return;

  // If only one page is modified then the modification is atomic. The page
  // is written to the btree (no log required).
  //
  // If more than one page is modified then the modification is no longer
  // atomic. All dirty pages are written to the log.
  if (visitor.num_pages > 1) {
    state.env->journal()->append_changeset((const Page **)visitor.pages,
                        visitor.num_pages, lsn);
  }

  HAM_INDUCE_ERROR(ErrorInducer::kChangesetFlush);

  /* execute a post-log hook; this hook is set by the unittest framework
   * and can be used to make a backup copy of the logfile */
  if (g_CHANGESET_POST_LOG_HOOK)
    g_CHANGESET_POST_LOG_HOOK();

  /* now write all the pages to the file; if any of these writes fail,
   * we can still recover from the log */
  for (int i = 0; i < visitor.num_pages; i++) {
    Page *p = visitor.pages[i];
    if (p->is_without_header() == false)
      p->set_lsn(lsn);
    p->flush();

    HAM_INDUCE_ERROR(ErrorInducer::kChangesetFlush);
  }

  /* flush the file handle (if required) */
  if (state.env->get_flags() & HAM_ENABLE_FSYNC)
    state.env->device()->flush();

  HAM_INDUCE_ERROR(ErrorInducer::kChangesetFlush);
}

} // namespace Impl

Page *
Changeset::get(uint64_t address)
{
  return (Impl::get(m_state, address));
}

void
Changeset::del(Page *page)
{
  Impl::del(m_state, page);
}

void
Changeset::put(Page *page)
{
  Impl::put(m_state, page);
}

bool
Changeset::has(Page *page) const
{
  return (Impl::has(m_state, page));
}

bool
Changeset::is_empty() const
{
  return (Impl::is_empty(m_state));
}

void
Changeset::clear()
{
  Impl::clear(m_state);
}

void
Changeset::flush(uint64_t lsn)
{
  Impl::flush(m_state, lsn);
}

ChangesetState::ChangesetState(LocalEnvironment *env)
  : env(env), collection(Page::kListChangeset)
{
}

} // namespace hamsterdb
