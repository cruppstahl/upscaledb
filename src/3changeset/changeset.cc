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

struct PageCollectionVisitor
{
  PageCollectionVisitor(Page **pages)
    : num_pages(0), pages(pages) {
  }

  void prepare(size_t size) {
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

void
Changeset::flush(uint64_t lsn)
{
  // now flush all modified pages to disk
  if (m_collection.is_empty())
    return;
  
  HAM_INDUCE_ERROR(ErrorInducer::kChangesetFlush);

  // Fetch the pages, ignoring all pages that are not dirty
  Page **pages = (Page **)::alloca(sizeof(Page *) * m_collection.size());
  PageCollectionVisitor visitor(pages);
  m_collection.extract(visitor);

  // TODO sort by address (really?)

  if (visitor.num_pages == 0)
    return;

  // If only one page is modified then the modification is atomic. The page
  // is written to the btree (no log required).
  //
  // If more than one page is modified then the modification is no longer
  // atomic. All dirty pages are written to the log.
  if (visitor.num_pages > 1) {
    m_env->journal()->append_changeset((const Page **)visitor.pages,
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
  if (m_env->get_flags() & HAM_ENABLE_FSYNC)
    m_env->device()->flush();

  HAM_INDUCE_ERROR(ErrorInducer::kChangesetFlush);
}

} // namespace hamsterdb
