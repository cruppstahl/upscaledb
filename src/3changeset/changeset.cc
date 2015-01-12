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
#include "4env/env.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

/* a unittest hook for Changeset::flush() */
void (*g_CHANGESET_POST_LOG_HOOK)(void);

void
Changeset::add_page(Page *page)
{
  ham_assert(m_env->get_flags() & HAM_ENABLE_RECOVERY);

  if (contains(page))
    return;

  m_collection.add_page(page);
}

Page *
Changeset::get_page(uint64_t pageid)
{
  ham_assert(m_env->get_flags() & HAM_ENABLE_RECOVERY);

  return (m_collection.get_page(pageid));
}

void
Changeset::clear()
{
  m_collection.clear();
}

void
Changeset::flush(uint64_t lsn)
{
  // now flush all modified pages to disk
  ham_assert(m_env->get_flags() & HAM_ENABLE_RECOVERY);

  if (m_collection.is_empty())
    return;

  // TODO swap with new PageCollection
  // TODO but make sure that the swapped Collection stores the hazard pointer!

  HAM_INDUCE_ERROR(ErrorInducer::kChangesetFlush);

  // TODO get a copy of the whole collection
  // TODO sort by address (really?)
  // TODO remove all pages that are not dirty

  // If only one page is modified then the modification is atomic. The page
  // is written.
  //
  // If more than one page is modified then the modification is no longer
  // atomic. All dirty pages are written to the log.
  if (0) { // TODO
    m_env->get_journal()->append_changeset(m_page_manager, m_page_manager_size,
                    m_indices, m_indices_size,
                    m_others, m_others_size,
                    m_blobs, m_blobs_size,
                    lsn);
  }

  HAM_INDUCE_ERROR(ErrorInducer::kChangesetFlush);

  /* execute a post-log hook; this hook is set by the unittest framework
   * and can be used to make a backup copy of the logfile */
  if (g_CHANGESET_POST_LOG_HOOK)
    g_CHANGESET_POST_LOG_HOOK();

  /* now write all the pages to the file; if any of these writes fail,
   * we can still recover from the log */
  // TODO
  while (p) {
    if (p->is_without_header() == false)
      p->set_lsn(lsn);
    m_env->get_page_manager()->flush_page(p);
    p = p->get_next(Page::kListChangeset);

    HAM_INDUCE_ERROR(ErrorInducer::kChangesetFlush);
  }

  /* flush the file handle (if required) */
  if (m_env->get_flags() & HAM_ENABLE_FSYNC)
    m_env->get_device()->flush();

  HAM_INDUCE_ERROR(ErrorInducer::kChangesetFlush);
}

} // namespace hamsterdb
