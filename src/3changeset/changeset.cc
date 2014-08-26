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

#define INDUCE(id)                                                  \
  while (m_inducer) {                                               \
    ham_status_t st = m_inducer->induce(id);                        \
    if (st)                                                         \
      throw Exception(st);                                          \
    break;                                                          \
  }

namespace hamsterdb {

/* a unittest hook for Changeset::flush() */
void (*g_CHANGESET_POST_LOG_HOOK)(void);

void
Changeset::add_page(Page *page)
{
  if (page->is_in_list(m_head, Page::kListChangeset))
    return;

  ham_assert(0 == page->get_next(Page::kListChangeset));
  ham_assert(0 == page->get_previous(Page::kListChangeset));
  ham_assert(m_env->get_flags() & HAM_ENABLE_RECOVERY);

  m_head = page->list_insert(m_head, Page::kListChangeset);
}

Page *
Changeset::get_page(ham_u64_t pageid)
{
  Page *page = m_head;

  while (page) {
    ham_assert(m_env->get_flags() & HAM_ENABLE_RECOVERY);

    if (page->get_address() == pageid)
      return (page);
    page = page->get_next(Page::kListChangeset);
  }

  return (0);
}

void
Changeset::clear()
{
  while (m_head)
    m_head = m_head->list_remove(m_head, Page::kListChangeset);
}

#define append(b, bs, bc, p)                                          \
  if (bs + 1 >= bc) {                                                 \
    bc = bc ? bc * 2 : 8;                                             \
    b = (Page **)::realloc(b, sizeof(Page *) * bc);                   \
  }                                                                   \
  b[bs++] = p;

void
Changeset::flush(ham_u64_t lsn)
{
  ham_u32_t page_count = 0;
  Page *n, *p = m_head;
  if (!p)
    return;

  INDUCE(ErrorInducer::kChangesetFlush);

  m_blobs_size = 0;
  m_page_manager_size = 0;
  m_indices_size = 0;
  m_others_size = 0;

  // first step: remove all pages that are not dirty and sort all others
  // into the buckets
  while (p) {
    n = p->get_next(Page::kListChangeset);
    if (!p->is_dirty()) {
      p = n;
      continue;
    }

    if (p->is_header()) {
      append(m_indices, m_indices_size, m_indices_capacity, p);
    }
    else if (p->get_flags() & Page::kNpersNoHeader) {
      append(m_blobs, m_blobs_size, m_blobs_capacity, p);
    }
    else {
      switch (p->get_type()) {
        case Page::kTypeBlob:
          append(m_blobs, m_blobs_size, m_blobs_capacity, p);
          break;
        case Page::kTypeBroot:
        case Page::kTypeBindex:
        case Page::kTypeHeader:
          append(m_indices, m_indices_size, m_indices_capacity, p);
          break;
        case Page::kTypePageManager:
          append(m_page_manager, m_page_manager_size,
                          m_page_manager_capacity, p);
          break;
        default:
          append(m_others, m_others_size, m_others_capacity, p);
          break;
      }
    }
    page_count++;
    p = n;

    INDUCE(ErrorInducer::kChangesetFlush);
  }

  if (page_count == 0) {
    INDUCE(ErrorInducer::kChangesetFlush);
    clear();
    return;
  }

  INDUCE(ErrorInducer::kChangesetFlush);

  // If there's more than one index operation then the operation must
  // be atomic and therefore logged.
  //
  // If there are unknown pages (in m_others) or PageManager state pages
  // then we also log the modifications.
  //
  // Make sure that blob pages are logged at the end. Multi-page blob pages
  // do not have a header and therefore don't store a lsn. But the lsn is
  // required for recovery. Therefore make sure that pages WITH a page header
  // are logged first, and Journal::recover_changeset can extract a valid
  // lsn from those pages.
  if (m_others_size
      || m_page_manager_size
      || m_indices_size > 1
      || m_blobs_size) {
    m_env->get_journal()->append_changeset(m_page_manager, m_page_manager_size,
                    m_indices, m_indices_size,
                    m_others, m_others_size,
                    m_blobs, m_blobs_size,
                    lsn);
  }

  p = m_head;

  INDUCE(ErrorInducer::kChangesetFlush);

  // now flush all modified pages to disk
  ham_assert(m_env->get_flags() & HAM_ENABLE_RECOVERY);

  /* execute a post-log hook; this hook is set by the unittest framework
   * and can be used to make a backup copy of the logfile */
  if (g_CHANGESET_POST_LOG_HOOK)
    g_CHANGESET_POST_LOG_HOOK();

  /* now write all the pages to the file; if any of these writes fail,
   * we can still recover from the log */
  while (p) {
    if (!(p->get_flags() & Page::kNpersNoHeader))
      p->set_lsn(lsn);
    m_env->get_page_manager()->flush_page(p);
    p = p->get_next(Page::kListChangeset);

    INDUCE(ErrorInducer::kChangesetFlush);
  }

  /* flush the file handle (if required) */
  if (m_env->get_flags() & HAM_ENABLE_FSYNC)
    m_env->get_device()->flush();

  /* done - we can now clear the changeset */
  clear();
}

} // namespace hamsterdb
