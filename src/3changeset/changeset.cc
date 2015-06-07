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

#include "2worker/worker.h"

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!
#include "1base/signal.h"
#include "1errorinducer/errorinducer.h"
#include "2device/device.h"
#include "2page/page.h"
#include "3changeset/changeset.h"
#include "3journal/journal.h"
#include "3page_manager/page_manager.h"
#include "4db/db_local.h"
#include "4env/env_local.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

/* a unittest hook for Changeset::flush() */
void (*g_CHANGESET_POST_LOG_HOOK)(void);

struct FlushChangesetVisitor
{
  bool operator()(Page *page) {
    ham_assert(page->mutex().try_lock() == false);

    if (page->is_dirty())
      list.push_back(page->get_persisted_data());
    else
      page->mutex().unlock();
    return (true); // remove this page from the PageCollection
  }

  std::vector<Page::PersistedData *> list;
};

static void
async_flush_changeset(std::vector<Page::PersistedData *> list,
                Device *device, Journal *journal, uint64_t lsn,
                bool enable_fsync, int fd_index)
{
  std::vector<Page::PersistedData *>::iterator it = list.begin();
  for (; it != list.end(); it++) {
    Page::PersistedData *page_data = *it;

    // move lock ownership to this thread, otherwise unlocking the mutex
    // will trigger an exception
    ham_assert(page_data->mutex.try_lock() == false);
    page_data->mutex.acquire_ownership();
    page_data->mutex.try_lock(); // TODO remove this

    if (page_data->is_without_header == false)
      page_data->raw_data->header.lsn = lsn;

    Page::flush(device, page_data);
    page_data->mutex.unlock();
    HAM_INDUCE_ERROR(ErrorInducer::kChangesetFlush);
  }

  /* flush the file handle (if required) */
  if (enable_fsync)
    device->flush();

  /* inform the journal that the Changeset was flushed */
  journal->changeset_flushed(fd_index);

  HAM_INDUCE_ERROR(ErrorInducer::kChangesetFlush);
}

void
Changeset::flush(uint64_t lsn)
{
  // now flush all modified pages to disk
  if (m_collection.is_empty())
    return;
  
  HAM_INDUCE_ERROR(ErrorInducer::kChangesetFlush);

  // Fetch the pages, ignoring all pages that are not dirty
  FlushChangesetVisitor visitor;
  m_collection.extract(visitor);

  if (visitor.list.empty())
    return;

  /* Append all changes to the journal. This operation basically
   * "write-ahead logs" all changes. */
  int fd_index = m_env->journal()->append_changeset(visitor.list, lsn);

  HAM_INDUCE_ERROR(ErrorInducer::kChangesetFlush);

  /* execute a post-log hook; this hook is set by the unittest framework
   * and can be used to make a backup copy of the logfile */
  if (g_CHANGESET_POST_LOG_HOOK)
    g_CHANGESET_POST_LOG_HOOK();

  /* The modified pages are now flushed (and unlocked) asynchronously. */
  m_env->page_manager()->run_async(boost::bind(&async_flush_changeset,
                          visitor.list, m_env->device(), m_env->journal(), lsn,
                          (m_env->config().flags & HAM_ENABLE_FSYNC) != 0,
                          fd_index));
}

} // namespace hamsterdb
