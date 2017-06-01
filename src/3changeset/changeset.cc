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
#include "4env/env_local.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

/* a unittest hook for Changeset::flush() */
void (*g_CHANGESET_POST_LOG_HOOK)(void);

struct UnlockPage {
  bool operator()(Page *page) {
#ifdef UPS_ENABLE_HELGRIND
    page->mutex().try_lock();
#endif
    page->mutex().unlock();
    return true;
  }
};

struct FlushChangesetVisitor {
  bool operator()(Page *page) {
    assert(page->mutex().try_lock() == false);

    if (page->is_dirty())
      list.push_back(page);
    else
      page->mutex().unlock();
    return true; // remove this page from the PageCollection
  }

  std::vector<Page *> list;
};

static void
flush_changeset_to_file(std::vector<Page *> list, Device *device,
                Journal *journal, uint64_t lsn, bool enable_fsync)
{
  std::vector<Page *>::iterator it = list.begin();
  for (; it != list.end(); it++) {
    Page *page = *it;

    // move lock ownership to this thread, otherwise unlocking the mutex
    // will trigger an exception
    assert(page->mutex().try_lock() == false);
    page->mutex().acquire_ownership();
    page->mutex().try_lock(); // TODO remove this

    if (likely(page->is_without_header() == false))
      page->set_lsn(lsn);

    page->flush();
    page->mutex().unlock();
    UPS_INDUCE_ERROR(ErrorInducer::kChangesetFlush);
  }

  /* flush the file handle (if required) */
  if (enable_fsync)
    device->flush();

  UPS_INDUCE_ERROR(ErrorInducer::kChangesetFlush);
}

void
Changeset::clear()
{
  UnlockPage unlocker;
  collection.for_each(unlocker);
  collection.clear();
}

void
Changeset::flush(uint64_t lsn)
{
  // now flush all modified pages to disk
  if (collection.is_empty())
    return;
  
  UPS_INDUCE_ERROR(ErrorInducer::kChangesetFlush);

  // Fetch the pages, ignoring all pages that are not dirty
  FlushChangesetVisitor visitor;
  collection.extract(visitor);

  if (visitor.list.empty())
    return;

  // Append all changes to the journal. This operation basically
  // "write-ahead logs" all changes.
  env->journal->append_changeset(visitor.list,
                  env->page_manager->last_blob_page_id(), lsn);

  UPS_INDUCE_ERROR(ErrorInducer::kChangesetFlush);

  // execute a post-log hook; this hook is set by the unittest framework
  // and can be used to make a backup copy of the logfile
  if (unlikely(g_CHANGESET_POST_LOG_HOOK != 0))
    g_CHANGESET_POST_LOG_HOOK();

  // The modified pages are now flushed (and unlocked) asynchronously
  // to the database file
  env->page_manager->run_async(boost::bind(&flush_changeset_to_file,
                          visitor.list, env->device.get(), env->journal.get(),
                          lsn, ISSET(env->config.flags, UPS_ENABLE_FSYNC)));
}

} // namespace upscaledb
