/*
 * Copyright (C) 2005-2016 Christoph Rupp (chris@crupp.de).
 * All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
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
#include "4db/db_local.h"
#include "4env/env_local.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

/* a unittest hook for Changeset::flush() */
void (*g_CHANGESET_POST_LOG_HOOK)(void);

struct FlushChangesetVisitor
{
  bool operator()(Page *page) {
    ups_assert(page->mutex().try_lock() == false);

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
    ups_assert(page_data->mutex.try_lock() == false);
    page_data->mutex.acquire_ownership();
    page_data->mutex.try_lock(); // TODO remove this

    if (page_data->is_without_header == false)
      page_data->raw_data->header.lsn = lsn;

    Page::flush(device, page_data);
    page_data->mutex.unlock();
    UPS_INDUCE_ERROR(ErrorInducer::kChangesetFlush);
  }

  /* flush the file handle (if required) */
  if (enable_fsync)
    device->flush();

  /* inform the journal that the Changeset was flushed */
  journal->changeset_flushed(fd_index);

  UPS_INDUCE_ERROR(ErrorInducer::kChangesetFlush);
}

void
Changeset::flush(uint64_t lsn)
{
  // now flush all modified pages to disk
  if (m_collection.is_empty())
    return;
  
  UPS_INDUCE_ERROR(ErrorInducer::kChangesetFlush);

  // Fetch the pages, ignoring all pages that are not dirty
  FlushChangesetVisitor visitor;
  m_collection.extract(visitor);

  if (visitor.list.empty())
    return;

  /* Append all changes to the journal. This operation basically
   * "write-ahead logs" all changes. */
  uint64_t last_blob_page_id = m_env->page_manager()->last_blob_page_id();
  int fd_index = m_env->journal()->append_changeset(visitor.list,
                                        last_blob_page_id, lsn);

  UPS_INDUCE_ERROR(ErrorInducer::kChangesetFlush);

  /* execute a post-log hook; this hook is set by the unittest framework
   * and can be used to make a backup copy of the logfile */
  if (g_CHANGESET_POST_LOG_HOOK)
    g_CHANGESET_POST_LOG_HOOK();

  /* The modified pages are now flushed (and unlocked) asynchronously. */
  m_env->page_manager()->run_async(boost::bind(&async_flush_changeset,
                          visitor.list, m_env->device(), m_env->journal(), lsn,
                          (m_env->config().flags & UPS_ENABLE_FSYNC) != 0,
                          fd_index));
}

} // namespace upscaledb
