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
#include "3page_manager/page_manager_worker.h"
#include "4db/db_local.h"
#include "4env/env_local.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

/* a unittest hook for Changeset::flush() */
void (*g_CHANGESET_POST_LOG_HOOK)(void);

void
Changeset::flush(uint64_t lsn)
{
  // now flush all modified pages to disk
  if (m_collection.is_empty())
    return;
  
  HAM_INDUCE_ERROR(ErrorInducer::kChangesetFlush);

  // Fetch the pages, ignoring all pages that are not dirty
  FlushChangesetMessage *message = new FlushChangesetMessage(
                              m_env->device(), m_env->journal(), lsn,
                              (m_env->config().flags & HAM_ENABLE_FSYNC) != 0);
  m_collection.extract(*message);

  if (message->list.empty()) {
    delete message;
    return;
  }

  /* Append all changes to the journal. This operation basically
   * "write-ahead logs" all changes. */
  message->fd_index = m_env->journal()->append_changeset(message->list, lsn);

  HAM_INDUCE_ERROR(ErrorInducer::kChangesetFlush);

  /* execute a post-log hook; this hook is set by the unittest framework
   * and can be used to make a backup copy of the logfile */
  if (g_CHANGESET_POST_LOG_HOOK)
    g_CHANGESET_POST_LOG_HOOK();

  /* The modified pages are now flushed (and unlocked) asynchronously. */
  m_env->page_manager()->add_to_worker_queue(message);
}

} // namespace hamsterdb
