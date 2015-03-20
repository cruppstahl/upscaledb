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

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!
#include "1errorinducer/errorinducer.h"
#include "2device/device.h"
#include "3page_manager/page_manager_worker.h"
#include "3page_manager/page_manager.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

void
PageManagerWorker::handle_message(MessageBase *message)
{
  switch (message->type) {
    case kPurgeCache: {
      PurgeCacheMessage *pcm = (PurgeCacheMessage *)message;

      std::vector<uint64_t>::iterator it;
      Page::PersistedData *page_data;
      for (it = pcm->page_ids.begin(); it != pcm->page_ids.end(); it++) {
        page_data = pcm->page_manager->try_fetch_page_data(*it);
        if (!page_data)
          continue;
        ham_assert(page_data->mutex.try_lock() == false);

        // flush dirty pages
        if (page_data->is_dirty) {
          try {
            Page::flush(pcm->device, page_data);
          }
          catch (Exception &ex) {
            page_data->mutex.unlock();
            throw;
          }
        }
        page_data->mutex.unlock();
      }

      *pcm->pcompleted = true;
      break;
    }

    case kReleasePointer: {
      ReleasePointerMessage *rpm = (ReleasePointerMessage *)message;

      delete rpm->ptr;
      break;
    }

    case kCloseDatabase: {
      CloseDatabaseMessage *cdbm = (CloseDatabaseMessage *)message;
      cdbm->cache->purge_if(*cdbm);
      std::vector<Page *>::iterator it = cdbm->list.begin();
      for (; it != cdbm->list.end(); it++) {
        Page *page = *it;
        Page::flush(cdbm->device, page->get_persisted_data());
        page->mutex().safe_unlock();
        delete page;
      } 
      // when done: wake up the main thread
      cdbm->notify();
      break;
    }

    case kFlushPages: {
      FlushPagesMessage *fpm = (FlushPagesMessage *)message;
      fpm->cache->purge_if(*fpm);
      std::vector<Page::PersistedData *>::iterator it = fpm->list.begin();
      for (; it != fpm->list.end(); it++) {
        Page::PersistedData *page_data = *it;
        ScopedSpinlock lock(page_data->mutex);
        Page::flush(fpm->device, page_data);
      } 
      // when done: wake up the main thread
      fpm->notify();
      break;
    }

    case kFlushChangeset: {
      FlushChangesetMessage *fcm = (FlushChangesetMessage *)message;
      std::vector<Page::PersistedData *>::iterator it = fcm->list.begin();
      for (; it != fcm->list.end(); it++) {
        Page::PersistedData *page_data = *it;

        // move lock ownership to this thread, otherwise unlocking the mutex
        // will trigger an exception
        ham_assert(page_data->mutex.try_lock() == false);
        page_data->mutex.acquire_ownership();

        Page::flush(fcm->device, page_data);
        HAM_INDUCE_ERROR(ErrorInducer::kChangesetFlush);
        page_data->mutex.unlock();
      }

      /* flush the file handle (if required) */
      if (fcm->enable_fsync)
        fcm->device->flush();

      /* inform the journal that the Changeset was flushed */
      fcm->journal->changeset_flushed(fcm->fd_index);

      HAM_INDUCE_ERROR(ErrorInducer::kChangesetFlush);
      break;
    }

    default:
      ham_assert(!"shouldn't be here");
  }
}

} // namespace hamsterdb
