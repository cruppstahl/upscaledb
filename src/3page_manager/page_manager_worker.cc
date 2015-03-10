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
      for (it = pcm->page_ids.begin(); it != pcm->page_ids.end(); it++) {
        Page *page = pcm->page_manager->try_fetch(*it);
        if (!page)
          continue;
        ham_assert(page->mutex().try_lock() == false);

        // flush dirty pages
        if (page->is_dirty()) {
          Page::PersistedData *page_data = page->get_persisted_data();
          try {
            Page::flush(pcm->device, page_data);
          }
          catch (Exception &ex) {
            page_data->mutex.unlock();
            throw;
          }
        }
        page->mutex().unlock();
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
        page->mutex().try_lock();
        page->mutex().unlock();
        delete page;
      } 
      // when done: wake up the main thread
      cdbm->notify();
      break;
    }

    case kFlushPages: {
      FlushPagesMessage *fpm = (FlushPagesMessage *)message;
      fpm->cache->purge_if(*fpm);
      std::vector<Page *>::iterator it = fpm->list.begin();
      for (; it != fpm->list.end(); it++) {
        Page *page = *it;
        ScopedSpinlock lock(page->mutex());
        Page::flush(fpm->device, page->get_persisted_data());
      } 
      // when done: wake up the main thread
      fpm->notify();
      break;
    }

    default:
      ham_assert(!"shouldn't be here");
  }
}

} // namespace hamsterdb
