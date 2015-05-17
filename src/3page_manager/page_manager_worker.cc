/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
 * All Rights Reserved.
 *
 * NOTICE: All information contained herein is, and remains the property
 * of Christoph Rupp and his suppliers, if any. The intellectual and
 * technical concepts contained herein are proprietary to Christoph Rupp
 * and his suppliers and may be covered by Patents, patents in process,
 * and are protected by trade secret or copyright law. Dissemination of
 * this information or reproduction of this material is strictly forbidden
 * unless prior written permission is obtained from Christoph Rupp.
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
          catch (Exception &) {
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
