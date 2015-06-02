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

ham_status_t
PageManagerWorker::handle_message(MessageBase *message) const
{
  switch (message->type) {
    case kFlushPages: {
      FlushPagesMessage *fpm = (FlushPagesMessage *)message;

      std::vector<uint64_t>::iterator it = fpm->page_ids.begin();
      Page::PersistedData *page_data;
      for (it = fpm->page_ids.begin(); it != fpm->page_ids.end(); it++) {
        page_data = fpm->page_manager->try_fetch_page_data(*it);
        if (!page_data)
          continue;
        ham_assert(page_data->mutex.try_lock() == false);

        // flush dirty pages
        if (page_data->is_dirty) {
          try {
            Page::flush(fpm->device, page_data);
          }
          catch (Exception &ex) {
            page_data->mutex.unlock();
            return (ex.code);
          }
        }
        page_data->mutex.unlock();
      }
      return (0);
    }

    case kReleasePointer: {
      ReleasePointerMessage *rpm = (ReleasePointerMessage *)message;
      delete rpm->ptr;
      return (0);
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
        page_data->mutex.try_lock(); // TODO remove this

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
      return (0);
    }

    default:
      ham_assert(!"shouldn't be here");
      return (HAM_INTERNAL_ERROR);
  }
}

} // namespace hamsterdb
