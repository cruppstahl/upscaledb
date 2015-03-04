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

/*
 * The worker thread for the PageManager
 */

#ifndef HAM_PAGE_MANAGER_WORKER_H
#define HAM_PAGE_MANAGER_WORKER_H

#include "0root/root.h"

#include <vector>
#include <boost/thread.hpp>
#include <boost/atomic.hpp>

// Always verify that a file of level N does not include headers > N!
#include "2device/device.h"
#include "2queue/queue.h"
#include "2worker/worker.h"
#include "3cache/cache.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

class Device;

// The available message types
enum {
  kFlushPage = 1,
  kReleasePointer = 2,
};


struct FlushPageMessage : public MessageBase
{
  FlushPageMessage(Device *device)
    : MessageBase(kFlushPage, 0), device(device) {
  }

  std::vector<Page::PersistedData *> list;
  Device *device;
};

struct ReleasePointerMessage : public MessageBase
{
  ReleasePointerMessage(Page::PersistedData *ptr)
    : MessageBase(kReleasePointer, 0), ptr(ptr) {
  }

  Page::PersistedData *ptr;
};


class PageManagerWorker : public Worker
{
  public:
    PageManagerWorker(Cache *cache)
      : Worker(), m_cache(cache) {
    }

  private:
    virtual void handle_message(MessageBase *message) {
      switch (message->type) {
        case kFlushPage: {
          FlushPageMessage *fpm = (FlushPageMessage *)message;
          std::vector<Page::PersistedData *>::iterator it;
          for (it = fpm->list.begin(); it != fpm->list.end(); ++it) {
            Page::PersistedData *page_data = *it;
            ham_assert(page_data != 0);
            ham_assert(page_data->mutex.try_lock() == false);
            try {
              Page::flush(fpm->device, page_data);
            }
            catch (Exception &ex) {
              page_data->mutex.unlock();
              throw;
            }
            page_data->mutex.unlock();
          }
          break;
        }
        case kReleasePointer: {
          ReleasePointerMessage *rpm = (ReleasePointerMessage *)message;
          Memory::release(rpm->ptr->raw_data);
          delete rpm->ptr;
          break;
        }
        default:
          ham_assert(!"shouldn't be here");
      }
    }

    // The PageManager's cache
    Cache *m_cache;
};

} // namespace hamsterdb

#endif // HAM_PAGE_MANAGER_WORKER_H
