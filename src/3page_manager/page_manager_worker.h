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
#include "1base/mutex.h"
#include "2device/device.h"
#include "2queue/queue.h"
#include "2worker/worker.h"
#include "3cache/cache.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

class Device;
class LocalDatabase;

// The available message types
enum {
  kFlushPage = 1,
  kDeletePage = 2,
  kReleasePointer = 3,
};

struct DeletePageMessage : public MessageBase
{
  DeletePageMessage(Device *device, LocalDatabase *db)
    : MessageBase(kDeletePage, MessageBase::kDontDelete),
      device(device), db(db), completed(false) {
  }

  bool operator()(Page *page) {
    if (page->get_db() == db && page->get_address() != 0) {
      list.push_back(page);
      return (true);
    }
    return (false);
  }

  std::vector<Page *> list;
  Device *device;
  LocalDatabase *db;
  Mutex mutex;      // to protect |cond| and |complete|
  Condition cond;
  bool completed;
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
              if (page_data->raw_data != 0)
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
#ifdef HAM_ENABLE_HELGRIND
          rpm->ptr->mutex.try_lock();
          rpm->ptr->mutex.unlock();
#endif
          delete rpm->ptr;
          break;
        }
        case kDeletePage: {
          DeletePageMessage *dpm = (DeletePageMessage *)message;
          std::vector<Page *>::iterator it = dpm->list.begin();
          for (; it != dpm->list.end(); it++) {
            Page *page = *it;
            Page::flush(dpm->device, page->get_persisted_data());
            delete page;
          } 
          // when done: wake up the main thread
          ScopedLock lock(dpm->mutex);
          dpm->completed = true;
          dpm->cond.notify_all();
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
