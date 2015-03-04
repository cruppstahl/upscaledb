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

struct FlushPageMessage : public MessageBase
{
  // The available message types
  enum {
    kFlushPage = 1,
  };

  FlushPageMessage()
    : MessageBase(kFlushPage, 0) {
  }

  std::vector<Page *> list;
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
        case FlushPageMessage::kFlushPage: {
          FlushPageMessage *fpm = (FlushPageMessage *)message;
          for (std::vector<Page *>::iterator it = fpm->list.begin();
                          it != fpm->list.end();
                          ++it) {
            Page *page = *it;
            ham_assert(page != 0);
            ham_assert(page->mutex().try_lock() == false);
            try {
              page->flush();
            }
            catch (Exception &) {
              page->mutex().unlock();
              throw;
            }
            page->mutex().unlock();
          }
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
