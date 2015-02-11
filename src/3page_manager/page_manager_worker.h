/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
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
#include "2queue/queue.h"
#include "3cache/cache.h"
#include "4worker/worker.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

struct PurgeCacheMessage : public MessageBase
{
  // The available message types
  enum {
    kPurgeCache = 1,
  };

  PurgeCacheMessage(boost::atomic<bool> *pending)
    : MessageBase(kPurgeCache, 0), ppending(pending) {
  }

  std::vector<uint64_t> addresses;
  boost::atomic<bool> *ppending;
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
        case PurgeCacheMessage::kPurgeCache: {
          PurgeCacheMessage *pcm = (PurgeCacheMessage *)message;
          std::vector<uint64_t>::iterator it = pcm->addresses.begin();
          for (; it != pcm->addresses.end(); ++it) {
            Page *page = m_cache->get(*it);
            if (page && page->mutex().try_lock()) {
              if (page->get_data()
                    && page->cursor_list() == 0
                    && !page->is_allocated()) {
                try {
                  page->flush();
                  page->free_buffer();
                }
                catch (Exception &ex) {
                  page->mutex().unlock();
                  throw;
                }
              }
              page->mutex().unlock();
            }
          }
          *pcm->ppending = false;
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
