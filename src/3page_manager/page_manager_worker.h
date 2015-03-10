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
#include "2queue/queue.h"
#include "2worker/worker.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

class Cache;
class Device;
class PageManager;
class LocalDatabase;

// The available message types
enum {
  kCloseDatabase = 1,
  kReleasePointer = 2,
  kPurgeCache = 3,
  kFlushPages = 4,
};

struct FlushPagesMessage : public BlockingMessageBase
{
  FlushPagesMessage(Device *device, Cache *cache)
    : BlockingMessageBase(kFlushPages, MessageBase::kDontDelete),
      device(device), cache(cache) {
  }

  bool operator()(Page *page) {
    list.push_back(page);
    return (false);
  }

  std::vector<Page *> list;
  Device *device;
  Cache *cache;
};

struct CloseDatabaseMessage : public BlockingMessageBase
{
  CloseDatabaseMessage(Device *device, Cache *cache, LocalDatabase *db)
    : BlockingMessageBase(kCloseDatabase, MessageBase::kDontDelete),
      device(device), cache(cache), db(db) {
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
  Cache *cache;
  LocalDatabase *db;
};

struct PurgeCacheMessage : public MessageBase
{
  PurgeCacheMessage(PageManager *page_manager, Device *device,
                  boost::atomic<bool> *pcompleted)
    : MessageBase(kPurgeCache, 0), page_manager(page_manager), device(device),
      pcompleted(pcompleted) {
  }

  PageManager *page_manager;
  Device *device;
  boost::atomic<bool> *pcompleted;
  std::vector<uint64_t> page_ids;
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
    virtual void handle_message(MessageBase *message);

    // The PageManager's cache
    Cache *m_cache;
};

} // namespace hamsterdb

#endif // HAM_PAGE_MANAGER_WORKER_H
