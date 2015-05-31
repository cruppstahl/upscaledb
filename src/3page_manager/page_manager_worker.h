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
  kReleasePointer       = 1,
  kFlushPages           = 2,
  kFlushChangeset       = 3,
};

struct FlushPagesMessage : public MessageBase
{
  FlushPagesMessage(PageManager *page_manager_, Device *device_,
           Mutex *mutex_ = 0, Condition *cond_ = 0)
    : MessageBase(mutex_, cond_, kFlushPages),
      page_manager(page_manager_), device(device_) {
  }

  std::vector<uint64_t> page_ids;
  PageManager *page_manager;
  Device *device;
};

struct FlushChangesetMessage : public MessageBase
{
  FlushChangesetMessage(Device *device, Journal *journal,
                  uint64_t lsn, bool enable_fsync)
    : MessageBase(kFlushChangeset),
      device(device), journal(journal), lsn(lsn), enable_fsync(enable_fsync) {
  }

  bool operator()(Page *page) {
    ham_assert(page->mutex().try_lock() == false);

    if (page->is_dirty()) {
      if (page->is_without_header() == false)
        page->set_lsn(lsn);
      list.push_back(page->get_persisted_data());
    }
    else
      page->mutex().unlock();
    return (true); // remove this page from the PageCollection
  }

  std::vector<Page::PersistedData *> list;
  Device *device;
  Journal *journal;
  uint64_t lsn;
  bool enable_fsync;
  int fd_index;
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
  private:
    virtual ham_status_t handle_message(MessageBase *message);
};

} // namespace hamsterdb

#endif // HAM_PAGE_MANAGER_WORKER_H
