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

#include <string.h>

#include "1base/error.h"
#include "1os/os.h"
#include "2page/page.h"
#include "2device/device.h"
#include "3btree/btree_node_proxy.h"

namespace hamsterdb {

uint64_t Page::ms_page_count_flushed = 0;

Page::Page(Device *device, LocalDatabase *db)
  : m_device(device), m_db(db), m_address(0), m_is_allocated(false),
    m_is_without_header(false), m_is_dirty(false), m_cursor_list(0),
    m_node_proxy(0), m_data(0)
{
  memset(&m_prev[0], 0, sizeof(m_prev));
  memset(&m_next[0], 0, sizeof(m_next));
}

Page::~Page()
{
  ham_assert(m_cursor_list == 0);

#ifdef HAM_ENABLE_HELGRIND
  // safely unlock the mutex
  m_mutex.try_lock();
#endif
  m_mutex.unlock();

  if (m_node_proxy) {
    delete m_node_proxy;
    m_node_proxy = 0;
  }

  if (m_data != 0)
    m_device->free_page(this);
}

void
Page::alloc(uint32_t type, uint32_t flags)
{
  m_device->alloc_page(this);

  if (flags & kInitializeWithZeroes) {
    size_t page_size = m_device->page_size();
    memset(get_raw_payload(), 0, page_size);
  }

  if (type)
    set_type(type);
}

void
Page::fetch(uint64_t address)
{
  m_device->read_page(this, address);
  set_address(address);
}

void
Page::flush()
{
  if (is_dirty()) {
    m_device->write_page(this);
    set_dirty(false);
    ms_page_count_flushed++;
  }
}

void
Page::free_buffer()
{
  if (m_node_proxy) {
    delete m_node_proxy;
    m_node_proxy = 0;
  }

  if (m_is_allocated)
    Memory::release(m_data);
  m_data = 0;
}

} // namespace hamsterdb
