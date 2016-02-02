/*
 * Copyright (C) 2005-2016 Christoph Rupp (chris@crupp.de).
 * All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * See the file COPYING for License information.
 */

#include "0root/root.h"

#include <string.h>
#include "3rdparty/murmurhash3/MurmurHash3.h"

#include "1base/error.h"
#include "1os/os.h"
#include "2page/page.h"
#include "2device/device.h"
#include "3btree/btree_node_proxy.h"

namespace upscaledb {

uint64_t Page::ms_page_count_flushed = 0;

Page::Page(Device *device, LocalDatabase *db)
  : m_device(device), m_db(db), m_cursor_list(0), m_node_proxy(0)
{
  ::memset(&m_prev[0], 0, sizeof(m_prev));
  ::memset(&m_next[0], 0, sizeof(m_next));

  persisted_data.raw_data = 0;
  persisted_data.is_dirty = false;
  persisted_data.is_allocated = false;
  persisted_data.address  = 0;
  persisted_data.size     = device->page_size();
}

Page::~Page()
{
  assert(m_cursor_list == 0);

  free_buffer();
}

void
Page::alloc(uint32_t type, uint32_t flags)
{
  m_device->alloc_page(this);

  if (flags & kInitializeWithZeroes) {
    size_t page_size = m_device->page_size();
    ::memset(raw_payload(), 0, page_size);
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
Page::flush(Device *device, PersistedData *page_data)
{
  if (page_data->is_dirty) {
    // update crc32
    if ((device->config.flags & UPS_ENABLE_CRC32)
        && likely(!page_data->is_without_header)) {
      MurmurHash3_x86_32(page_data->raw_data->header.payload,
                         page_data->size - (sizeof(PPageHeader) - 1),
                         (uint32_t)page_data->address,
                         &page_data->raw_data->header.crc32);
    }
    device->write(page_data->address, page_data->raw_data, page_data->size);
    page_data->is_dirty = false;
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
}

} // namespace upscaledb
