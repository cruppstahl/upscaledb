/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
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
  : m_device(device), m_db(db), m_cursor_list(0),
    m_node_proxy(0), m_datap(&m_data_inline)
{
  ::memset(&m_prev[0], 0, sizeof(m_prev));
  ::memset(&m_next[0], 0, sizeof(m_next));

  m_data_inline.raw_data = 0;
  m_data_inline.is_dirty = false;
  m_data_inline.is_allocated = false;
  m_data_inline.address  = 0;
  m_data_inline.size     = device->page_size();
}

Page::~Page()
{
  ups_assert(m_cursor_list == 0);

  free_buffer();
}

void
Page::alloc(uint32_t type, uint32_t flags)
{
  m_device->alloc_page(this);

  if (flags & kInitializeWithZeroes) {
    size_t page_size = m_device->page_size();
    ::memset(get_raw_payload(), 0, page_size);
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
    // Pro: update crc32
    if ((device->config().flags & UPS_ENABLE_CRC32)
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

Page::PersistedData *
Page::deep_copy_data()
{
  PersistedData *ret = m_datap == &m_data_inline ? 0 : m_datap;

  PersistedData *pd = new PersistedData(*m_datap);
  pd->raw_data = Memory::allocate<PPageData>(pd->size);
  ::memcpy(pd->raw_data, m_datap->raw_data, pd->size);
  m_datap = pd;

  // Delete the node proxy; they maintain pointers into the persisted data,
  // and these pointers are now invalid
  if (m_node_proxy) {
    delete m_node_proxy;
    m_node_proxy = 0;
  }

  return (ret);
}

void
Page::free_buffer()
{
  if (m_node_proxy) {
    delete m_node_proxy;
    m_node_proxy = 0;
  }

  if (m_datap != &m_data_inline) {
    delete m_datap;
    m_datap = &m_data_inline;
  }
}

} // namespace upscaledb
