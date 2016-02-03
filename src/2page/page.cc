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
  : device_(device), db_(db), cursor_list_(0), node_proxy_(0)
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
  assert(cursor_list_ == 0);
  free();
}

uint32_t
Page::usable_page_size()
{
  uint32_t raw_page_size = db_->lenv()->config().page_size_bytes;
  return raw_page_size - Page::kSizeofPersistentHeader;
}

void
Page::alloc(uint32_t type, uint32_t flags)
{
  device_->alloc_page(this);

  if (flags & kInitializeWithZeroes) {
    size_t page_size = device_->page_size();
    ::memset(raw_payload(), 0, page_size);
  }

  if (type)
    set_type(type);
}

void
Page::fetch(uint64_t address)
{
  device_->read_page(this, address);
  set_address(address);
}

void
Page::flush()
{
  if (persisted_data.is_dirty) {
    // update crc32
    if (isset(device_->config.flags, UPS_ENABLE_CRC32)
        && likely(!persisted_data.is_without_header)) {
      MurmurHash3_x86_32(persisted_data.raw_data->header.payload,
                         persisted_data.size - (sizeof(PPageHeader) - 1),
                         (uint32_t)persisted_data.address,
                         &persisted_data.raw_data->header.crc32);
    }
    device_->write(persisted_data.address, persisted_data.raw_data,
                    persisted_data.size);
    persisted_data.is_dirty = false;
    ms_page_count_flushed++;
  }
}

void
Page::free()
{
  if (node_proxy_) {
    delete node_proxy_;
    node_proxy_ = 0;
  }
}

} // namespace upscaledb
