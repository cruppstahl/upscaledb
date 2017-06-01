/*
 * Copyright (C) 2005-2017 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

Page::Page(Device *device, LocalDb *db)
  : device_(device), db_(db), node_proxy_(0)
{
  persisted_data.raw_data = 0;
  persisted_data.is_dirty = false;
  persisted_data.is_allocated = false;
  persisted_data.address  = 0;
  persisted_data.size     = (uint32_t)device->page_size();
}

Page::~Page()
{
  assert(cursor_list.is_empty());
  free_buffer();
}

uint32_t
Page::usable_page_size()
{
  uint32_t raw_page_size = db_->env->config.page_size_bytes;
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
    if (ISSET(device_->config.flags, UPS_ENABLE_CRC32)
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
Page::free_buffer()
{
  if (node_proxy_) {
    delete node_proxy_;
    node_proxy_ = 0;
  }
}

} // namespace upscaledb
