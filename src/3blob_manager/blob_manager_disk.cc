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

#include <algorithm>
#include <vector>

#include "3rdparty/murmurhash3/MurmurHash3.h"

// Always verify that a file of level N does not include headers > N!
#include "1base/error.h"
#include "1base/dynamic_array.h"
#include "2compressor/compressor.h"
#include "2device/device_disk.h"
#include "3blob_manager/blob_manager_disk.h"
#include "3page_manager/page_manager.h"
#include "4context/context.h"
#include "4db/db_local.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

using namespace upscaledb;

static bool
check_integrity(DiskBlobManager *dbm, PBlobPageHeader *header)
{
  assert(header->num_pages > 0);

  if (header->free_bytes + DiskBlobManager::kPageOverhead
        > dbm->config->page_size_bytes * header->num_pages) {
    ups_trace(("integrity violated: free bytes exceeds page boundary"));
    return false;
  }

  // freelist is not used if this is a multi-page blob
  if (header->num_pages > 1)
    return true;

  uint32_t total_sizes = 0;
  typedef std::pair<uint32_t, uint32_t> Range;
  typedef std::vector<Range> RangeVec;
  RangeVec ranges;

  for (uint32_t i = 0; i < PBlobPageHeader::kFreelistLength - 1; i++) {
    const auto& entry = header->freelist[i];
    if (entry.size == 0) {
      assert(entry.offset == 0);
      continue;
    }
    total_sizes += entry.size;
    ranges.emplace_back(entry.offset, entry.size);
  }

  // the sum of freelist chunks must not exceed total number of free bytes
  if (total_sizes > header->free_bytes) {
    ups_trace(("integrity violated: total freelist slots exceed free bytes"));
    return false;
  }

  std::sort(ranges.begin(), ranges.end());

  if (!ranges.empty()) {
    for (uint32_t i = 0; i < ranges.size() - 1; i++) {
      if (ranges[i].first + ranges[i].second
          > dbm->config->page_size_bytes * header->num_pages) {
        ups_trace(("integrity violated: freelist slot %u/%u exceeds page",
                    ranges[i].first, ranges[i].second));
        return false;
      }
      if (ranges[i].first + ranges[i].second > ranges[i + 1].first) {
        ups_trace(("integrity violated: freelist slot %u/%u overlaps with %lu",
                    ranges[i].first, ranges[i].second,
                    ranges[i + 1].first));
        throw Exception(UPS_INTEGRITY_VIOLATED);
      }
    }
  }

  return true;
}

static void
add_to_freelist(DiskBlobManager *dbm, PBlobPageHeader *header,
                uint32_t offset, uint32_t size)
{
  assert(check_integrity(dbm, header));

  // freelist is not used if this is a multi-page blob
  if (header->num_pages > 1)
    return;

  // first try to collapse the blobs
  for (uint32_t i = 0; i < PBlobPageHeader::kFreelistLength; i++) {
    if (offset + size == header->freelist[i].offset) {
      header->freelist[i].offset = offset;
      header->freelist[i].size += size;
      assert(check_integrity(dbm, header));
      return;
    }
    if (header->freelist[i].offset + header->freelist[i].size == offset) {
      header->freelist[i].size += size;
      assert(check_integrity(dbm, header));
      return;
    }
  }

  // otherwise store the blob in a new slot, if available
  uint32_t smallest = 0;
  for (uint32_t i = 0; i < PBlobPageHeader::kFreelistLength; i++) {
    // slot is empty
    if (header->freelist[i].size == 0) {
      header->freelist[i].offset = offset;
      header->freelist[i].size = size;
      assert(check_integrity(dbm, header));
      return;
    }
    // otherwise look for the smallest entry
    if (header->freelist[i].size < header->freelist[smallest].size) {
      smallest = i;
      continue;
    }
  }

  // overwrite the smallest entry?
  if (size > header->freelist[smallest].size) {
    header->freelist[smallest].offset = offset;
    header->freelist[smallest].size = size;
  }

  assert(check_integrity(dbm, header));
}

static bool
alloc_from_freelist(DiskBlobManager *dbm, PBlobPageHeader *header,
                uint32_t size, uint64_t *poffset)
{
  assert(check_integrity(dbm, header));

  // freelist is not used if this is a multi-page blob
  if (header->num_pages > 1)
    return false;

  for (uint32_t i = 0; i < PBlobPageHeader::kFreelistLength; i++) {
    // exact match
    if (header->freelist[i].size == size) {
      *poffset = header->freelist[i].offset;
      header->freelist[i].offset = 0;
      header->freelist[i].size = 0;
      assert(check_integrity(dbm, header));
      return true;
    }
    // space in freelist is larger than what we need? return this space,
    // make sure the remaining gap stays in the freelist
    if (header->freelist[i].size > size) {
      *poffset = header->freelist[i].offset;
      header->freelist[i].offset = (uint32_t)(*poffset + size);
      header->freelist[i].size -= size;
      assert(check_integrity(dbm, header));
      return true;
    }
  }

  // there was no gap large enough for the blob
  return false;
}

static uint8_t *
read_chunk(DiskBlobManager *dbm, Context *context, Page *page, Page **ppage,
                uint64_t address, bool fetch_read_only, bool mapped_pointer)
{
  // get the page-id from this chunk
  uint32_t page_size = dbm->config->page_size_bytes;
  uint64_t pageid = address - (address % page_size);

  // is this the current page? if yes then continue working with this page,
  // otherwise fetch the page
  if (page && page->address() != pageid)
    page = 0;

  uint8_t *data;

  if (unlikely(!page)) {
    uint32_t flags = 0;
    if (fetch_read_only)
      flags |= PageManager::kReadOnly;
    if (mapped_pointer)
      flags |= PageManager::kOnlyFromCache;
    page = dbm->page_manager->fetch(context, pageid, flags);
    if (ppage)
      *ppage = page;
    if (page)
      data = page->raw_payload();
    else {
      DiskDevice *dd = (DiskDevice *)dbm->device;
      data = dd->mapped_pointer(pageid);
    }
  }
  else
    data = page->raw_payload();

  uint32_t read_start = (uint32_t)(address - page->address());
  return &data[read_start];
}

static void
copy_chunk(DiskBlobManager *dbm, Context *context, Page *page, Page **ppage,
                uint64_t address, uint8_t *data, uint32_t size,
                bool fetch_read_only)
{
  uint32_t page_size = dbm->config->page_size_bytes;
  bool first_page = true;

  while (size) {
    // get the page-id from this chunk
    uint64_t pageid = address - (address % page_size);

    // is this the current page? if yes then continue working with this page,
    // otherwise fetch the page
    if (page && page->address() != pageid)
      page = 0;

    if (!page) {
      uint32_t flags = 0;
      if (fetch_read_only)
        flags |= PageManager::kReadOnly;
      if (!first_page)
        flags |= PageManager::kNoHeader;
      page = dbm->page_manager->fetch(context, pageid, flags);
    }

    // now read the data from the page
    uint32_t read_start = (uint32_t)(address - page->address());
    uint32_t read_size = (uint32_t)(page_size - read_start);
    if (read_size > size)
      read_size = size;
    ::memcpy(data, &page->raw_payload()[read_start], read_size);
    address += read_size;
    data += read_size;
    size -= read_size;

    first_page = false;
  }

  if (ppage)
    *ppage = page;
}

static void
write_chunks(DiskBlobManager *dbm, Context *context, Page *page,
                uint64_t address, uint8_t **chunk_data, uint32_t *chunk_size,
                uint32_t chunks)
{
  uint32_t page_size = dbm->config->page_size_bytes;

  // for each chunk...
  for (uint32_t i = 0; i < chunks; i++) {
    uint32_t size = chunk_size[i];
    uint8_t *data = chunk_data[i];

    while (size) {
      // get the page-id from this chunk
      uint64_t pageid = address - (address % page_size);

      // is this the current page? if yes then continue working with this page,
      // otherwise fetch the page
      if (page && page->address() != pageid)
        page = 0;
      if (!page)
        page = dbm->page_manager->fetch(context, pageid, PageManager::kNoHeader);

      uint32_t write_start = (uint32_t)(address - page->address());
      uint32_t write_size = (uint32_t)(page_size - write_start);

      // now write the data
      if (write_size > size)
        write_size = size;
      ::memmove(&page->raw_payload()[write_start], data, write_size);
      page->set_dirty(true);
      address += write_size;
      data += write_size;
      size -= write_size;
    }
  }
}

uint64_t
DiskBlobManager::allocate(Context *context, ups_record_t *record,
                uint32_t flags)
{
  metric_total_allocated++;

  uint8_t *chunk_data[2];
  uint32_t chunk_size[2];
  uint32_t page_size = config->page_size_bytes;

  void *record_data = record->data;
  uint32_t record_size = record->size;
  uint32_t original_size = record->size;

  // compression enabled? then try to compress the data
  Compressor *compressor = context->db->record_compressor.get();
  if (compressor && !(flags & kDisableCompression)) {
    metric_before_compression += record_size;
    uint32_t len = compressor->compress((uint8_t *)record->data,
                        record->size);
    if (len < record->size) {
      record_data = compressor->arena.data();
      record_size = len;
    }
    metric_after_compression += record_size;
  }
  PBlobHeader blob_header;
  uint32_t alloc_size = sizeof(PBlobHeader) + record_size;

  // first check if we can add another blob to the last used page
  Page *page = page_manager->last_blob_page(context);

  PBlobPageHeader *header = 0;
  uint64_t address = 0;
  if (page) {
    header = PBlobPageHeader::from_page(page);
    // allocate space for the blob
    if (!alloc_from_freelist(this, header, alloc_size, &address))
      page = 0;
    else
      address += page->address();
  }

  if (!address) {
    // Allocate a new page. If the blob exceeds a page then allocate multiple
    // pages that are directly next to each other.
    uint32_t required_size = alloc_size + kPageOverhead;
    uint32_t num_pages = required_size / page_size;
    if (num_pages * page_size < required_size)
      num_pages++;

    // |page| now points to the first page that was allocated, and
    // the only one which has a header and a freelist
    page = page_manager->alloc_multiple_blob_pages(context, num_pages);
    assert(page->is_without_header() == false);

    // initialize the PBlobPageHeader
    header = PBlobPageHeader::from_page(page);
    header->initialize();
    header->num_pages = num_pages;
    header->free_bytes = (num_pages * page_size) - kPageOverhead;

    // and move the remaining space to the freelist, unless we span multiple
    // pages (then the rest will be discarded) - TODO can we reuse it somehow?
    if (num_pages == 1
          && kPageOverhead + alloc_size > 0
          && header->free_bytes - alloc_size > 0) {
      header->freelist[0].offset = kPageOverhead + alloc_size;
      header->freelist[0].size = header->free_bytes - alloc_size;
    }

    // multi-page blobs store their CRC in the first freelist offset
    if (unlikely(num_pages > 1
            && (config->flags & UPS_ENABLE_CRC32))) {
      uint32_t crc32 = 0;
      MurmurHash3_x86_32(record->data, record->size, 0, &crc32);
      header->freelist[0].offset = crc32;
    }

    address = page->address() + kPageOverhead;
    assert(check_integrity(this, header));
  }

  // addjust "free bytes" counter
  assert(header->free_bytes >= alloc_size);
  header->free_bytes -= alloc_size;

  // store the page id if it still has space left
  if (header->free_bytes)
    page_manager->set_last_blob_page(page);
  else
    page_manager->set_last_blob_page(0);

  // initialize the blob header
  blob_header.allocated_size = alloc_size;
  blob_header.size = record->size;
  blob_header.blob_id = address;
  blob_header.flags = original_size != record_size
                            ? PBlobHeader::kIsCompressed
                            : 0;

  chunk_data[0] = (uint8_t *)&blob_header;
  chunk_size[0] = sizeof(blob_header);
  chunk_data[1] = (uint8_t *)record_data;
  chunk_size[1] = record_size;

  write_chunks(this, context, page, address, chunk_data, chunk_size, 2);
  address += chunk_size[0] + chunk_size[1];

  // store the blob_id; it will be returned to the caller
  uint64_t blob_id = blob_header.blob_id;
  assert(check_integrity(this, header));
  return blob_id;
}

void
DiskBlobManager::read(Context *context, uint64_t blob_id,
                ups_record_t *record, uint32_t flags, ByteArray *arena)
{
  metric_total_read++;

  // first step: read the blob header
  Page *page;
  PBlobHeader *blob_header = (PBlobHeader *)read_chunk(this, context, 0, &page,
                  blob_id, true, false);

  // sanity check
  if (unlikely(blob_header->blob_id != blob_id)) {
    ups_log(("blob %lld not found", blob_id));
    throw Exception(UPS_BLOB_NOT_FOUND);
  }

  uint32_t blobsize = (uint32_t)blob_header->size;
  record->size = blobsize;

  // empty blob?
  if (unlikely(!blobsize)) {
    record->data = 0;
    record->size = 0;
    return;
  }

  // if the blob is in memory-mapped storage (and the user does not require
  // a copy of the data): simply return a pointer
  if (NOTSET(flags, UPS_FORCE_DEEP_COPY)
        && device->is_mapped(blob_id, blobsize)
        && NOTSET(blob_header->flags, PBlobHeader::kIsCompressed)
        && NOTSET(record->flags, UPS_RECORD_USER_ALLOC)) {
    record->data = read_chunk(this, context, page, 0,
                        blob_id + sizeof(PBlobHeader), true, true);
  }
  // otherwise resize the blob buffer and copy the blob data into the buffer
  else {
    // read the blob data. if compression is enabled then
    // read into the Compressor's arena, otherwise read directly into the
    // caller's arena
    if (ISSET(blob_header->flags, PBlobHeader::kIsCompressed)) {
      Compressor *compressor = context->db->record_compressor.get();
      assert(compressor != 0);

      // read into temporary buffer; we reuse the compressor's memory arena
      // for this
      ByteArray *dest = &compressor->arena;
      dest->resize(blob_header->allocated_size - sizeof(PBlobHeader));

      copy_chunk(this, context, page, 0, blob_id + sizeof(PBlobHeader),
                    dest->data(),
                    blob_header->allocated_size - sizeof(PBlobHeader), true);

      // now uncompress into the caller's memory arena
      if (ISSET(record->flags, UPS_RECORD_USER_ALLOC)) {
        compressor->decompress(dest->data(),
                      blob_header->allocated_size - sizeof(PBlobHeader),
                      blobsize, (uint8_t *)record->data);
      }
      else {
        arena->resize(blobsize);
        compressor->decompress(dest->data(),
                      blob_header->allocated_size - sizeof(PBlobHeader),
                      blobsize, arena);
        record->data = arena->data();
      }
    }
    // if the data is uncompressed then allocate storage and read
    // into the allocated buffer
    else {
      if (NOTSET(record->flags, UPS_RECORD_USER_ALLOC)) {
        arena->resize(blobsize);
        record->data = arena->data();
      }

      copy_chunk(this, context, page, 0,
                  blob_id + sizeof(PBlobHeader),
                  (uint8_t *)record->data, blobsize, true);
    }
  }

  // multi-page blobs store their CRC in the first freelist offset
  PBlobPageHeader *header = PBlobPageHeader::from_page(page);
  if (unlikely(header->num_pages > 1
        && ISSET(config->flags, UPS_ENABLE_CRC32))) {
    uint32_t old_crc32 = header->freelist[0].offset;
    uint32_t new_crc32;
    MurmurHash3_x86_32(record->data, record->size, 0, &new_crc32);

    if (unlikely(old_crc32 != new_crc32)) {
      ups_trace(("crc32 mismatch in page %lu: 0x%lx != 0x%lx",
                      page->address(), old_crc32, new_crc32));
      throw Exception(UPS_INTEGRITY_VIOLATED);
    }
  }
}

uint32_t
DiskBlobManager::blob_size(Context *context, uint64_t blob_id)
{
  // read the blob header
  PBlobHeader *blob_header = (PBlobHeader *)read_chunk(this, context,
                  0, 0, blob_id, true, true);

  if (unlikely(blob_header->blob_id != blob_id))
    throw Exception(UPS_BLOB_NOT_FOUND);

  return blob_header->size;
}

uint64_t
DiskBlobManager::overwrite(Context *context, uint64_t old_blobid,
                ups_record_t *record, uint32_t flags)
{
  PBlobHeader *old_blob_header, new_blob_header;

  // This routine basically ignores compression. The likelyhood that a
  // compressed buffer has an identical size as the record that's overwritten,
  // is very small. In most cases this check will be false, and then
  // the record would be compressed again in do_allocate().
  //
  // As a consequence, the existing record is only overwritten if the
  // uncompressed record would fit in. Otherwise a new record is allocated,
  // and this one then is compressed.
  uint32_t alloc_size = sizeof(PBlobHeader) + record->size;

  // first, read the blob header; if the new blob fits into the
  // old blob, we overwrite the old blob (and add the remaining
  // space to the freelist, if there is any)
  Page *page;
  old_blob_header = (PBlobHeader *)read_chunk(this, context, 0, &page,
                    old_blobid, false, false);

  // sanity check
  if (unlikely(old_blob_header->blob_id != old_blobid))
    throw Exception(UPS_BLOB_NOT_FOUND);

  // now compare the sizes; does the new data fit in the old allocated
  // space?
  if (alloc_size <= old_blob_header->allocated_size) {
    uint8_t *chunk_data[2];
    uint32_t chunk_size[2];

    // setup the new blob header
    new_blob_header.blob_id = old_blob_header->blob_id;
    new_blob_header.size = record->size;
    new_blob_header.allocated_size = alloc_size;
    new_blob_header.flags = 0; // disable compression, just in case...

    chunk_data[0] = (uint8_t *)&new_blob_header;
    chunk_size[0] = sizeof(new_blob_header);
    chunk_data[1] = (uint8_t *)record->data;
    chunk_size[1] = record->size;

    write_chunks(this, context, page, new_blob_header.blob_id,
                    chunk_data, chunk_size, 2);

    PBlobPageHeader *header = PBlobPageHeader::from_page(page);

    // move remaining data to the freelist
    if (alloc_size < old_blob_header->allocated_size) {
      PBlobPageHeader *header = PBlobPageHeader::from_page(page);
      header->free_bytes += old_blob_header->allocated_size - alloc_size;
      add_to_freelist(this, header,
                  (uint32_t)(old_blobid + alloc_size) - page->address(),
                  (uint32_t)old_blob_header->allocated_size - alloc_size);
    }

    // multi-page blobs store their CRC in the first freelist offset
    if (unlikely(header->num_pages > 1
            && ISSET(config->flags, UPS_ENABLE_CRC32))) {
      uint32_t crc32 = 0;
      MurmurHash3_x86_32(record->data, record->size, 0, &crc32);
      header->freelist[0].offset = crc32;
    }

    // the old rid is the new rid
    return new_blob_header.blob_id;
  }

  // if the new data is larger: allocate a fresh space for it
  // and discard the old; 'overwrite' has become (delete + insert) now.
  uint64_t new_blobid = allocate(context, record, flags);
  erase(context, old_blobid, 0, 0);

  return new_blobid;
}

uint64_t
DiskBlobManager::overwrite_regions(Context *context, uint64_t old_blob_id,
                  ups_record_t *record, uint32_t flags,
                  Region *regions, size_t num_regions)
{
  assert(num_regions > 0);

  uint32_t page_size = config->page_size_bytes;
  uint32_t alloc_size = sizeof(PBlobHeader) + record->size;

  // only one page is written? then don't bother updating the regions
  if (alloc_size < page_size)
    return overwrite(context, old_blob_id, record, flags);

  // read the blob header
  Page *page;
  PBlobHeader *blob_header = (PBlobHeader *)read_chunk(this, context, 0, &page,
                  old_blob_id, false, false);

  // sanity check
  if (unlikely(blob_header->blob_id != old_blob_id)) {
    ups_log(("blob %lld not found", old_blob_id));
    throw Exception(UPS_BLOB_NOT_FOUND);
  }

  PBlobPageHeader *header = PBlobPageHeader::from_page(page);

  // only overwrite the regions if
  // - the blob does not grow
  // - blob is compressed
  if (alloc_size > blob_header->allocated_size
        || header->num_pages == 1
        || ISSET(blob_header->flags, PBlobHeader::kIsCompressed))
    return overwrite(context, old_blob_id, record, flags);

  uint8_t *chunk_data[2];
  uint32_t chunk_size[2];

  uint64_t address = old_blob_id;

  // setup the new blob header
  int c = 0;
  if (alloc_size != blob_header->allocated_size) {
    PBlobHeader new_blob_header;
    new_blob_header.blob_id = blob_header->blob_id;
    new_blob_header.size = record->size;
    new_blob_header.allocated_size = alloc_size;
    new_blob_header.flags = 0; // disable compression, just in case...

    chunk_data[c] = (uint8_t *)&new_blob_header;
    chunk_size[c] = sizeof(new_blob_header);
    c++;
  }

  address += sizeof(PBlobHeader);

  for (size_t i = 0; i < num_regions; i++) {
    chunk_data[c] = (uint8_t *)record->data + regions[i].offset;
    chunk_size[c] = regions[i].size;
    c++;

    write_chunks(this, context, page, address + regions[i].offset,
                    chunk_data, chunk_size, c);

    c = 0;
  }

  // move remaining data to the freelist
  if (alloc_size < blob_header->allocated_size) {
    header->free_bytes += blob_header->allocated_size - alloc_size;
    add_to_freelist(this, header,
                (uint32_t)(old_blob_id + alloc_size) - page->address(),
                (uint32_t)blob_header->allocated_size - alloc_size);
    page->set_dirty(true);
  }

  // multi-page blobs store their CRC in the first freelist offset
  if (unlikely(header->num_pages > 1
          && ISSET(config->flags, UPS_ENABLE_CRC32))) {
    uint32_t crc32 = 0;
    MurmurHash3_x86_32(record->data, record->size, 0, &crc32);
    header->freelist[0].offset = crc32;
    page->set_dirty(true);
  }

  // the old rid is the new rid
  return old_blob_id;
}

void
DiskBlobManager::erase(Context *context, uint64_t blob_id, Page *page,
                uint32_t flags)
{
  // fetch the blob header
  PBlobHeader *blob_header = (PBlobHeader *)read_chunk(this, context, 0, &page,
                        blob_id, false, false);

  if (unlikely(blob_header->blob_id != blob_id))
    throw Exception(UPS_BLOB_NOT_FOUND);

  // update the "free bytes" counter in the blob page header
  PBlobPageHeader *header = PBlobPageHeader::from_page(page);
  header->free_bytes += blob_header->allocated_size;

  // if the page is now completely empty (all blobs were erased) then move
  // it to the freelist
  if (header->free_bytes ==
            (header->num_pages * config->page_size_bytes) - kPageOverhead) {
    page_manager->set_last_blob_page(0);
    page_manager->del(context, page, header->num_pages);
    header->initialize();
    return;
  }

  // otherwise move the blob to the freelist
  add_to_freelist(this, header, (uint32_t)(blob_id - page->address()),
                  (uint32_t)blob_header->allocated_size);
}
