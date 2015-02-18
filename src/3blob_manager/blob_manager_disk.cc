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

#include <algorithm>
#include <vector>

// Always verify that a file of level N does not include headers > N!
#include "1base/error.h"
#include "1base/dynamic_array.h"
#include "2device/device.h"
#include "3blob_manager/blob_manager_disk.h"
#include "3page_manager/page_manager.h"
#include "4db/db_local.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

using namespace hamsterdb;

uint64_t
DiskBlobManager::do_allocate(Context *context, ham_record_t *record,
                uint32_t flags)
{
  uint8_t *chunk_data[2];
  uint32_t chunk_size[2];
  uint32_t page_size = m_env->config().page_size_bytes;

  PBlobHeader blob_header;
  uint32_t alloc_size = sizeof(PBlobHeader) + record->size;

  // first check if we can add another blob to the last used page
  Page *page = m_env->page_manager()->get_last_blob_page(context);

  PBlobPageHeader *header = 0;
  uint64_t address = 0;
  if (page) {
    header = PBlobPageHeader::from_page(page);
    // allocate space for the blob
    if (!alloc_from_freelist(header, alloc_size, &address))
      page = 0;
    else
      address += page->get_address();
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
    page = m_env->page_manager()->alloc_multiple_blob_pages(context, num_pages);
    ham_assert(page->is_without_header() == false);

    // initialize the PBlobPageHeader
    header = PBlobPageHeader::from_page(page);
    header->initialize();
    header->set_num_pages(num_pages);
    header->set_free_bytes((num_pages * page_size) - kPageOverhead);

    // and move the remaining space to the freelist, unless we span multiple
    // pages (then the rest will be discarded) - TODO can we reuse it somehow?
    if (num_pages == 1
          && kPageOverhead + alloc_size > 0
          && header->get_free_bytes() - alloc_size > 0) {
      header->set_freelist_offset(0, kPageOverhead + alloc_size);
      header->set_freelist_size(0, header->get_free_bytes() - alloc_size);
    }

    address = page->get_address() + kPageOverhead;
    ham_assert(check_integrity(header));
  }

  // addjust "free bytes" counter
  ham_assert(header->get_free_bytes() >= alloc_size);
  header->set_free_bytes(header->get_free_bytes() - alloc_size);

  // store the page id if it still has space left
  if (header->get_free_bytes())
    m_env->page_manager()->set_last_blob_page(page);
  else
    m_env->page_manager()->set_last_blob_page(0);

  // initialize the blob header
  blob_header.set_alloc_size(alloc_size);
  blob_header.set_size(record->size);
  blob_header.set_self(address);

  // PARTIAL WRITE
  //
  // Are there gaps at the beginning? If yes, then we'll fill with zeros
  ByteArray zeroes;
  if ((flags & HAM_PARTIAL) && (record->partial_offset > 0)) {
    uint32_t gapsize = record->partial_offset;

    // first: write the header
    chunk_data[0] = (uint8_t *)&blob_header;
    chunk_size[0] = sizeof(blob_header);
    write_chunks(context, page, address, chunk_data, chunk_size, 1);

    address += sizeof(blob_header);

    // now fill the gap; if the gap is bigger than a pagesize we'll
    // split the gap into smaller chunks
    while (gapsize) {
      uint32_t size = gapsize >= page_size
                          ? page_size
                          : gapsize;
      chunk_data[0] = (uint8_t *)zeroes.resize(size, 0);
      chunk_size[0] = size;
      write_chunks(context, page, address, chunk_data, chunk_size, 1);
      gapsize -= size;
      address += size;
    }

    // now write the "real" data
    chunk_data[0] = (uint8_t *)record->data;
    chunk_size[0] = record->partial_size;

    write_chunks(context, page, address, chunk_data, chunk_size, 1);
    address += record->partial_size;
  }
  else {
    // not writing partially: write header and data, then we're done
    chunk_data[0] = (uint8_t *)&blob_header;
    chunk_size[0] = sizeof(blob_header);
    chunk_data[1] = (uint8_t *)record->data;
    chunk_size[1] = (flags & HAM_PARTIAL)
                        ? record->partial_size
                        : record->size;

    write_chunks(context, page, address, chunk_data, chunk_size, 2);
    address += chunk_size[0] + chunk_size[1];
  }

  // store the blobid; it will be returned to the caller
  uint64_t blobid = blob_header.get_self();

  // PARTIAL WRITES:
  //
  // if we have gaps at the end of the blob: just append more chunks to
  // fill these gaps. Since they can be pretty large we split them into
  // smaller chunks if necessary.
  if (flags & HAM_PARTIAL) {
    if (record->partial_offset + record->partial_size < record->size) {
      uint32_t gapsize = record->size
                      - (record->partial_offset + record->partial_size);

      // now fill the gap; if the gap is bigger than a pagesize we'll
      // split the gap into smaller chunks
      //
      // we split this loop in two - the outer loop will allocate the
      // memory buffer, thus saving some allocations
      while (gapsize) {
        uint32_t size = gapsize > page_size
                            ? page_size
                            : gapsize;
        chunk_data[0] = (uint8_t *)zeroes.resize(size, 0);
        chunk_size[0] = size;
        write_chunks(context, page, address, chunk_data, chunk_size, 1);
        gapsize -= size;
        address += size;
      }
    }
  }

  ham_assert(check_integrity(header));

  return (blobid);
}

void
DiskBlobManager::do_read(Context *context, uint64_t blobid,
                ham_record_t *record, uint32_t flags, ByteArray *arena)
{
  Page *page;

  // first step: read the blob header
  PBlobHeader *blob_header = (PBlobHeader *)read_chunk(context, 0, &page,
                  blobid, true);

  // sanity check
  if (blob_header->get_self() != blobid) {
    ham_log(("blob %lld not found", blobid));
    throw Exception(HAM_BLOB_NOT_FOUND);
  }

  uint32_t blobsize = (uint32_t)blob_header->get_size();
  record->size = blobsize;

  if (flags & HAM_PARTIAL) {
    if (record->partial_offset > blobsize) {
      ham_trace(("partial offset is greater than the total record size"));
      throw Exception(HAM_INV_PARAMETER);
    }
    if (record->partial_offset + record->partial_size > blobsize)
      record->partial_size = blobsize = blobsize - record->partial_offset;
    else
      blobsize = record->partial_size;
  }

  // empty blob?
  if (!blobsize) {
    record->data = 0;
    record->size = 0;
    return;
  }

  // if the blob is in memory-mapped storage (and the user does not require
  // a copy of the data): simply return a pointer
  if ((flags & HAM_FORCE_DEEP_COPY) == 0
        && m_env->device()->is_mapped(blobid, blobsize)
        && !(record->flags & HAM_RECORD_USER_ALLOC)) {
    record->data = read_chunk(context, page, 0,
                        blobid + sizeof(PBlobHeader) + (flags & HAM_PARTIAL
                                ? record->partial_offset
                                : 0), true);
  }
  // otherwise resize the blob buffer and copy the blob data into the buffer
  else {
    if (!(record->flags & HAM_RECORD_USER_ALLOC)) {
      arena->resize(blobsize);
      record->data = arena->get_ptr();
    }

    copy_chunk(context, page, 0,
                  blobid + sizeof(PBlobHeader) + (flags & HAM_PARTIAL
                          ? record->partial_offset
                          : 0),
                  (uint8_t *)record->data, blobsize, true);
  }
}

uint64_t
DiskBlobManager::do_get_blob_size(Context *context, uint64_t blobid)
{
  // read the blob header
  PBlobHeader *blob_header = (PBlobHeader *)read_chunk(context, 0, 0, blobid,
                  true);

  if (blob_header->get_self() != blobid)
    throw Exception(HAM_BLOB_NOT_FOUND);

  return (blob_header->get_size());
}

uint64_t
DiskBlobManager::do_overwrite(Context *context, uint64_t old_blobid,
                ham_record_t *record, uint32_t flags)
{
  PBlobHeader *old_blob_header, new_blob_header;
  Page *page;

  uint32_t alloc_size = sizeof(PBlobHeader) + record->size;

  // first, read the blob header; if the new blob fits into the
  // old blob, we overwrite the old blob (and add the remaining
  // space to the freelist, if there is any)
  old_blob_header = (PBlobHeader *)read_chunk(context, 0, &page,
                    old_blobid, false);

  // sanity check
  ham_assert(old_blob_header->get_self() == old_blobid);
  if (old_blob_header->get_self() != old_blobid)
    throw Exception(HAM_BLOB_NOT_FOUND);

  // now compare the sizes; does the new data fit in the old allocated
  // space?
  if (alloc_size <= old_blob_header->get_alloc_size()) {
    uint8_t *chunk_data[2];
    uint32_t chunk_size[2];

    // setup the new blob header
    new_blob_header.set_self(old_blob_header->get_self());
    new_blob_header.set_size(record->size);
    new_blob_header.set_alloc_size(alloc_size);
    new_blob_header.set_flags(0); // disable compression, just in case...

    // PARTIAL WRITE
    //
    // if we have a gap at the beginning, then we have to write the
    // blob header and the blob data in two steps; otherwise we can
    // write both immediately
    if ((flags & HAM_PARTIAL) && (record->partial_offset)) {
      chunk_data[0] = (uint8_t *)&new_blob_header;
      chunk_size[0] = sizeof(new_blob_header);
      write_chunks(context, page, new_blob_header.get_self(),
                      chunk_data, chunk_size, 1);

      chunk_data[0] = (uint8_t *)record->data;
      chunk_size[0] = record->partial_size;
      write_chunks(context, page, new_blob_header.get_self()
                    + sizeof(new_blob_header) + record->partial_offset,
                      chunk_data, chunk_size, 1);
    }
    else {
      chunk_data[0] = (uint8_t *)&new_blob_header;
      chunk_size[0] = sizeof(new_blob_header);
      chunk_data[1] = (uint8_t *)record->data;
      chunk_size[1] = (flags & HAM_PARTIAL)
                          ? record->partial_size
                          : record->size;

      write_chunks(context, page, new_blob_header.get_self(),
                      chunk_data, chunk_size, 2);
    }

    // move remaining data to the freelist
    if (alloc_size < old_blob_header->get_alloc_size()) {
      PBlobPageHeader *header = PBlobPageHeader::from_page(page);
      header->set_free_bytes(header->get_free_bytes()
                  + (uint32_t)(old_blob_header->get_alloc_size() - alloc_size));
      add_to_freelist(header,
                  (uint32_t)(old_blobid + alloc_size) - page->get_address(),
                  (uint32_t)old_blob_header->get_alloc_size() - alloc_size);
    }

    // the old rid is the new rid
    return (new_blob_header.get_self());
  }

  // if the new data is larger: allocate a fresh space for it
  // and discard the old; 'overwrite' has become (delete + insert) now.
  uint64_t new_blobid = allocate(context, record, flags);
  erase(context, old_blobid, 0, 0);

  return (new_blobid);
}

void
DiskBlobManager::do_erase(Context *context, uint64_t blobid, Page *page,
                uint32_t flags)
{
  // fetch the blob header
  PBlobHeader *blob_header = (PBlobHeader *)read_chunk(context, 0, &page,
                        blobid, false);

  // sanity check
  ham_verify(blob_header->get_self() == blobid);
  if (blob_header->get_self() != blobid)
    throw Exception(HAM_BLOB_NOT_FOUND);

  // update the "free bytes" counter in the blob page header
  PBlobPageHeader *header = PBlobPageHeader::from_page(page);
  header->set_free_bytes(header->get_free_bytes()
                  + blob_header->get_alloc_size());

  // if the page is now completely empty (all blobs were erased) then move
  // it to the freelist
  if (header->get_free_bytes() == (header->get_num_pages()
              * m_env->config().page_size_bytes) - kPageOverhead) {
    m_env->page_manager()->set_last_blob_page(0);
    m_env->page_manager()->del(context, page, header->get_num_pages());
    header->initialize();
    return;
  }

  // otherwise move the blob to the freelist
  add_to_freelist(header, (uint32_t)(blobid - page->get_address()),
                  (uint32_t)blob_header->get_alloc_size());
}

bool
DiskBlobManager::alloc_from_freelist(PBlobPageHeader *header, uint32_t size,
                uint64_t *poffset)
{
  ham_assert(check_integrity(header));

  // freelist is not used if this is a multi-page blob
  if (header->get_num_pages() > 1)
    return (false);

  uint32_t count = header->get_freelist_entries();

  for (uint32_t i = 0; i < count; i++) {
    // exact match
    if (header->get_freelist_size(i) == size) {
      *poffset = header->get_freelist_offset(i);
      header->set_freelist_offset(i, 0);
      header->set_freelist_size(i, 0);
      ham_assert(check_integrity(header));
      return (true);
    }
    // space in freelist is larger than what we need? return this space,
    // make sure the remaining gap stays in the freelist
    if (header->get_freelist_size(i) > size) {
      *poffset = header->get_freelist_offset(i);
      header->set_freelist_offset(i, (uint32_t)(*poffset + size));
      header->set_freelist_size(i, header->get_freelist_size(i) - size);
      ham_assert(check_integrity(header));
      return (true);
    }
  }

  // there was no gap large enough for the blob
  return (false);
}

void
DiskBlobManager::add_to_freelist(PBlobPageHeader *header,
                uint32_t offset, uint32_t size)
{
  ham_assert(check_integrity(header));

  // freelist is not used if this is a multi-page blob
  if (header->get_num_pages() > 1)
    return;

  uint32_t count = header->get_freelist_entries();

  // first try to collapse the blobs
  for (uint32_t i = 0; i < count; i++) {
    if (offset + size == header->get_freelist_offset(i)) {
      header->set_freelist_offset(i, offset);
      header->set_freelist_size(i, header->get_freelist_size(i) + size);
      ham_assert(check_integrity(header));
      return;
    }
    if (header->get_freelist_offset(i) + header->get_freelist_size(i)
            == offset) {
      header->set_freelist_size(i, header->get_freelist_size(i) + size);
      ham_assert(check_integrity(header));
      return;
    }
  }

  // otherwise store the blob in a new slot, if available
  uint32_t smallest = 0;
  for (uint32_t i = 0; i < count; i++) {
    // slot is empty
    if (header->get_freelist_size(i) == 0) {
      header->set_freelist_offset(i, offset);
      header->set_freelist_size(i, size);
      ham_assert(check_integrity(header));
      return;
    }
    // otherwise look for the smallest entry
    if (header->get_freelist_size(i) < header->get_freelist_size(smallest)) {
      smallest = i;
      continue;
    }
  }

  // overwrite the smallest entry?
  if (size > header->get_freelist_size(smallest)) {
    header->set_freelist_offset(smallest, offset);
    header->set_freelist_size(smallest, size);
  }

  ham_assert(check_integrity(header));
}

bool
DiskBlobManager::check_integrity(PBlobPageHeader *header) const
{
  ham_assert(header->get_num_pages() > 0);

  if (header->get_free_bytes() + kPageOverhead
        > (m_env->config().page_size_bytes * header->get_num_pages())) {
    ham_trace(("integrity violated: free bytes exceeds page boundary"));
    return (false);
  }

  // freelist is not used if this is a multi-page blob
  if (header->get_num_pages() > 1)
    return (true);

  uint32_t count = header->get_freelist_entries();
  uint32_t total_sizes = 0;
  typedef std::pair<uint32_t, uint32_t> Range;
  typedef std::vector<Range> RangeVec;
  RangeVec ranges;

  for (uint32_t i = 0; i < count - 1; i++) {
    if (header->get_freelist_size(i) == 0) {
      ham_assert(header->get_freelist_offset(i) == 0);
      continue;
    }
    total_sizes += header->get_freelist_size(i);
    ranges.push_back(std::make_pair(header->get_freelist_offset(i),
                header->get_freelist_size(i)));
  }

  // the sum of freelist chunks must not exceed total number of free bytes
  if (total_sizes > header->get_free_bytes()) {
    ham_trace(("integrity violated: total freelist slots exceed free bytes"));
    return (false);
  }

  std::sort(ranges.begin(), ranges.end());

  if (!ranges.empty()) {
    for (uint32_t i = 0; i < ranges.size() - 1; i++) {
      if (ranges[i].first + ranges[i].second
          > m_env->config().page_size_bytes * header->get_num_pages()) {
        ham_trace(("integrity violated: freelist slot %u/%u exceeds page",
                    ranges[i].first, ranges[i].second));
        return (false);
      }
      if (ranges[i].first + ranges[i].second > ranges[i + 1].first) {
        ham_trace(("integrity violated: freelist slot %u/%u overlaps with %lu",
                    ranges[i].first, ranges[i].second,
                    ranges[i + 1].first));
        throw Exception(HAM_INTEGRITY_VIOLATED);
      }
    }
  }

  return (true);
}

void
DiskBlobManager::write_chunks(Context *context, Page *page,
                uint64_t address, uint8_t **chunk_data, uint32_t *chunk_size,
                uint32_t chunks)
{
  uint32_t page_size = m_env->config().page_size_bytes;

  // for each chunk...
  for (uint32_t i = 0; i < chunks; i++) {
    uint32_t size = chunk_size[i];
    uint8_t *data = chunk_data[i];

    while (size) {
      // get the page-id from this chunk
      uint64_t pageid = address - (address % page_size);

      // is this the current page? if yes then continue working with this page,
      // otherwise fetch the page
      if (page && page->get_address() != pageid)
        page = 0;
      if (!page)
        page = m_env->page_manager()->fetch(context, pageid,
                        PageManager::kNoHeader);

      uint32_t write_start = (uint32_t)(address - page->get_address());
      uint32_t write_size = (uint32_t)(page_size - write_start);

      // now write the data
      if (write_size > size)
        write_size = size;
      memcpy(&page->get_raw_payload()[write_start], data, write_size);
      page->set_dirty(true);
      address += write_size;
      data += write_size;
      size -= write_size;
    }
  }
}

void
DiskBlobManager::copy_chunk(Context *context, Page *page, Page **ppage,
                uint64_t address, uint8_t *data, uint32_t size,
                bool fetch_read_only)
{
  uint32_t page_size = m_env->config().page_size_bytes;
  bool first_page = true;

  while (size) {
    // get the page-id from this chunk
    uint64_t pageid = address - (address % page_size);

    // is this the current page? if yes then continue working with this page,
    // otherwise fetch the page
    if (page && page->get_address() != pageid)
      page = 0;

    if (!page) {
      uint32_t flags = 0;
      if (fetch_read_only)
        flags |= PageManager::kReadOnly;
      if (!first_page)
        flags |= PageManager::kNoHeader;
      page = m_env->page_manager()->fetch(context, pageid, flags);
    }

    // now read the data from the page
    uint32_t read_start = (uint32_t)(address - page->get_address());
    uint32_t read_size = (uint32_t)(page_size - read_start);
    if (read_size > size)
      read_size = size;
    memcpy(data, &page->get_raw_payload()[read_start], read_size);
    address += read_size;
    data += read_size;
    size -= read_size;

    first_page = false;
  }

  if (ppage)
    *ppage = page;
}

uint8_t *
DiskBlobManager::read_chunk(Context *context, Page *page, Page **ppage,
                uint64_t address, bool fetch_read_only)
{
  // get the page-id from this chunk
  uint32_t page_size = m_env->config().page_size_bytes;
  uint64_t pageid = address - (address % page_size);

  // is this the current page? if yes then continue working with this page,
  // otherwise fetch the page
  if (page && page->get_address() != pageid)
    page = 0;

  if (!page) {
    uint32_t flags = 0;
    if (fetch_read_only)
      flags |= PageManager::kReadOnly;
    page = m_env->page_manager()->fetch(context, pageid, flags);
    if (ppage)
      *ppage = page;
  }

  uint32_t read_start = (uint32_t)(address - page->get_address());
  return (&page->get_raw_payload()[read_start]);
}
