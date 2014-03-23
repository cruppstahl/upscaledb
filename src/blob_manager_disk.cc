/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 */

#include <algorithm>
#include <vector>

#include "config.h"

#include "device.h"
#include "error.h"
#include "page_manager.h"

#include "blob_manager_disk.h"

using namespace hamsterdb;


void
DiskBlobManager::write_chunks(LocalDatabase *db, Page *page, ham_u64_t address,
        ham_u8_t **chunk_data, ham_u32_t *chunk_size, ham_u32_t chunks)
{
  ham_u32_t page_size = m_env->get_page_size();

  // for each chunk...
  for (ham_u32_t i = 0; i < chunks; i++) {
    ham_u32_t size = chunk_size[i];
    ham_u8_t *data = chunk_data[i];

    while (size) {
      // get the page-id from this chunk
      ham_u64_t pageid = address - (address % page_size);

      // is this the current page? if yes then continue working with this page,
      // otherwise fetch the page
      if (page && page->get_address() != pageid)
        page = 0;
      if (!page)
        page = m_env->get_page_manager()->fetch_page(db, pageid);

      // now write the data
      ham_u32_t write_start = (ham_u32_t)(address - page->get_address());
      ham_u32_t write_size = (ham_u32_t)(page_size - write_start);
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
DiskBlobManager::read_chunk(Page *page, Page **ppage, ham_u64_t address,
            LocalDatabase *db, ham_u8_t *data, ham_u32_t size,
            bool fetch_read_only)
{
  ham_u32_t page_size = m_env->get_page_size();

  while (size) {
    // get the page-id from this chunk
    ham_u64_t pageid = address - (address % page_size);

    // is this the current page? if yes then continue working with this page,
    // otherwise fetch the page
    if (page && page->get_address() != pageid)
      page = 0;
    if (!page)
      page = m_env->get_page_manager()->fetch_page(db, pageid,
                        fetch_read_only ? PageManager::kReadOnly : 0);

    // now read the data from the page
    ham_u32_t read_start = (ham_u32_t)(address - page->get_address());
    ham_u32_t read_size = (ham_u32_t)(page_size - read_start);
    if (read_size > size)
      read_size = size;
    memcpy(data, &page->get_raw_payload()[read_start], read_size);
    address += read_size;
    data += read_size;
    size -= read_size;
  }

  if (ppage)
    *ppage = page;
}

ham_u64_t
DiskBlobManager::allocate(LocalDatabase *db, ham_record_t *record,
                ham_u32_t flags)
{
  ham_u32_t page_size = m_env->get_page_size();

  PBlobPageHeader *header = 0;
  ham_u64_t address = 0;
  ham_u8_t *chunk_data[2];
  ham_u32_t chunk_size[2];
  ByteArray zeroes;
  ham_u64_t blobid = 0;

  m_blob_total_allocated++;

  // PARTIAL WRITE
  //
  // if offset+partial_size equals the full record size, then we won't
  // have any gaps. In this case we just write the full record and ignore
  // the partial parameters.
  if (flags & HAM_PARTIAL) {
    if (record->partial_offset == 0 && record->partial_size == record->size)
      flags &= ~HAM_PARTIAL;
  }

  PBlobHeader blob_header;

  ham_u32_t alloc_size = sizeof(PBlobHeader) + record->size;

  // first check if we can add another blob to the last used page
  Page *page = m_env->get_page_manager()->get_last_blob_page(db);

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
    ham_u32_t required_size = alloc_size + kPageOverhead;
    ham_u32_t num_pages = required_size / page_size;
    if (num_pages * page_size < required_size)
      num_pages++;

    // |page| now points to the first page that was allocated, and
    // the only one which has a header and a freelist
    page = m_env->get_page_manager()->alloc_multiple_blob_pages(db, num_pages);

    // initialize the PBlobPageHeader
    header = PBlobPageHeader::from_page(page);
    header->initialize();
    header->set_num_pages(num_pages);
    header->set_free_bytes((num_pages * page_size) - kPageOverhead);

    // and move the remaining space to the freelist, unless we span multiple
    // pages (then the rest will be discarded) - TODO can we reuse it somehow?
    if (num_pages == 1 && kPageOverhead + alloc_size > 0) {
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
    m_env->get_page_manager()->set_last_blob_page(page);
  else
    m_env->get_page_manager()->set_last_blob_page(0);

  // initialize the blob header
  blob_header.set_alloc_size(alloc_size);
  blob_header.set_size(record->size);
  blob_header.set_self(address);

  // PARTIAL WRITE
  //
  // are there gaps at the beginning? If yes, then we'll fill with zeros
  if ((flags & HAM_PARTIAL) && (record->partial_offset > 0)) {
    ham_u32_t gapsize = record->partial_offset;

    // first: write the header
    chunk_data[0] = (ham_u8_t *)&blob_header;
    chunk_size[0] = sizeof(blob_header);
    write_chunks(db, page, address, chunk_data, chunk_size, 1);

    address += sizeof(blob_header);

    // now fill the gap; if the gap is bigger than a pagesize we'll
    // split the gap into smaller chunks
    while (gapsize) {
      ham_u32_t size = gapsize >= page_size
                          ? page_size
                          : gapsize;
      chunk_data[0] = (ham_u8_t *)zeroes.resize(size, 0);
      chunk_size[0] = size;
      write_chunks(db, page, address, chunk_data, chunk_size, 1);
      gapsize -= size;
      address += size;
    }

    // now write the "real" data
    chunk_data[0] = (ham_u8_t *)record->data;
    chunk_size[0] = record->partial_size;

    write_chunks(db, page, address, chunk_data, chunk_size, 1);
    address += record->partial_size;
  }
  else {
    // not writing partially: write header and data, then we're done
    chunk_data[0] = (ham_u8_t *)&blob_header;
    chunk_size[0] = sizeof(blob_header);
    chunk_data[1] = (ham_u8_t *)record->data;
    chunk_size[1] = (flags & HAM_PARTIAL)
                        ? record->partial_size
                        : record->size;

    write_chunks(db, page, address, chunk_data, chunk_size, 2);
    address += chunk_size[0] + chunk_size[1];
  }

  // store the blobid; it will be returned to the caller
  blobid = blob_header.get_self();

  // PARTIAL WRITES:
  //
  // if we have gaps at the end of the blob: just append more chunks to
  // fill these gaps. Since they can be pretty large we split them into
  // smaller chunks if necessary.
  if (flags & HAM_PARTIAL) {
    if (record->partial_offset + record->partial_size < record->size) {
      ham_u32_t gapsize = record->size
                      - (record->partial_offset + record->partial_size);

      // now fill the gap; if the gap is bigger than a pagesize we'll
      // split the gap into smaller chunks
      //
      // we split this loop in two - the outer loop will allocate the
      // memory buffer, thus saving some allocations
      while (gapsize) {
        ham_u32_t size = gapsize > page_size
                            ? page_size
                            : gapsize;
        chunk_data[0] = (ham_u8_t *)zeroes.resize(size, 0);
        chunk_size[0] = size;
        write_chunks(db, page, address, chunk_data, chunk_size, 1);
        gapsize -= size;
        address += size;
      }
    }
  }

  ham_assert(check_integrity(header));

  return (blobid);
}

void
DiskBlobManager::read(LocalDatabase *db, ham_u64_t blobid, ham_record_t *record,
        ham_u32_t flags, ByteArray *arena)
{
  m_blob_total_read++;

  Page *page;

  // first step: read the blob header
  PBlobHeader blob_header;
  read_chunk(0, &page, blobid, db, (ham_u8_t *)&blob_header,
          sizeof(blob_header), true);

  // sanity check
  if (blob_header.get_self() != blobid) {
    ham_log(("blob %lld not found", blobid));
    throw Exception(HAM_BLOB_NOT_FOUND);
  }

  ham_u32_t blobsize = (ham_u32_t)blob_header.get_size();

  record->size = blobsize;

  if (flags & HAM_PARTIAL) {
    if (record->partial_offset > blobsize) {
      ham_trace(("partial offset+size is greater than the total record size"));
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

  // second step: resize the blob buffer
  if (!(record->flags & HAM_RECORD_USER_ALLOC)) {
    arena->resize(blobsize);
    record->data = arena->get_ptr();
  }

  // third step: read the blob data
  read_chunk(page, 0,
                  blobid + sizeof(PBlobHeader) + (flags & HAM_PARTIAL
                          ? record->partial_offset
                          : 0),
                  db, (ham_u8_t *)record->data, blobsize, true);
}

ham_u64_t
DiskBlobManager::get_blob_size(LocalDatabase *db, ham_u64_t blobid)
{
  // read the blob header
  PBlobHeader blob_header;
  read_chunk(0, 0, blobid, db, (ham_u8_t *)&blob_header,
          sizeof(blob_header), true);

  if (blob_header.get_self() != blobid)
    throw Exception(HAM_BLOB_NOT_FOUND);

  return (blob_header.get_size());
}

ham_u64_t
DiskBlobManager::overwrite(LocalDatabase *db, ham_u64_t old_blobid,
                ham_record_t *record, ham_u32_t flags)
{
  PBlobHeader old_blob_header, new_blob_header;
  Page *page;

  // PARTIAL WRITE
  //
  // if offset+partial_size equals the full record size, then we won't
  // have any gaps. In this case we just write the full record and ignore
  // the partial parameters.
  if (flags & HAM_PARTIAL) {
    if (record->partial_offset == 0 && record->partial_size == record->size)
      flags &= ~HAM_PARTIAL;
  }

  ham_u32_t alloc_size = sizeof(PBlobHeader) + record->size;

  // first, read the blob header; if the new blob fits into the
  // old blob, we overwrite the old blob (and add the remaining
  // space to the freelist, if there is any)
  read_chunk(0, &page, old_blobid, db, (ham_u8_t *)&old_blob_header,
          sizeof(old_blob_header), false);

  // sanity check
  ham_assert(old_blob_header.get_self() == old_blobid);
  if (old_blob_header.get_self() != old_blobid)
    throw Exception(HAM_BLOB_NOT_FOUND);

  // now compare the sizes; does the new data fit in the old allocated
  // space?
  if (alloc_size <= old_blob_header.get_alloc_size()) {
    ham_u8_t *chunk_data[2];
    ham_u32_t chunk_size[2];

    // setup the new blob header
    new_blob_header.set_self(old_blob_header.get_self());
    new_blob_header.set_size(record->size);
    new_blob_header.set_alloc_size(alloc_size);

    // PARTIAL WRITE
    //
    // if we have a gap at the beginning, then we have to write the
    // blob header and the blob data in two steps; otherwise we can
    // write both immediately
    if ((flags & HAM_PARTIAL) && (record->partial_offset)) {
      chunk_data[0] = (ham_u8_t *)&new_blob_header;
      chunk_size[0] = sizeof(new_blob_header);
      write_chunks(db, page, new_blob_header.get_self(),
                      chunk_data, chunk_size, 1);

      chunk_data[0] = (ham_u8_t *)record->data;
      chunk_size[0] = record->partial_size;
      write_chunks(db, page, new_blob_header.get_self()
                    + sizeof(new_blob_header) + record->partial_offset,
                      chunk_data, chunk_size, 1);
    }
    else {
      chunk_data[0] = (ham_u8_t *)&new_blob_header;
      chunk_size[0] = sizeof(new_blob_header);
      chunk_data[1] = (ham_u8_t *)record->data;
      chunk_size[1] = (flags & HAM_PARTIAL)
                          ? record->partial_size
                          : record->size;

      write_chunks(db, page, new_blob_header.get_self(),
                      chunk_data, chunk_size, 2);
    }

    // move remaining data to the freelist
    if (alloc_size < old_blob_header.get_alloc_size()) {
      PBlobPageHeader *header = PBlobPageHeader::from_page(page);
      header->set_free_bytes(header->get_free_bytes()
                      + (old_blob_header.get_alloc_size() - alloc_size));
      add_to_freelist(header,
                      (old_blobid + alloc_size) - page->get_address(),
                      old_blob_header.get_alloc_size() - alloc_size);
    }

    // the old rid is the new rid
    return (new_blob_header.get_self());
  }

  // if the new data is larger: allocate a fresh space for it
  // and discard the old; 'overwrite' has become (delete + insert) now.
  ham_u64_t new_blobid = allocate(db, record, flags);
  erase(db, old_blobid, 0, 0);

  return (new_blobid);
}

void
DiskBlobManager::erase(LocalDatabase *db, ham_u64_t blobid, Page *page,
                ham_u32_t flags)
{
  // fetch the blob header
  PBlobHeader blob_header;
  read_chunk(0, &page, blobid, db, (ham_u8_t *)&blob_header,
                  sizeof(blob_header), false);

  // sanity check
  ham_verify(blob_header.get_self() == blobid);
  if (blob_header.get_self() != blobid)
    throw Exception(HAM_BLOB_NOT_FOUND);

  // update the "free bytes" counter in the blob page header
  PBlobPageHeader *header = PBlobPageHeader::from_page(page);
  header->set_free_bytes(header->get_free_bytes()
                  + blob_header.get_alloc_size());

  // if the page is now completely empty (all blobs were erased) then move
  // it to the freelist
  if (header->get_free_bytes() == (header->get_num_pages()
              * m_env->get_page_size()) - kPageOverhead) {
    m_env->get_page_manager()->set_last_blob_page(0);
    m_env->get_page_manager()->add_to_freelist(page, header->get_num_pages());
    header->initialize();
    return;
  }

  // otherwise move the blob to the freelist
  add_to_freelist(header, blobid - page->get_address(),
                  (ham_u32_t)blob_header.get_alloc_size());
}

bool
DiskBlobManager::alloc_from_freelist(PBlobPageHeader *header, ham_u32_t size,
                    ham_u64_t *poffset)
{
  ham_assert(check_integrity(header));

  // freelist is not used if this is a multi-page blob
  if (header->get_num_pages() > 1)
    return (false);

  ham_u32_t count = header->get_freelist_entries();

  for (ham_u32_t i = 0; i < count; i++) {
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
      header->set_freelist_offset(i, *poffset + size);
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
                ham_u32_t offset, ham_u32_t size)
{
  ham_assert(check_integrity(header));

  // freelist is not used if this is a multi-page blob
  if (header->get_num_pages() > 1)
    return;

  ham_u32_t count = header->get_freelist_entries();

  // first try to collapse the blobs
  for (ham_u32_t i = 0; i < count; i++) {
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
  ham_u32_t smallest = 0;
  for (ham_u32_t i = 0; i < count; i++) {
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
        > (m_env->get_page_size() * header->get_num_pages())) {
    ham_trace(("integrity violated: free bytes exceeds page boundary"));
    return (false);
  }

  // freelist is not used if this is a multi-page blob
  if (header->get_num_pages() > 1)
    return (true);

  ham_u32_t count = header->get_freelist_entries();
  ham_u32_t total_sizes = 0;
  typedef std::pair<ham_u32_t, ham_u32_t> Range;
  typedef std::vector<Range> RangeVec;
  RangeVec ranges;

  for (ham_u32_t i = 0; i < count - 1; i++) {
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
    for (ham_u32_t i = 0; i < ranges.size() - 1; i++) {
      if (ranges[i].first + ranges[i].second
          > m_env->get_page_size() * header->get_num_pages()) {
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
