/*
 * Copyright (C) 2005-2013 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 *
 */

#include "config.h"

#include "device.h"
#include "error.h"
#include "page_manager.h"
#include "log.h"
#include "freelist.h"

#include "blob_manager_disk.h"

using namespace hamsterdb;


#define SMALLEST_CHUNK_SIZE  (sizeof(ham_u64_t) + sizeof(PBlobHeader) + 1)

ham_status_t
DiskBlobManager::write_chunks(LocalDatabase *db, Page *page, ham_u64_t addr,
        bool allocated, bool freshly_created, ham_u8_t **chunk_data,
        ham_u32_t *chunk_size, ham_u32_t chunks)
{
  ham_status_t st;
  ham_u32_t pagesize = m_env->get_pagesize();

  ham_assert(freshly_created ? allocated : 1);

  // for each chunk...
  for (ham_u32_t i = 0; i < chunks; i++) {
    while (chunk_size[i]) {
      // get the page-id from this chunk
      ham_u64_t pageid = addr - (addr % pagesize);

      // is this the current page? if yes then continue working with this page
      if (page && page->get_address() != pageid)
        page = 0;

      if (!page) {
        // we need to fetch a new page
        //
        // only fetch the page from cache if the blob is small enough.
        // otherwise, if the page is not cached, directly write to the file.
        //
        // exception: if logging is enabled then always use the cache.
        bool at_blob_edge = (blob_from_cache(chunk_size[i])
                            || (addr % pagesize) != 0
                            || chunk_size[i] < pagesize);
        bool cache_only = (!at_blob_edge
                            && (!m_env->get_log()
                                || freshly_created));

        st = m_env->get_page_manager()->fetch_page(&page, db,
                            pageid, cache_only);
        /* blob pages don't have a page header */
        if (page)
          page->set_flags(page->get_flags() | Page::kNpersNoHeader);
        else if (st)
          return (st);
      }

      // if we have a page pointer: use it; otherwise write directly
      // to the device
      if (page) {
        ham_u32_t writestart = (ham_u32_t)(addr - page->get_address());
        ham_u32_t writesize = (ham_u32_t)(pagesize - writestart);
        if (writesize > chunk_size[i])
          writesize = chunk_size[i];
        memcpy(&page->get_raw_payload()[writestart], chunk_data[i], writesize);
        page->set_dirty(true);
        addr += writesize;
        chunk_data[i] += writesize;
        chunk_size[i] -= writesize;
      }
      else {
        ham_u32_t s = chunk_size[i];
        // limit to the next page boundary
        if (s > pageid + pagesize - addr)
          s = (ham_u32_t)(pageid + pagesize - addr);

        m_blob_direct_written++;

        st = m_env->get_device()->write(addr, chunk_data[i], s);
        if (st)
          return st;
        addr += s;
        chunk_data[i] += s;
        chunk_size[i] -= s;
      }
    }
  }

  return (0);
}

ham_status_t
DiskBlobManager::read_chunk(Page *page, Page **fpage, ham_u64_t addr,
        LocalDatabase *db, ham_u8_t *data, ham_u32_t size)
{
  ham_status_t st;
  ham_u32_t pagesize = m_env->get_pagesize();

  while (size) {
    // get the page-id from this chunk
    ham_u64_t pageid = addr - (addr % pagesize);

    if (page && page->get_address() != pageid)
      page = 0;

    // is it the current page? if not, try to fetch the page from
    // the cache - but only read the page from disk if the
    // chunk is small
    if (!page) {
      st = m_env->get_page_manager()->fetch_page(&page, db,
                          pageid, !blob_from_cache(size));
      if (st)
        return st;
      // blob pages don't have a page header
      if (page)
        page->set_flags(page->get_flags() | Page::kNpersNoHeader);
    }

    // if we have a page pointer: use it; otherwise read directly
    // from the device
    if (page) {
      ham_u32_t readstart = (ham_u32_t)(addr - page->get_address());
      ham_u32_t readsize = (ham_u32_t)(pagesize - readstart);
      if (readsize > size)
        readsize = size;
      memcpy(data, &page->get_raw_payload()[readstart], readsize);
      addr += readsize;
      data += readsize;
      size -= readsize;
    }
    else {
      ham_u32_t s = (size < pagesize
                    ? size
                    : m_env->get_pagesize());
      // limit to the next page boundary
      if (s > pageid + pagesize - addr)
        s = (ham_u32_t)(pageid + pagesize - addr);

      m_blob_direct_read++;

      st = m_env->get_device()->read(addr, data, s);
      if (st)
        return st;
      addr += s;
      data += s;
      size -= s;
    }
  }

  if (fpage)
    *fpage = page;

  return (0);
}

ham_status_t
DiskBlobManager::allocate(LocalDatabase *db, ham_record_t *record,
                ham_u32_t flags, ham_u64_t *blobid)
{
  m_blob_total_allocated++;

  Page *page = 0;
  ham_u64_t addr = 0;
  ham_u8_t *chunk_data[2];
  ham_u32_t alloc_size;
  ham_u32_t chunk_size[2];
  bool freshly_created = false;
  ByteArray zeroes;

  *blobid = 0;

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

  // blobs are aligned!
  alloc_size = sizeof(PBlobHeader) + record->size;
  alloc_size += Freelist::kBlobAlignment - 1;
  alloc_size -= alloc_size % Freelist::kBlobAlignment;

  // check if we have space in the freelist
  bool tmp;
  ham_status_t st = m_env->get_page_manager()->alloc_blob(db,
                  alloc_size, &addr, &tmp);
  if (st)
    return (st);

  if (!addr) {
    // if the blob is small AND if logging is disabled: load the page
    // through the cache
    if (blob_from_cache(alloc_size)) {
      st = m_env->get_page_manager()->alloc_page(&page, db, Page::kTypeBlob,
                      PageManager::kIgnoreFreelist);
      if (st)
          return (st);
      // blob pages don't have a page header
      page->set_flags(page->get_flags() | Page::kNpersNoHeader);
      addr = page->get_address();
      // move the remaining space to the freelist
      m_env->get_page_manager()->add_to_freelist(db, addr + alloc_size,
                    m_env->get_pagesize() - alloc_size);
      blob_header.set_alloc_size(alloc_size);
    }
    else {
      // otherwise use direct IO to allocate the space
      ham_u32_t aligned = alloc_size;
      aligned += m_env->get_pagesize() - 1;
      aligned -= aligned % m_env->get_pagesize();

      m_blob_direct_allocated++;

      st = m_env->get_device()->alloc(aligned, &addr);
      if (st)
          return (st);

      // if aligned!=size, and the remaining chunk is large enough:
      // move it to the freelist
      {
        ham_u32_t diff = aligned - alloc_size;
        if (diff > SMALLEST_CHUNK_SIZE) {
          m_env->get_page_manager()->add_to_freelist(db,
                  addr + alloc_size, diff);
          blob_header.set_alloc_size(aligned - diff);
        }
        else {
          blob_header.set_alloc_size(aligned);
        }
      }
      freshly_created = true;
    }
  }
  else {
    ham_assert(st == 0);
    blob_header.set_alloc_size(alloc_size);
  }

  blob_header.set_size(record->size);
  blob_header.set_self(addr);

  // PARTIAL WRITE
  //
  // are there gaps at the beginning? If yes, then we'll fill with zeros
  if ((flags & HAM_PARTIAL) && (record->partial_offset)) {
    ham_u32_t gapsize = record->partial_offset;

    ham_u8_t *ptr = (ham_u8_t *)zeroes.resize(gapsize > m_env->get_pagesize()
                          ? m_env->get_pagesize()
                          : gapsize,
                       0);
    if (!ptr)
      return (HAM_OUT_OF_MEMORY);

    // first: write the header
    chunk_data[0] = (ham_u8_t *)&blob_header;
    chunk_size[0] = sizeof(blob_header);
    st = write_chunks(db, page, addr, true, freshly_created,
                    chunk_data, chunk_size, 1);
    if (st)
      return (st);

    addr += sizeof(blob_header);

    // now fill the gap; if the gap is bigger than a pagesize we'll
    // split the gap into smaller chunks
    while (gapsize >= m_env->get_pagesize()) {
      chunk_data[0] = ptr;
      chunk_size[0] = m_env->get_pagesize();
      st = write_chunks(db, page, addr, true, freshly_created,
                        chunk_data, chunk_size, 1);
      if (st)
        break;
      gapsize -= m_env->get_pagesize();
      addr += m_env->get_pagesize();
    }

    // fill the remaining gap
    if (gapsize) {
      chunk_data[0] = ptr;
      chunk_size[0] = gapsize;

      st = write_chunks(db, page, addr, true, freshly_created,
                      chunk_data, chunk_size, 1);
      if (st)
        return (st);
      addr += gapsize;
    }

    // now write the "real" data
    chunk_data[0] = (ham_u8_t *)record->data;
    chunk_size[0] = record->partial_size;

    st = write_chunks(db, page, addr, true, freshly_created,
                    chunk_data, chunk_size, 1);
    if (st)
      return (st);
    addr += record->partial_size;
  }
  else {
    // not writing partially: write header and data, then we're done
    chunk_data[0] = (ham_u8_t *)&blob_header;
    chunk_size[0] = sizeof(blob_header);
    chunk_data[1] = (ham_u8_t *)record->data;
    chunk_size[1] = (flags & HAM_PARTIAL)
                        ? record->partial_size
                        : record->size;

    st = write_chunks(db, page, addr, true, freshly_created,
                    chunk_data, chunk_size, 2);
    if (st)
      return (st);
    addr += sizeof(blob_header) + ((flags&HAM_PARTIAL)
                              ? record->partial_size
                              : record->size);
  }

  // store the blobid; it will be returned to the caller
  *blobid = blob_header.get_self();

  // PARTIAL WRITES:
  //
  // if we have gaps at the end of the blob: just append more chunks to
  // fill these gaps. Since they can be pretty large we split them into
  // smaller chunks if necessary.
  if (flags & HAM_PARTIAL) {
    if (record->partial_offset + record->partial_size < record->size) {
        ham_u8_t *ptr;
        ham_u32_t gapsize = record->size
                        - (record->partial_offset + record->partial_size);

        // now fill the gap; if the gap is bigger than a pagesize we'll
        // split the gap into smaller chunks
        //
        // we split this loop in two - the outer loop will allocate the
        // memory buffer, thus saving some allocations
        while (gapsize > m_env->get_pagesize()) {
          ptr = (ham_u8_t *)zeroes.resize(m_env->get_pagesize(), 0);
          if (!ptr)
            return (HAM_OUT_OF_MEMORY);

          while (gapsize > m_env->get_pagesize()) {
            chunk_data[0] = ptr;
            chunk_size[0] = m_env->get_pagesize();
            st = write_chunks(db, page, addr, true,
                          freshly_created, chunk_data, chunk_size, 1);
            if (st)
              break;
            gapsize -= m_env->get_pagesize();
            addr += m_env->get_pagesize();
          }
          if (st)
            return (st);
        }

        // now write the remainder, which is less than a pagesize
        ham_assert(gapsize < m_env->get_pagesize());

        ptr = chunk_data[0] = (ham_u8_t *)zeroes.resize(gapsize, 0);
        if (!ptr)
          return (HAM_OUT_OF_MEMORY);
        chunk_size[0] = gapsize;

        st = write_chunks(db, page, addr, true, freshly_created,
                    chunk_data, chunk_size, 1);
        if (st)
          return (st);
    }
  }

  return (0);
}

ham_status_t
DiskBlobManager::read(LocalDatabase *db, ham_u64_t blobid, ham_record_t *record,
        ham_u32_t flags, ByteArray *arena)
{
  m_blob_total_read++;

  Page *page;

  ham_assert(blobid % Freelist::kBlobAlignment == 0);

  // first step: read the blob header
  PBlobHeader blob_header;
  ham_status_t st = read_chunk(0, &page, blobid, db, (ham_u8_t *)&blob_header,
          sizeof(blob_header));
  if (st)
    return (st);

  ham_assert(blob_header.get_alloc_size() % Freelist::kBlobAlignment == 0);

  // sanity check
  if (blob_header.get_self() != blobid) {
    ham_log(("blob %lld not found", blobid));
    return (HAM_BLOB_NOT_FOUND);
  }

  ham_u32_t blobsize = (ham_u32_t)blob_header.get_size();

  record->size = blobsize;

  if (flags & HAM_PARTIAL) {
    if (record->partial_offset > blobsize) {
      ham_trace(("partial offset+size is greater than the total record size"));
      return (HAM_INV_PARAMETER);
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
    return (0);
  }

  // second step: resize the blob buffer
  if (!(record->flags & HAM_RECORD_USER_ALLOC)) {
    arena->resize(blobsize);
    record->data = arena->get_ptr();
  }

  // third step: read the blob data
  st = read_chunk(page, 0,
                  blobid + sizeof(PBlobHeader) + (flags & HAM_PARTIAL
                          ? record->partial_offset
                          : 0),
                  db, (ham_u8_t *)record->data, blobsize);
  if (st)
    return (st);

  return (0);
}

ham_status_t
DiskBlobManager::get_datasize(LocalDatabase *db, ham_u64_t blobid,
                ham_u64_t *size)
{
  *size = 0;

  ham_assert(blobid % Freelist::kBlobAlignment == 0);

  // read the blob header
  PBlobHeader blob_header;
  ham_status_t st = read_chunk(0, 0, blobid, db,
          (ham_u8_t *)&blob_header, sizeof(blob_header));
  if (st)
    return (st);

  if (blob_header.get_self() != blobid)
    return (HAM_BLOB_NOT_FOUND);

  *size = blob_header.get_size();
  return (0);
}

ham_status_t
DiskBlobManager::overwrite(LocalDatabase *db, ham_u64_t old_blobid,
                ham_record_t *record, ham_u32_t flags, ham_u64_t *new_blobid)
{
  ham_status_t st;
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

  // blobs are aligned!
  ham_u32_t alloc_size = sizeof(PBlobHeader) + record->size;
  alloc_size += Freelist::kBlobAlignment - 1;
  alloc_size -= alloc_size % Freelist::kBlobAlignment;

  ham_assert(old_blobid % Freelist::kBlobAlignment == 0);

  // first, read the blob header; if the new blob fits into the
  // old blob, we overwrite the old blob (and add the remaining
  // space to the freelist, if there is any)
  st = read_chunk(0, &page, old_blobid, db,
                  (ham_u8_t *)&old_blob_header, sizeof(old_blob_header));
  if (st)
    return (st);

  ham_assert(old_blob_header.get_alloc_size() % Freelist::kBlobAlignment == 0);

  // sanity check
  ham_assert(old_blob_header.get_self() == old_blobid);
  if (old_blob_header.get_self() != old_blobid)
    return (HAM_BLOB_NOT_FOUND);

  // now compare the sizes; does the new data fit in the old allocated
  // space?
  if (alloc_size <= old_blob_header.get_alloc_size()) {
    ham_u8_t *chunk_data[2];
    ham_u32_t chunk_size[2];

    // setup the new blob header
    new_blob_header.set_self(old_blob_header.get_self());
    new_blob_header.set_size(record->size);
    if (old_blob_header.get_alloc_size() - alloc_size > SMALLEST_CHUNK_SIZE)
      new_blob_header.set_alloc_size(alloc_size);
    else
      new_blob_header.set_alloc_size(old_blob_header.get_alloc_size());

    // PARTIAL WRITE
    //
    // if we have a gap at the beginning, then we have to write the
    // blob header and the blob data in two steps; otherwise we can
    // write both immediately
    if ((flags & HAM_PARTIAL) && (record->partial_offset)) {
      chunk_data[0] = (ham_u8_t *)&new_blob_header;
      chunk_size[0] = sizeof(new_blob_header);
      st = write_chunks(db, page, new_blob_header.get_self(), false,
                        false, chunk_data, chunk_size, 1);
      if (st)
        return (st);

      chunk_data[0] = (ham_u8_t *)record->data;
      chunk_size[0] = record->partial_size;
      st = write_chunks(db, page, new_blob_header.get_self()
                    + sizeof(new_blob_header) + record->partial_offset,
                    false, false, chunk_data, chunk_size, 1);
      if (st)
        return (st);
    }
    else {
      chunk_data[0] = (ham_u8_t *)&new_blob_header;
      chunk_size[0] = sizeof(new_blob_header);
      chunk_data[1] = (ham_u8_t *)record->data;
      chunk_size[1] = (flags & HAM_PARTIAL)
                          ? record->partial_size
                          : record->size;

      st = write_chunks(db, page, new_blob_header.get_self(), false,
                            false, chunk_data, chunk_size, 2);
      if (st)
        return (st);
    }

    // move remaining data to the freelist
    if (old_blob_header.get_alloc_size() != new_blob_header.get_alloc_size()) {
      m_env->get_page_manager()->add_to_freelist(db,
                  new_blob_header.get_self() + new_blob_header.get_alloc_size(),
                  (ham_u32_t)(old_blob_header.get_alloc_size() -
                        new_blob_header.get_alloc_size()));
    }

    // the old rid is the new rid
    *new_blobid = new_blob_header.get_self();
  }
  else {
    // when the new data is larger, allocate a fresh space for it
    // and discard the old; 'overwrite' has become (delete + insert) now.
    st = allocate(db, record, flags, new_blobid);
    if (st)
      return (st);

    m_env->get_page_manager()->add_to_freelist(db, old_blobid,
                  (ham_u32_t)old_blob_header.get_alloc_size());
  }

  return (HAM_SUCCESS);
}

ham_status_t
DiskBlobManager::free(LocalDatabase *db, ham_u64_t blobid, Page *page,
                ham_u32_t flags)
{
  ham_assert(blobid % Freelist::kBlobAlignment == 0);

  // fetch the blob header
  PBlobHeader blob_header;
  ham_status_t st = read_chunk(0, 0, blobid, db,
          (ham_u8_t *)&blob_header, sizeof(blob_header));
  if (st)
    return (st);

  ham_assert(blob_header.get_alloc_size() % Freelist::kBlobAlignment == 0);

  // sanity check
  ham_verify(blob_header.get_self() == blobid);
  if (blob_header.get_self() != blobid)
    return (HAM_BLOB_NOT_FOUND);

  // move the blob to the freelist
  m_env->get_page_manager()->add_to_freelist(db, blobid,
                (ham_u32_t)blob_header.get_alloc_size());
  return (0);
}

