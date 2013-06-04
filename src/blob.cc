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

#include <string.h>

#include "blob.h"
#include "db.h"
#include "device.h"
#include "env.h"
#include "error.h"
#include "log.h"
#include "mem.h"
#include "page.h"
#include "txn.h"
#include "btree.h"
#include "btree_key.h"

using namespace hamsterdb;


#define SMALLEST_CHUNK_SIZE  (sizeof(ham_u64_t) + sizeof(PBlobHeader) + 1)

ham_status_t
InMemoryBlobManager::allocate(Database *db, ham_record_t *record,
                    ham_u32_t flags, ham_u64_t *blobid)
{
  /*
   * PARTIAL WRITE
   *
   * if offset+partial_size equals the full record size, then we won't
   * have any gaps. In this case we just write the full record and ignore
   * the partial parameters.
   */
  if (flags & HAM_PARTIAL) {
    if (record->partial_offset == 0
        && record->partial_offset + record->partial_size == record->size)
      flags &= ~HAM_PARTIAL;
  }

  /*
   * in-memory-database: the blobid is actually a pointer to the memory
   * buffer, in which the blob (with the blob-header) is stored
   */
  ham_u8_t *p = (ham_u8_t *)m_env->get_allocator()->alloc(
                              record->size + sizeof(PBlobHeader));
  if (!p)
    return (HAM_OUT_OF_MEMORY);

  /* initialize the header */
  PBlobHeader *blob_header = (PBlobHeader *)p;
  memset(blob_header, 0, sizeof(*blob_header));
  blob_header->set_self((ham_u64_t)PTR_TO_U64(p));
  blob_header->set_alloc_size(record->size + sizeof(PBlobHeader));
  blob_header->set_size(record->size);

  /* do we have gaps? if yes, fill them with zeroes */
  if (flags & HAM_PARTIAL) {
    ham_u8_t *s = p + sizeof(PBlobHeader);
    if (record->partial_offset)
      memset(s, 0, record->partial_offset);
    memcpy(s + record->partial_offset, record->data, record->partial_size);
    if (record->partial_offset + record->partial_size < record->size)
      memset(s + record->partial_offset + record->partial_size, 0,
              record->size - (record->partial_offset + record->partial_size));
  }
  else {
    memcpy(p + sizeof(PBlobHeader), record->data, record->size);
  }

  *blobid = (ham_u64_t)PTR_TO_U64(p);
  return (0);
}

ham_status_t
InMemoryBlobManager::read(Database *db, Transaction *txn, ham_u64_t blobid,
                    ham_record_t *record, ham_u32_t flags)
{
  ByteArray *arena = (txn == 0 || (txn->get_flags() & HAM_TXN_TEMPORARY))
                          ? &db->get_record_arena()
                          : &txn->get_record_arena();

  /*
   * in-memory-database: the blobid is actually a pointer to the memory
   * buffer, in which the blob is stored
   */
  PBlobHeader *blob_header = (PBlobHeader *)U64_TO_PTR(blobid);
  ham_u8_t *data = (ham_u8_t *)(U64_TO_PTR(blobid)) + sizeof(PBlobHeader);

  /* when the database is closing, the header is already deleted */
  if (!blob_header) {
    record->size = 0;
    return (0);
  }

  ham_size_t blobsize = (ham_size_t)blob_header->get_size();

  if (flags & HAM_PARTIAL) {
    if (record->partial_offset > blobsize) {
      ham_trace(("partial offset is greater than the total record size"));
      return (HAM_INV_PARAMETER);
    }
    if (record->partial_offset + record->partial_size > blobsize)
      blobsize = blobsize-record->partial_offset;
    else
      blobsize = record->partial_size;
  }

  if (!blobsize) {
    /* empty blob? */
    record->data = 0;
    record->size = 0;
  }
  else {
    ham_u8_t *d = data;
    if (flags & HAM_PARTIAL)
      d += record->partial_offset;

    if ((flags & HAM_DIRECT_ACCESS)
          && !(record->flags & HAM_RECORD_USER_ALLOC)) {
      record->size = blobsize;
      record->data = d;
    }
    else {
      /* resize buffer if necessary */
      if (!(record->flags & HAM_RECORD_USER_ALLOC)) {
          arena->resize(blobsize);
          record->data = arena->get_ptr();
      }
      /* and copy the data */
      memcpy(record->data, d, blobsize);
      record->size = blobsize;
    }
  }

  return (0);
}

ham_status_t
InMemoryBlobManager::get_datasize(Database *db, ham_u64_t blobid,
                    ham_u64_t *size)
{
  PBlobHeader *blob_header = (PBlobHeader *)U64_TO_PTR(blobid);
  *size = (ham_size_t)blob_header->get_size();
  return (0);
}

ham_status_t
InMemoryBlobManager::overwrite(Database *db, ham_u64_t old_blobid,
                    ham_record_t *record, ham_u32_t flags,
                    ham_u64_t *new_blobid)
{
  /*
   * PARTIAL WRITE
   *
   * if offset+partial_size equals the full record size, then we won't
   * have any gaps. In this case we just write the full record and ignore
   * the partial parameters.
   */
  if (flags & HAM_PARTIAL) {
    if (record->partial_offset == 0
          && record->partial_offset + record->partial_size == record->size)
      flags &= ~HAM_PARTIAL;
  }

  /*
   * free the old blob, allocate a new blob (but if both sizes are equal,
   * just overwrite the data)
   */
  PBlobHeader *phdr = (PBlobHeader *)U64_TO_PTR(old_blobid);

  if (phdr->get_size() == record->size) {
    ham_u8_t *p = (ham_u8_t *)phdr;
    if (flags & HAM_PARTIAL) {
      memmove(p + sizeof(PBlobHeader) + record->partial_offset,
              record->data, record->partial_size);
    }
    else {
      memmove(p + sizeof(PBlobHeader), record->data, record->size);
    }
    *new_blobid = (ham_u64_t)PTR_TO_U64(phdr);
  }
  else {
    ham_status_t st = m_env->get_blob_manager()->allocate(db, record,
            flags, new_blobid);
    if (st)
      return (st);

    m_env->get_allocator()->free(phdr);
  }

  return (HAM_SUCCESS);
}

ham_status_t
InMemoryBlobManager::free(Database *db, ham_u64_t blobid,
                    Page *page, ham_u32_t flags)
{
  m_env->get_allocator()->free((void *)U64_TO_PTR(blobid));
  return (0);
}

bool
DiskBlobManager::blob_from_cache(ham_size_t size)
{
  if (m_env->get_log())
    return (size < (m_env->get_usable_pagesize()));
  return (size < (ham_size_t)(m_env->get_pagesize() >> 3));
}

ham_status_t
DiskBlobManager::write_chunks(Database *db, Page *page, ham_u64_t addr,
        bool allocated, bool freshly_created, ham_u8_t **chunk_data,
        ham_size_t *chunk_size, ham_size_t chunks)
{
  ham_status_t st;
  Device *device = m_env->get_device();
  ham_size_t pagesize = m_env->get_pagesize();

  ham_assert(freshly_created ? allocated : 1);

  /* for each chunk...  */
  for (ham_size_t i = 0; i < chunks; i++) {
    while (chunk_size[i]) {
      /* get the page-ID from this chunk */
      ham_u64_t pageid = addr - (addr % pagesize);

      /* is this the current page? */
      if (page && page->get_self() != pageid)
        page = 0;

      /*
       * fetch the page from the cache, if it's in the cache
       * (unless we're logging - in this case always go through
       * the buffered routines)
       */
      if (!page) {
        /*
         * keep pages in cache when they are located at the 'edges' of
         * the blob, as they MAY be accessed for different data.
         * Of course, when a blob is small, there's only one (partial)
         * page accessed anyhow, so that one should end up in cache
         * then.
         */
        bool at_blob_edge = (blob_from_cache(chunk_size[i])
                            || (addr % pagesize) != 0
                            || chunk_size[i] < pagesize);
        bool cacheonly = (!at_blob_edge
                            && (!m_env->get_log()
                                || freshly_created));

        st = m_env->get_page_manager()->fetch_page(&page, db,
                            pageid, cacheonly);
        /* blob pages don't have a page header */
        if (page)
          page->set_flags(page->get_flags() | Page::NPERS_NO_HEADER);
        else if (st)
          return (st);
      }

      /*
       * if we have a page pointer: use it; otherwise write directly
       * to the device
       */
      if (page) {
        ham_size_t writestart = (ham_size_t)(addr - page->get_self());
        ham_size_t writesize = (ham_size_t)(pagesize - writestart);
        if (writesize > chunk_size[i])
          writesize = chunk_size[i];
        memcpy(&page->get_raw_payload()[writestart], chunk_data[i], writesize);
        page->set_dirty(true);
        addr += writesize;
        chunk_data[i] += writesize;
        chunk_size[i] -= writesize;
      }
      else {
        ham_size_t s = chunk_size[i];
        /* limit to the next page boundary */
        if (s > pageid + pagesize - addr)
          s = (ham_size_t)(pageid + pagesize - addr);

        st = device->write(addr, chunk_data[i], s);
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
        Database *db, ham_u8_t *data, ham_size_t size)
{
  ham_status_t st;
  Device *device = m_env->get_device();
  ham_size_t pagesize = m_env->get_pagesize();

  while (size) {
    /* get the page-ID from this chunk */
    ham_u64_t pageid = addr - (addr % pagesize);

    if (page && page->get_self() != pageid)
      page = 0;

    /*
     * is it the current page? if not, try to fetch the page from
     * the cache - but only read the page from disk, if the
     * chunk is small
     */
    if (!page) {
      st = m_env->get_page_manager()->fetch_page(&page, db,
                          pageid, !blob_from_cache(size));
      if (st)
        return st;
      /* blob pages don't have a page header */
      if (page)
        page->set_flags(page->get_flags() | Page::NPERS_NO_HEADER);
    }

    /*
     * if we have a page pointer: use it; otherwise read directly
     * from the device
     */
    if (page) {
      ham_size_t readstart = (ham_size_t)(addr - page->get_self());
      ham_size_t readsize = (ham_size_t)(pagesize - readstart);
      if (readsize > size)
        readsize = size;
      memcpy(data, &page->get_raw_payload()[readstart], readsize);
      addr += readsize;
      data += readsize;
      size -= readsize;
    }
    else {
      ham_size_t s = (size < pagesize
                    ? size
                    : m_env->get_pagesize());
      /* limit to the next page boundary */
      if (s > pageid + pagesize - addr)
        s = (ham_size_t)(pageid + pagesize - addr);

      st = device->read(addr, data, s);
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
DiskBlobManager::allocate(Database *db, ham_record_t *record, ham_u32_t flags,
                ham_u64_t *blobid)
{
  ham_status_t st = 0;
  Page *page = 0;
  ham_u64_t addr = 0;
  ham_u8_t *chunk_data[2];
  ham_size_t alloc_size;
  ham_size_t chunk_size[2];
  Device *device = m_env->get_device();
  bool freshly_created = false;
  ByteArray zeroes(m_env->get_allocator());

  *blobid = 0;

  /*
   * PARTIAL WRITE
   *
   * if offset+partial_size equals the full record size, then we won't
   * have any gaps. In this case we just write the full record and ignore
   * the partial parameters.
   */
  if (flags & HAM_PARTIAL) {
    if (record->partial_offset == 0
        && record->partial_offset + record->partial_size == record->size)
      flags &= ~HAM_PARTIAL;
  }

  PBlobHeader blob_header;

  /* blobs are aligned! */
  alloc_size = sizeof(PBlobHeader) + record->size;
  int alignment = m_env->get_page_manager()->get_blob_alignment(db);
  if (alignment > 1) {
    alloc_size += alignment - 1;
    alloc_size -= alloc_size % alignment;
  }

  /* check if we have space in the freelist */
  bool tmp;
  st = m_env->get_page_manager()->alloc_blob(db, alloc_size, &addr, &tmp);
  if (st)
    return (st);

  if (!addr) {
    /*
     * if the blob is small AND if logging is disabled: load the page
     * through the cache
     */
    if (blob_from_cache(alloc_size)) {
      st = db->alloc_page(&page, Page::TYPE_BLOB, PAGE_IGNORE_FREELIST);
      if (st)
          return (st);
      /* blob pages don't have a page header */
      page->set_flags(page->get_flags() | Page::NPERS_NO_HEADER);
      addr = page->get_self();
      /* move the remaining space to the freelist */
      m_env->get_page_manager()->add_to_freelist(db, addr + alloc_size,
                    m_env->get_pagesize() - alloc_size);
      blob_header.set_alloc_size(alloc_size);
    }
    else {
      /* otherwise use direct IO to allocate the space */
      ham_size_t aligned = alloc_size;
      aligned += m_env->get_pagesize() - 1;
      aligned -= aligned % m_env->get_pagesize();

      st = device->alloc(aligned, &addr);
      if (st)
          return (st);

      /* if aligned!=size, and the remaining chunk is large enough:
       * move it to the freelist */
      {
        ham_size_t diff = aligned - alloc_size;
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

  /*
   * PARTIAL WRITE
   *
   * are there gaps at the beginning? If yes, then we'll fill with zeros
   */
  if ((flags & HAM_PARTIAL) && (record->partial_offset)) {
    ham_size_t gapsize = record->partial_offset;

    ham_u8_t *ptr = (ham_u8_t *)zeroes.resize(gapsize > m_env->get_pagesize()
                          ? m_env->get_pagesize()
                          : gapsize,
                       0);
    if (!ptr)
      return (HAM_OUT_OF_MEMORY);

    /* first: write the header */
    chunk_data[0] = (ham_u8_t *)&blob_header;
    chunk_size[0] = sizeof(blob_header);
    st = write_chunks(db, page, addr, true, freshly_created,
                    chunk_data, chunk_size, 1);
    if (st)
      return (st);

    addr += sizeof(blob_header);

    /* now fill the gap; if the gap is bigger than a pagesize we'll
     * split the gap into smaller chunks
     */
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

    /* fill the remaining gap */
    if (gapsize) {
      chunk_data[0] = ptr;
      chunk_size[0] = gapsize;

      st = write_chunks(db, page, addr, true, freshly_created,
                      chunk_data, chunk_size, 1);
      if (st)
        return (st);
      addr += gapsize;
    }

    /* now write the "real" data */
    chunk_data[0] = (ham_u8_t *)record->data;
    chunk_size[0] = record->partial_size;

    st = write_chunks(db, page, addr, true, freshly_created,
                    chunk_data, chunk_size, 1);
    if (st)
      return (st);
    addr += record->partial_size;
  }
  else {
    /* not writing partially: write header and data, then we're done */
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

  /* store the blobid; it will be returned to the caller */
  *blobid = blob_header.get_self();

  /*
   * PARTIAL WRITES:
   *
   * if we have gaps at the end of the blob: just append more chunks to
   * fill these gaps. Since they can be pretty large we split them into
   * smaller chunks if necessary.
   */
  if (flags & HAM_PARTIAL) {
    if (record->partial_offset + record->partial_size < record->size) {
        ham_u8_t *ptr;
        ham_size_t gapsize = record->size
                        - (record->partial_offset + record->partial_size);

        /* now fill the gap; if the gap is bigger than a pagesize we'll
         * split the gap into smaller chunks
         *
         * we split this loop in two - the outer loop will allocate the
         * memory buffer, thus saving some allocations
         */
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

        /* now write the remainder, which is less than a pagesize */
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
DiskBlobManager::read(Database *db, Transaction *txn, ham_u64_t blobid,
                ham_record_t *record, ham_u32_t flags)
{
  Page *page;

  ByteArray *arena = (txn == 0 || (txn->get_flags() & HAM_TXN_TEMPORARY))
                          ? &db->get_record_arena()
                          : &txn->get_record_arena();

  ham_assert(blobid % m_env->get_page_manager()->get_blob_alignment(db) == 0);
  

  /* first step: read the blob header */
  PBlobHeader blob_header;
  ham_status_t st = read_chunk(0, &page, blobid, db, (ham_u8_t *)&blob_header,
          sizeof(blob_header));
  if (st)
    return (st);

  ham_assert(blob_header.get_alloc_size()
          % m_env->get_page_manager()->get_blob_alignment(db) == 0);

  /* sanity check */
  if (blob_header.get_self() != blobid) {
    ham_log(("blob %lld not found", blobid));
    return (HAM_BLOB_NOT_FOUND);
  }

  ham_size_t blobsize = (ham_size_t)blob_header.get_size();

  if (flags & HAM_PARTIAL) {
    if (record->partial_offset > blobsize) {
      ham_trace(("partial offset+size is greater than the total record size"));
      return (HAM_INV_PARAMETER);
    }
    if (record->partial_offset + record->partial_size > blobsize)
      blobsize = blobsize - record->partial_offset;
    else
      blobsize = record->partial_size;
  }

  /* empty blob?  */
  if (!blobsize) {
    record->data = 0;
    record->size = 0;
    return (0);
  }

  /* second step: resize the blob buffer */
  if (!(record->flags & HAM_RECORD_USER_ALLOC)) {
    arena->resize(blobsize);
    record->data = arena->get_ptr();
  }

  /* third step: read the blob data */
  st = read_chunk(page, 0,
                  blobid + sizeof(PBlobHeader) + (flags & HAM_PARTIAL
                          ? record->partial_offset
                          : 0),
                  db, (ham_u8_t *)record->data, blobsize);
  if (st)
    return (st);

  record->size = blobsize;

  return (0);
}

ham_status_t
DiskBlobManager::get_datasize(Database *db, ham_u64_t blobid, ham_u64_t *size)
{
  *size = 0;

  ham_assert(blobid % m_env->get_page_manager()->get_blob_alignment(db) == 0);

  /* read the blob header */
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
DiskBlobManager::overwrite(Database *db, ham_u64_t old_blobid,
                ham_record_t *record, ham_u32_t flags, ham_u64_t *new_blobid)
{
  ham_status_t st;
  PBlobHeader old_blob_header, new_blob_header;
  Page *page;

  /*
   * PARTIAL WRITE
   *
   * if offset+partial_size equals the full record size, then we won't
   * have any gaps. In this case we just write the full record and ignore
   * the partial parameters.
   */
  if (flags & HAM_PARTIAL) {
    if (record->partial_offset == 0
          && record->partial_offset + record->partial_size == record->size)
      flags &= ~HAM_PARTIAL;
  }

  /* blobs are aligned! */
  ham_size_t alloc_size = sizeof(PBlobHeader) + record->size;
  int alignment = m_env->get_page_manager()->get_blob_alignment(db);
  if (alignment > 1) {
    alloc_size += alignment - 1;
    alloc_size -= alloc_size % alignment;
  }

  ham_assert(old_blobid % alignment == 0);

  /*
   * first, read the blob header; if the new blob fits into the
   * old blob, we overwrite the old blob (and add the remaining
   * space to the freelist, if there is any)
   */
  st = read_chunk(0, &page, old_blobid, db,
                  (ham_u8_t *)&old_blob_header, sizeof(old_blob_header));
  if (st)
    return (st);

  ham_assert(old_blob_header.get_alloc_size() % alignment == 0);

  /* sanity check */
  ham_assert(old_blob_header.get_self() == old_blobid);
  if (old_blob_header.get_self() != old_blobid)
    return (HAM_BLOB_NOT_FOUND);

  /*
   * now compare the sizes; does the new data fit in the old allocated
   * space?
   */
  if (alloc_size <= old_blob_header.get_alloc_size()) {
    ham_u8_t *chunk_data[2];
    ham_size_t chunk_size[2];

    /* setup the new blob header */
    new_blob_header.set_self(old_blob_header.get_self());
    new_blob_header.set_size(record->size);
    if (old_blob_header.get_alloc_size() - alloc_size > SMALLEST_CHUNK_SIZE)
      new_blob_header.set_alloc_size(alloc_size);
    else
      new_blob_header.set_alloc_size(old_blob_header.get_alloc_size());

    /*
     * PARTIAL WRITE
     *
     * if we have a gap at the beginning, then we have to write the
     * blob header and the blob data in two steps; otherwise we can
     * write both immediately
     */
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

    /* move remaining data to the freelist */
    if (old_blob_header.get_alloc_size() != new_blob_header.get_alloc_size()) {
      m_env->get_page_manager()->add_to_freelist(db,
                  new_blob_header.get_self() + new_blob_header.get_alloc_size(),
                  (ham_size_t)(old_blob_header.get_alloc_size() -
                        new_blob_header.get_alloc_size()));
    }

    /* the old rid is the new rid */
    *new_blobid = new_blob_header.get_self();
  }
  else {
    /*
     * when the new data is larger, allocate a fresh space for it
     * and discard the old; 'overwrite' has become (delete + insert) now.
     */
    st = allocate(db, record, flags, new_blobid);
    if (st)
      return (st);

    m_env->get_page_manager()->add_to_freelist(db, old_blobid,
                  (ham_size_t)old_blob_header.get_alloc_size());
  }

  return (HAM_SUCCESS);
}

ham_status_t
DiskBlobManager::free(Database *db, ham_u64_t blobid, Page *page,
                ham_u32_t flags)
{
  ham_assert(blobid % m_env->get_page_manager()->get_blob_alignment(db) == 0);

  /* fetch the blob header */
  PBlobHeader blob_header;
  ham_status_t st = read_chunk(0, 0, blobid, db,
          (ham_u8_t *)&blob_header, sizeof(blob_header));
  if (st)
    return (st);

  ham_assert(blob_header.get_alloc_size()
          % m_env->get_page_manager()->get_blob_alignment(db) == 0);

  /* sanity check */
  ham_verify(blob_header.get_self() == blobid);
  if (blob_header.get_self() != blobid)
    return (HAM_BLOB_NOT_FOUND);

  /* move the blob to the freelist */
  m_env->get_page_manager()->add_to_freelist(db, blobid,
                (ham_size_t)blob_header.get_alloc_size());
  return (0);
}

