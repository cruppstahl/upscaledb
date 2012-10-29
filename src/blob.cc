/*
 * Copyright (C) 2005-2012 Christoph Rupp (chris@crupp.de).
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
#include "freelist.h"
#include "log.h"
#include "mem.h"
#include "page.h"
#include "txn.h"
#include "btree.h"
#include "btree_key.h"

using namespace ham;


#define SMALLEST_CHUNK_SIZE  (sizeof(ham_offset_t) + sizeof(blob_t) + 1)

bool
BlobManager::blob_from_cache(ham_size_t size)
{
  if (m_env->get_log())
    return (size < (m_env->get_usable_pagesize()));
  return (size < (ham_size_t)(m_env->get_pagesize() >> 3));
}

ham_status_t
BlobManager::write_chunks(Page *page, ham_offset_t addr, bool allocated,
        bool freshly_created, ham_u8_t **chunk_data, ham_size_t *chunk_size,
        ham_size_t chunks)
{
  ham_status_t st;
  Device *device = m_env->get_device();
  ham_size_t pagesize = m_env->get_pagesize();

  ham_assert(freshly_created ? allocated : 1);

  /* for each chunk...  */
  for (ham_size_t i = 0; i < chunks; i++) {
    while (chunk_size[i]) {
      /* get the page-ID from this chunk */
      ham_offset_t pageid = addr - (addr % pagesize);

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

        st = env_fetch_page(&page, m_env, pageid,
                cacheonly ? DB_ONLY_FROM_CACHE :
                    at_blob_edge ? 0 : 0/*DB_NEW_PAGE_DOES_THRASH_CACHE*/);
        /* blob pages don't have a page header */
        if (page)
          page->set_flags(page->get_flags() | Page::NPERS_NO_HEADER);
        else if (st)
          return st;
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
BlobManager::read_chunk(Page *page, Page **fpage, ham_offset_t addr,
        Database *db, ham_u8_t *data, ham_size_t size)
{
  ham_status_t st;
  Device *device = m_env->get_device();
  ham_size_t pagesize = m_env->get_pagesize();

  while (size) {
    /* get the page-ID from this chunk */
    ham_offset_t pageid = addr - (addr % pagesize);

    if (page && page->get_self() != pageid)
      page = 0;

    /*
     * is it the current page? if not, try to fetch the page from
     * the cache - but only read the page from disk, if the
     * chunk is small
     */
    if (!page) {
      if (db)
        st = db_fetch_page(&page, db, pageid,
                blob_from_cache(size) ? 0 : DB_ONLY_FROM_CACHE);
      else
        st = env_fetch_page(&page, m_env, pageid,
                blob_from_cache(size) ? 0 : DB_ONLY_FROM_CACHE);
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

/**
 * Allocate space in storage for and write the content references by 'data'
 * (and length 'size') to storage.
 *
 * Conditions will apply whether the data is written through cache or direct
 * to device.
 *
 * The content is, of course, prefixed by a BLOB header.
 *
 * Partial writes are handled in this function.
 */
ham_status_t
BlobManager::allocate(Database *db, ham_record_t *record, ham_u32_t flags,
                ham_offset_t *blobid)
{
  ham_status_t st = 0;
  Page *page = 0;
  ham_offset_t addr = 0;
  blob_t hdr;
  ham_u8_t *chunk_data[2];
  ham_size_t alloc_size;
  ham_size_t chunk_size[2];
  Device *device = m_env->get_device();
  bool freshly_created = false;

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

  /*
   * in-memory-database: the blobid is actually a pointer to the memory
   * buffer, in which the blob (with the blob-header) is stored
   */
  if (m_env->get_flags() & HAM_IN_MEMORY) {
    blob_t *hdr;
    ham_u8_t *p = (ham_u8_t *)m_env->get_allocator()->alloc(
                                record->size + sizeof(blob_t));
    if (!p)
      return (HAM_OUT_OF_MEMORY);

    /* initialize the header */
    hdr = (blob_t *)p;
    memset(hdr, 0, sizeof(*hdr));
    blob_set_self(hdr, (ham_offset_t)PTR_TO_U64(p));
    blob_set_alloc_size(hdr, record->size + sizeof(blob_t));
    blob_set_size(hdr, record->size);

    /* do we have gaps? if yes, fill them with zeroes */
    if (flags & HAM_PARTIAL) {
      ham_u8_t *s = p + sizeof(blob_t);
      if (record->partial_offset)
        memset(s, 0, record->partial_offset);
      memcpy(s + record->partial_offset, record->data, record->partial_size);
      if (record->partial_offset + record->partial_size < record->size)
        memset(s + record->partial_offset + record->partial_size, 0,
                record->size - (record->partial_offset + record->partial_size));
    }
    else {
      memcpy(p + sizeof(blob_t), record->data, record->size);
    }

    *blobid = (ham_offset_t)PTR_TO_U64(p);
    return (0);
  }

  memset(&hdr, 0, sizeof(hdr));

  /* blobs are CHUNKSIZE-allocated */
  alloc_size = sizeof(blob_t) + record->size;
  alloc_size += DB_CHUNKSIZE - 1;
  alloc_size -= alloc_size % DB_CHUNKSIZE;

  /* check if we have space in the freelist */
  if (m_env->get_freelist()) {
    st = m_env->get_freelist()->alloc_area(&addr, db, alloc_size);
    if (st)
      return (st);
  }

  if (!addr) {
    /*
     * if the blob is small AND if logging is disabled: load the page
     * through the cache
     */
    if (blob_from_cache(alloc_size)) {
      st = db_alloc_page(&page, db, Page::TYPE_BLOB, PAGE_IGNORE_FREELIST);
      if (st)
          return (st);
      /* blob pages don't have a page header */
      page->set_flags(page->get_flags() | Page::NPERS_NO_HEADER);
      addr = page->get_self();
      /* move the remaining space to the freelist */
      if (m_env->get_freelist())
        m_env->get_freelist()->mark_free(db, addr + alloc_size,
                      m_env->get_pagesize() - alloc_size, false);
      blob_set_alloc_size(&hdr, alloc_size);
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
          if (m_env->get_freelist())
            m_env->get_freelist()->mark_free(db, addr + alloc_size,
                          diff, false);
          blob_set_alloc_size(&hdr, aligned - diff);
        }
        else {
          blob_set_alloc_size(&hdr, aligned);
        }
      }
      freshly_created = true;
    }
  }
  else {
    ham_assert(st == 0);
    blob_set_alloc_size(&hdr, alloc_size);
  }

  blob_set_size(&hdr, record->size);
  blob_set_self(&hdr, addr);

  /*
   * PARTIAL WRITE
   *
   * are there gaps at the beginning? If yes, then we'll fill with zeros
   */
  if ((flags & HAM_PARTIAL) && (record->partial_offset)) {
    ham_u8_t *ptr;
    ham_size_t gapsize = record->partial_offset;

    ptr = (ham_u8_t *)m_env->get_allocator()->calloc(
                                gapsize > m_env->get_pagesize()
                                    ? m_env->get_pagesize()
                                    : gapsize);
    if (!ptr)
        return (HAM_OUT_OF_MEMORY);

    /* first: write the header */
    chunk_data[0] = (ham_u8_t *)&hdr;
    chunk_size[0] = sizeof(hdr);
    st=write_chunks(page, addr, true, freshly_created,
                    chunk_data, chunk_size, 1);
    if (st)
      return (st);

    addr += sizeof(hdr);

    /* now fill the gap; if the gap is bigger than a pagesize we'll
     * split the gap into smaller chunks
     */
    while (gapsize>=m_env->get_pagesize()) {
      chunk_data[0] = ptr;
      chunk_size[0] = m_env->get_pagesize();
      st=write_chunks(page, addr, true, freshly_created,
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

      st = write_chunks(page, addr, true, freshly_created,
                      chunk_data, chunk_size, 1);
      if (st)
        return (st);
      addr += gapsize;
    }

    m_env->get_allocator()->free(ptr);

    /* now write the "real" data */
    chunk_data[0] = (ham_u8_t *)record->data;
    chunk_size[0] = record->partial_size;

    st = write_chunks(page, addr, true, freshly_created,
                    chunk_data, chunk_size, 1);
    if (st)
      return (st);
    addr += record->partial_size;
  }
  else {
    /* not writing partially: write header and data, then we're done */
    chunk_data[0] = (ham_u8_t *)&hdr;
    chunk_size[0] = sizeof(hdr);
    chunk_data[1] = (ham_u8_t *)record->data;
    chunk_size[1] = (flags&HAM_PARTIAL)
                        ? record->partial_size
                        : record->size;

    st = write_chunks(page, addr, true, freshly_created,
                    chunk_data, chunk_size, 2);
    if (st)
      return (st);
    addr += sizeof(hdr) + ((flags&HAM_PARTIAL)
                              ? record->partial_size
                              : record->size);
  }

  /* store the blobid; it will be returned to the caller */
  *blobid = blob_get_self(&hdr);

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
            ham_u8_t *ptr = (ham_u8_t *)m_env->get_allocator()->calloc(
                                        m_env->get_pagesize());
            if (!ptr)
              return (HAM_OUT_OF_MEMORY);
            while (gapsize > m_env->get_pagesize()) {
              chunk_data[0] = ptr;
              chunk_size[0] = m_env->get_pagesize();
              st = write_chunks(page, addr, true,
                            freshly_created, chunk_data, chunk_size, 1);
              if (st)
                break;
              gapsize -= m_env->get_pagesize();
              addr += m_env->get_pagesize();
            }
            m_env->get_allocator()->free(ptr);
            if (st)
              return (st);
        }

        /* now write the remainder, which is less than a pagesize */
        ham_assert(gapsize < m_env->get_pagesize());

        chunk_size[0] = gapsize;
        ptr = chunk_data[0] = (ham_u8_t *)m_env->get_allocator()->calloc(gapsize);
        if (!ptr)
          return (HAM_OUT_OF_MEMORY);

        st = write_chunks(page, addr, true, freshly_created,
                    chunk_data, chunk_size, 1);
        m_env->get_allocator()->free(ptr);
        if (st)
          return (st);
    }
  }

  return (0);
}

ham_status_t
BlobManager::read(Database *db, Transaction *txn, ham_offset_t blobid,
                ham_record_t *record, ham_u32_t flags)
{
  ham_status_t st;
  Page *page;
  blob_t hdr;
  ham_size_t blobsize = 0;

  ByteArray *arena = (txn == 0 || (txn->get_flags() & HAM_TXN_TEMPORARY))
                          ? &db->get_record_arena()
                          : &txn->get_record_arena();

  /*
   * in-memory-database: the blobid is actually a pointer to the memory
   * buffer, in which the blob is stored
   */
  if (db->get_env()->get_flags() & HAM_IN_MEMORY) {
    blob_t *hdr = (blob_t *)U64_TO_PTR(blobid);
    ham_u8_t *data = (ham_u8_t *)(U64_TO_PTR(blobid)) + sizeof(blob_t);

    /* when the database is closing, the header is already deleted */
    if (!hdr) {
      record->size = 0;
      return (0);
    }

    blobsize = (ham_size_t)blob_get_size(hdr);

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
      record->data=0;
      record->size=0;
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

  ham_assert(blobid % DB_CHUNKSIZE==0);

  /* first step: read the blob header */
  st=read_chunk(0, &page, blobid, db,
                    (ham_u8_t *)&hdr, sizeof(hdr));
  if (st)
    return (st);

  ham_assert(blob_get_alloc_size(&hdr) % DB_CHUNKSIZE == 0);

  /* sanity check */
  if (blob_get_self(&hdr) != blobid) {
    ham_log(("blob %lld not found", blobid));
    return (HAM_BLOB_NOT_FOUND);
  }

  blobsize = (ham_size_t)blob_get_size(&hdr);

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
  st=read_chunk(page, 0,
                  blobid + sizeof(blob_t) + (flags & HAM_PARTIAL
                          ? record->partial_offset
                          : 0),
                  db, (ham_u8_t *)record->data, blobsize);
  if (st)
    return (st);

  record->size = blobsize;

  return (0);
}

ham_status_t
BlobManager::get_datasize(Database *db, ham_offset_t blobid, ham_offset_t *size)
{
  ham_status_t st;
  Page *page;
  blob_t hdr;

  /*
   * in-memory-database: the blobid is actually a pointer to the memory
   * buffer, in which the blob is stored
   */
  if (m_env->get_flags() & HAM_IN_MEMORY) {
    blob_t *hdr = (blob_t *)U64_TO_PTR(blobid);
    *size = (ham_size_t)blob_get_size(hdr);
    return (0);
  }

  ham_assert(blobid % DB_CHUNKSIZE == 0);

  /* read the blob header */
  st=read_chunk(0, &page, blobid, db, (ham_u8_t *)&hdr, sizeof(hdr));
  if (st)
    return (st);

  if (blob_get_self(&hdr) != blobid)
    return (HAM_BLOB_NOT_FOUND);

  *size = blob_get_size(&hdr);
  return (0);
}

ham_status_t
BlobManager::overwrite(Database *db, ham_offset_t old_blobid,
                ham_record_t *record, ham_u32_t flags, ham_offset_t *new_blobid)
{
  ham_status_t st;
  ham_size_t alloc_size;
  blob_t old_hdr, new_hdr;
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

  /*
   * inmemory-databases: free the old blob,
   * allocate a new blob (but if both sizes are equal, just overwrite
   * the data)
   */
  if (m_env->get_flags() & HAM_IN_MEMORY) {
    blob_t *nhdr, *phdr = (blob_t *)U64_TO_PTR(old_blobid);

    if (blob_get_size(phdr) == record->size) {
      ham_u8_t *p = (ham_u8_t *)phdr;
      if (flags & HAM_PARTIAL) {
        memmove(p + sizeof(blob_t) + record->partial_offset,
                record->data, record->partial_size);
      }
      else {
        memmove(p + sizeof(blob_t), record->data, record->size);
      }
      *new_blobid = (ham_offset_t)PTR_TO_U64(phdr);
    }
    else {
      st = m_env->get_blob_manager()->allocate(db, record, flags, new_blobid);
      if (st)
        return (st);
      nhdr = (blob_t *)U64_TO_PTR(*new_blobid);
      blob_set_flags(nhdr, blob_get_flags(phdr));

      m_env->get_allocator()->free(phdr);
    }

    return (HAM_SUCCESS);
  }

  ham_assert(old_blobid % DB_CHUNKSIZE == 0);

  /* blobs are CHUNKSIZE-allocated */
  alloc_size = sizeof(blob_t) + record->size;
  alloc_size += DB_CHUNKSIZE - 1;
  alloc_size -= alloc_size % DB_CHUNKSIZE;

  /*
   * first, read the blob header; if the new blob fits into the
   * old blob, we overwrite the old blob (and add the remaining
   * space to the freelist, if there is any)
   */
  st = read_chunk(0, &page, old_blobid, db,
                  (ham_u8_t *)&old_hdr, sizeof(old_hdr));
  if (st)
    return (st);

  ham_assert(blob_get_alloc_size(&old_hdr) % DB_CHUNKSIZE == 0);

  /* sanity check */
  ham_assert(blob_get_self(&old_hdr) == old_blobid);
  if (blob_get_self(&old_hdr) != old_blobid)
    return (HAM_BLOB_NOT_FOUND);

  /*
   * now compare the sizes; does the new data fit in the old allocated
   * space?
   */
  if (alloc_size <= blob_get_alloc_size(&old_hdr)) {
    ham_u8_t *chunk_data[2];
    ham_size_t chunk_size[2];

    /* setup the new blob header */
    blob_set_self(&new_hdr, blob_get_self(&old_hdr));
    blob_set_size(&new_hdr, record->size);
    blob_set_flags(&new_hdr, blob_get_flags(&old_hdr));
    if (blob_get_alloc_size(&old_hdr) - alloc_size > SMALLEST_CHUNK_SIZE)
      blob_set_alloc_size(&new_hdr, alloc_size);
    else
      blob_set_alloc_size(&new_hdr, blob_get_alloc_size(&old_hdr));

    /*
     * PARTIAL WRITE
     *
     * if we have a gap at the beginning, then we have to write the
     * blob header and the blob data in two steps; otherwise we can
     * write both immediately
     */
    if ((flags&HAM_PARTIAL) && (record->partial_offset)) {
      chunk_data[0] = (ham_u8_t *)&new_hdr;
      chunk_size[0] = sizeof(new_hdr);
      st = write_chunks(page, blob_get_self(&new_hdr), false,
                        false, chunk_data, chunk_size, 1);
      if (st)
        return (st);

      chunk_data[0] = (ham_u8_t *)record->data;
      chunk_size[0] = record->partial_size;
      st = write_chunks(page, blob_get_self(&new_hdr) + sizeof(new_hdr)
                            + record->partial_offset, false, false,
                            chunk_data, chunk_size, 1);
      if (st)
        return (st);
    }
    else {
      chunk_data[0] = (ham_u8_t *)&new_hdr;
      chunk_size[0] = sizeof(new_hdr);
      chunk_data[1] = (ham_u8_t *)record->data;
      chunk_size[1] = (flags & HAM_PARTIAL)
                          ? record->partial_size
                          : record->size;

      st = write_chunks(page, blob_get_self(&new_hdr), false,
                            false, chunk_data, chunk_size, 2);
      if (st)
        return (st);
    }

    /* move remaining data to the freelist */
    if (blob_get_alloc_size(&old_hdr) != blob_get_alloc_size(&new_hdr)) {
      if (m_env->get_freelist())
        m_env->get_freelist()->mark_free(db,
                    blob_get_self(&new_hdr) + blob_get_alloc_size(&new_hdr),
                    (ham_size_t)(blob_get_alloc_size(&old_hdr) -
                        blob_get_alloc_size(&new_hdr)), false);
    }

    /* the old rid is the new rid */
    *new_blobid = blob_get_self(&new_hdr);

    return (HAM_SUCCESS);
  }
  else {
    /*
     * when the new data is larger, allocate a fresh space for it
     * and discard the old; 'overwrite' has become (delete + insert) now.
     */
    st = m_env->get_blob_manager()->allocate(db, record, flags, new_blobid);
    if (st)
      return (st);

    if (m_env->get_freelist())
      m_env->get_freelist()->mark_free(db, old_blobid,
                    (ham_size_t)blob_get_alloc_size(&old_hdr), false);
  }

  return (HAM_SUCCESS);
}

ham_status_t
BlobManager::free(Database *db, ham_offset_t blobid, ham_u32_t flags)
{
  ham_status_t st;
  blob_t hdr;

  /*
   * in-memory-database: the blobid is actually a pointer to the memory
   * buffer, in which the blob is stored
   */
  if (m_env->get_flags() & HAM_IN_MEMORY) {
    m_env->get_allocator()->free((void *)U64_TO_PTR(blobid));
    return (0);
  }

  ham_assert(blobid % DB_CHUNKSIZE == 0);

  /* fetch the blob header */
  st = read_chunk(0, 0, blobid, db, (ham_u8_t *)&hdr, sizeof(hdr));
  if (st)
    return (st);

  ham_assert(blob_get_alloc_size(&hdr) % DB_CHUNKSIZE == 0);

  /* sanity check */
  ham_verify(blob_get_self(&hdr) == blobid);
  if (blob_get_self(&hdr) != blobid)
    return (HAM_BLOB_NOT_FOUND);

  /* move the blob to the freelist */
  if (m_env->get_freelist())
    st = m_env->get_freelist()->mark_free(db, blobid,
                  (ham_size_t)blob_get_alloc_size(&hdr), false);

  return st;
}

