/*
 * Copyright (C) 2005-2010 Christoph Rupp (chris@crupp.de).
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
#include "keys.h"
#include "log.h"
#include "mem.h"
#include "page.h"
#include "txn.h"


#define SMALLEST_CHUNK_SIZE  (sizeof(ham_offset_t)+sizeof(blob_t)+1)

/* 
 * if the blob is small enough (or if logging is enabled) then go through
 * the cache. otherwise use direct I/O
 */
static ham_bool_t
__blob_from_cache(ham_env_t *env, ham_size_t size)
{
    if (env_get_log(env))
        return (size<(env_get_usable_pagesize(env)));  
    return (size<(ham_size_t)(env_get_pagesize(env)>>3));
}

/**
write a series of data chunks to storage at file offset 'addr'.

The chunks are assumed to be stored in sequential order, adjacent
to each other, i.e. as one long data strip.

Writing is performed on a per-page basis, where special conditions
will decide whether or not the write operation is performed
through the page cache or directly to device; such is determined 
on a per-page basis.
*/
static ham_status_t
__write_chunks(ham_env_t *env, ham_page_t *page, ham_offset_t addr, 
        ham_bool_t allocated, ham_bool_t freshly_created, 
        ham_u8_t **chunk_data, ham_size_t *chunk_size, 
        ham_size_t chunks)
{
    ham_size_t i;
    ham_status_t st;
    ham_offset_t pageid;
    ham_device_t *device=env_get_device(env);
	ham_size_t pagesize = env_get_pagesize(env);

    ham_assert(freshly_created ? allocated : 1, (0));

    /*
     * for each chunk...
     */
    for (i=0; i<chunks; i++) {
        while (chunk_size[i]) {
            /*
             * get the page-ID from this chunk
             */
            pageid = addr - (addr % pagesize);

            /*
             * is this the current page?
             */
            if (page && page_get_self(page)!=pageid)
                page=0;

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
                 *
                 * When transaction logging is turned on, it's the same story, 
                 * really. We _could_ keep all those pages in cache now,
                 * but this would be thrashing the cache with blob data that's 
                 * accessed once only and for transaction abort (or commit)
                 * the amount of effort does not change.
                 *
                 * THOUGHT:
                 *
                 * Do we actually care what was in that page, which is going 
                 * to be overwritten in its entirety, BEFORE we do this, i.e. 
                 * before the transaction? 
                 *
                 * Answer: NO (and YES in special circumstances).
                 *
                 * Elaboration: As this would have been free space before, the 
                 * actual content does not matter, so it's not required to add
                 * the FULL pages written by the blob write action here to the 
                 * transaction log: even on transaction abort, that lingering 
                 * data is marked as 'bogus'/free as it was before anyhow.
                 *
                 * And then, assuming a longer running transaction, where this 
                 * page was freed during a previous action WITHIN
                 * the transaction, well, than the transaction log should 
                 * already carry this page's previous content as instructed 
                 * by the erase operation. HOWEVER, the erase operation would 
                 * not have a particular NEED to edit this page, as an erase op 
                 * is complete by just marking this space as free in the 
                 * freelist, resulting in the freelist pages (and the btree 
                 * pages) being the only ones being edited and ending up in 
                 * the transaction log then.
                 *
                 * Which means we'll have to log the previous content of these 
                 * pages to the transaction log anyhow. UNLESS, that is, when
                 * WE allocated these pages in the first place: then there 
                 * cannot be any 'pre-transaction' state of these pages 
                 * except that of 'not existing', i.e. 'free'. In which case, 
                 * their actual content doesn't matter! (freshly_created)
                 *
                 * And what if we have recovery logging turned on, but it's 
                 * not about an active transaction here?
                 * In that case, the recovery log would only log the OLD page 
                 * content, which we've concluded is insignificant, ever. Of 
                 * course, that's assuming (again!) that we're writing to 
                 * freshly created pages, which no-one has seen before. 
                 *
                 * Just as long as we can prevent this section from thrashing 
                 * the page cache, thank you very much...
                 */
                ham_bool_t at_blob_edge = (__blob_from_cache(env, chunk_size[i])
                        || (addr % pagesize) != 0 
                        || chunk_size[i] < pagesize);
                ham_bool_t cacheonly = (!at_blob_edge 
                                    && (!env_get_log(env)
                                        || freshly_created));
				//ham_assert(db_get_txn(db) ? !!env_get_log(env) : 1, (0));

                st=env_fetch_page(&page, env, pageid, 
                        cacheonly ? DB_ONLY_FROM_CACHE : 
                        at_blob_edge ? 0 : DB_NEW_PAGE_DOES_THRASH_CACHE);
				ham_assert(st ? !page : 1, (0));
                /* blob pages don't have a page header */
                if (page)
                {
                    page_set_npers_flags(page, 
                        page_get_npers_flags(page)|PAGE_NPERS_NO_HEADER);
                    /* if this page was recently allocated by the parent
                     * function: set a flag */
                    if (cacheonly 
                            && allocated 
                            && addr==page_get_self(page) 
                            && env_get_txn(env))
                        page_set_alloc_txn_id(page, txn_get_id(env_get_txn(env)));
                }
                else if (st) {
                    return st;
                }
            }

            /*
             * if we have a page pointer: use it; otherwise write directly
             * to the device
             */
            if (page) {
                ham_size_t writestart=
                        (ham_size_t)(addr-page_get_self(page));
                ham_size_t writesize =
                        (ham_size_t)(pagesize - writestart);
                if (writesize>chunk_size[i])
                    writesize=chunk_size[i];
                if ((st=ham_log_add_page_before(page)))
                    return (st);
                memcpy(&page_get_raw_payload(page)[writestart], chunk_data[i],
                            writesize);
                page_set_dirty(page, env);
                addr+=writesize;
                chunk_data[i]+=writesize;
                chunk_size[i]-=writesize;
            }
            else {
                ham_size_t s = chunk_size[i];
                /* limit to the next page boundary */
                if (s > pageid+pagesize-addr)
                    s = (ham_size_t)(pageid+pagesize-addr);

                ham_assert(env_get_log(env) ? freshly_created : 1, (0));

                st=device->write(device, addr, chunk_data[i], s);
                if (st)
                    return st;
                addr+=s;
                chunk_data[i]+=s;
                chunk_size[i]-=s;
            }
        }
    }

    return (0);
}

static ham_status_t
__read_chunk(ham_env_t *env, ham_page_t *page, ham_page_t **fpage, 
        ham_offset_t addr, ham_u8_t *data, ham_size_t size)
{
    ham_status_t st;
    ham_device_t *device=env_get_device(env);

    while (size) {
        /*
         * get the page-ID from this chunk
         */
        ham_offset_t pageid;
		pageid = addr - (addr % env_get_pagesize(env));

        if (page) {
            if (page_get_self(page)!=pageid)
                page=0;
        }

        /*
         * is it the current page? if not, try to fetch the page from
         * the cache - but only read the page from disk, if the 
         * chunk is small
         */
        if (!page) {
            st=env_fetch_page(&page, env, pageid, 
                    __blob_from_cache(env, size) ? 0 : DB_ONLY_FROM_CACHE);
			ham_assert(st ? !page : 1, (0));
            /* blob pages don't have a page header */
            if (page)
                page_set_npers_flags(page, 
                    page_get_npers_flags(page)|PAGE_NPERS_NO_HEADER);
			else if (st)
				return st;
        }

        /*
         * if we have a page pointer: use it; otherwise read directly
         * from the device
         */
        if (page) {
            ham_size_t readstart=
                    (ham_size_t)(addr-page_get_self(page));
            ham_size_t readsize =
                    (ham_size_t)(env_get_pagesize(env)-readstart);
            if (readsize>size)
                readsize=size;
            memcpy(data, &page_get_raw_payload(page)[readstart], readsize);
            addr+=readsize;
            data+=readsize;
            size-=readsize;
        }
        else {
            ham_size_t s=(size<env_get_pagesize(env) 
                    ? size : env_get_pagesize(env));
            /* limit to the next page boundary */
            if (s>pageid+env_get_pagesize(env)-addr)
                s=(ham_size_t)(pageid+env_get_pagesize(env)-addr);

            st=device->read(device, addr, data, s);
            if (st) 
                return st;
            addr+=s;
            data+=s;
            size-=s;
        }
    }

    if (fpage)
        *fpage=page;

    return (0);
}

static ham_status_t
__get_duplicate_table(dupe_table_t **table_ref, ham_page_t **page, ham_env_t *env, ham_offset_t table_id)
{
    ham_status_t st;
    blob_t hdr;
    ham_page_t *hdrpage=0;
    dupe_table_t *table;

	*page = 0;

    if (env_get_rt_flags(env)&HAM_IN_MEMORY_DB) {
        ham_u8_t *p=(ham_u8_t *)table_id;
        *table_ref = (dupe_table_t *)(p+sizeof(hdr));
		return HAM_SUCCESS;
    }

	*table_ref = 0;

    /*
     * load the blob header
     */
    st=__read_chunk(env, 0, &hdrpage, table_id, (ham_u8_t *)&hdr, sizeof(hdr));
    if (st) {
        return st;
    }

    /*
     * if the whole table is in a page (and not split between several
     * pages), just return a pointer directly in the page
     */
    if (page_get_self(hdrpage)+env_get_usable_pagesize(env) >=
            table_id+blob_get_size(&hdr)) 
    {
        ham_u8_t *p=page_get_raw_payload(hdrpage);
        /* yes, table is in the page */
        *page=hdrpage;
        *table_ref = (dupe_table_t *)
                &p[table_id-page_get_self(hdrpage)+sizeof(hdr)];
		return HAM_SUCCESS;
    }

    /*
     * otherwise allocate memory for the table
     */
    table=allocator_alloc(env_get_allocator(env), (ham_size_t)blob_get_size(&hdr));
    if (!table) {
        return HAM_OUT_OF_MEMORY;
    }

    /*
     * then read the rest of the blob
     */
    st=__read_chunk(env, hdrpage, 0, table_id+sizeof(hdr), 
            (ham_u8_t *)table, (ham_size_t)blob_get_size(&hdr));
    if (st) {
        return st;
    }

    *table_ref = table;
	return HAM_SUCCESS;
}

/**
Allocate space in storage for and write the content references by 'data'
(and length 'size') to storage.

Conditions will apply whether the data is written through cache or direct
to device.

The content is, of course, prefixed by a BLOB header.
*/
ham_status_t
blob_allocate(ham_env_t *env, ham_db_t *db, ham_u8_t *data, ham_size_t size, 
        ham_u32_t flags, ham_offset_t *blobid)
{
    ham_status_t st;
    ham_page_t *page=0;
    ham_offset_t addr;
    blob_t hdr;
    ham_u8_t *chunk_data[2];
    ham_size_t alloc_size;
    ham_size_t chunk_size[2];
    ham_device_t *device=env_get_device(env);
    ham_bool_t freshly_created = HAM_FALSE;
   
    *blobid=0;

    /*
     * in-memory-database: the blobid is actually a pointer to the memory
     * buffer, in which the blob (with the blob-header) is stored
     */
    if (env_get_rt_flags(env)&HAM_IN_MEMORY_DB) 
    {
        blob_t *hdr;
        ham_u8_t *p=(ham_u8_t *)allocator_alloc(env_get_allocator(env), size+sizeof(blob_t));
        if (!p) {
            return HAM_OUT_OF_MEMORY;
        }
        memcpy(p+sizeof(blob_t), data, size);

        /* initialize the header */
        hdr=(blob_t *)p;
        memset(hdr, 0, sizeof(*hdr));
        blob_set_self(hdr, (ham_offset_t)p);
        blob_set_alloc_size(hdr, size+sizeof(blob_t));
        blob_set_size(hdr, size);

        *blobid=(ham_offset_t)p;
        return (0);
    }

    memset(&hdr, 0, sizeof(hdr));

    /*
     * blobs are CHUNKSIZE-allocated 
     */
    alloc_size=sizeof(blob_t)+size;
    alloc_size += DB_CHUNKSIZE - 1;
    alloc_size -= alloc_size % DB_CHUNKSIZE;

    /* 
     * check if we have space in the freelist 
     */
    st = freel_alloc_area(&addr, env, db, alloc_size);
    if (!addr) 
    {
        if (st)
            return st;

        /*
         * if the blob is small AND if logging is disabled: load the page 
         * through the cache
         */
        if (__blob_from_cache(env, alloc_size)) {
            st = db_alloc_page(&page, db, PAGE_TYPE_BLOB, 
                        PAGE_IGNORE_FREELIST);
			ham_assert(st ? page == NULL : 1, (0));
			ham_assert(!st ? page  != NULL : 1, (0));
            if (st)
                return st;
            /* blob pages don't have a page header */
            page_set_npers_flags(page, 
                    page_get_npers_flags(page)|PAGE_NPERS_NO_HEADER);
            addr=page_get_self(page);
            /* move the remaining space to the freelist */
            (void)freel_mark_free(env, db, addr+alloc_size,
                    env_get_pagesize(env)-alloc_size, HAM_FALSE);
            blob_set_alloc_size(&hdr, alloc_size);
        }
        else {
            /*
             * otherwise use direct IO to allocate the space
             */
            ham_size_t aligned=alloc_size;
            aligned += env_get_pagesize(env) - 1;
            aligned -= aligned % env_get_pagesize(env);

            st=device->alloc(device, aligned, &addr);
            if (st) 
                return (st);

            /* if aligned!=size, and the remaining chunk is large enough:
             * move it to the freelist */
            {
                ham_size_t diff=aligned-alloc_size;
                if (diff > SMALLEST_CHUNK_SIZE) {
                    (void)freel_mark_free(env, db, addr+alloc_size, 
                            diff, HAM_FALSE);
                    blob_set_alloc_size(&hdr, aligned-diff);
                }
                else {
                    blob_set_alloc_size(&hdr, aligned);
                }
            }
            freshly_created = HAM_TRUE;
        }

        ham_assert(HAM_SUCCESS == freel_check_area_is_allocated(env, db,
                    addr, alloc_size), (0));
    }
    else {
		ham_assert(!st, (0));
        blob_set_alloc_size(&hdr, alloc_size);
    }

    blob_set_size(&hdr, size);
    blob_set_self(&hdr, addr);

    /* 
     * write header and data 
     */
    chunk_data[0]=(ham_u8_t *)&hdr;
    chunk_size[0]=sizeof(hdr);
    chunk_data[1]=(ham_u8_t *)data;
    chunk_size[1]=size;

    st=__write_chunks(env, page, addr, HAM_TRUE, freshly_created, chunk_data, chunk_size, 2);
    if (st)
        return (st);

    *blobid=addr;

    return (0);
}

ham_status_t
blob_read(ham_db_t *db, ham_offset_t blobid, 
        ham_record_t *record, ham_u32_t flags)
{
    ham_status_t st;
    ham_page_t *page;
    blob_t hdr;
    ham_size_t blobsize;

    blobsize = 0;

    /*
     * in-memory-database: the blobid is actually a pointer to the memory
     * buffer, in which the blob is stored
     */
    if (env_get_rt_flags(db_get_env(db))&HAM_IN_MEMORY_DB) 
	{
        blob_t *hdr=(blob_t *)blobid;
        ham_u8_t *data=((ham_u8_t *)blobid)+sizeof(blob_t);

        /* when the database is closing, the header is already deleted */
        if (!hdr) {
            record->size = 0;
            return (0);
        }

        blobsize = (ham_size_t)blob_get_size(hdr);
        if (!blobsize) {
            /* empty blob? */
            record->data = 0;
            record->size = 0;
        }
        else {
            if ((flags&HAM_DIRECT_ACCESS) 
                    && !(record->flags&HAM_RECORD_USER_ALLOC)) {
                record->size = blobsize;
                record->data=data;
            }
            else {
                /* resize buffer, if necessary */
                if (!(record->flags & HAM_RECORD_USER_ALLOC)) {
                    st=db_resize_allocdata(db, blobsize);
                    if (st)
                        return (st);
                    record->data = db_get_record_allocdata(db);
                }
                /* and copy the data */
                memcpy(record->data, data, blobsize);
                record->size = blobsize;
            }
        }

        return (0);
    }

    ham_assert(blobid%DB_CHUNKSIZE==0, ("blobid is %llu", blobid));

    /*
     * first step: read the blob header 
     */
    st=__read_chunk(db_get_env(db), 0, &page, blobid, (ham_u8_t *)&hdr, sizeof(hdr));
    if (st)
        return (st);

    ham_assert(blob_get_alloc_size(&hdr)%DB_CHUNKSIZE==0, (0));

    /*
     * sanity check
     */
    if (blob_get_self(&hdr)!=blobid)
        return (HAM_BLOB_NOT_FOUND);

    /* 
     * empty blob? 
     */
    blobsize = (ham_size_t)blob_get_size(&hdr);
    if (!blobsize) {
        record->data = 0;
        record->size = 0;
        return (0);
    }

    /*
     * second step: resize the blob buffer
     */
    if (!(record->flags & HAM_RECORD_USER_ALLOC)) {
        st=db_resize_allocdata(db, blobsize);
        if (st)
            return (st);
        record->data = db_get_record_allocdata(db);
    }

    /*
     * third step: read the blob data
     */
    st=__read_chunk(db_get_env(db), page, 0, blobid+sizeof(blob_t), record->data, 
                    blobsize);
    if (st)
        return (st);

    record->size = blobsize;

    return (0);
}

ham_status_t
blob_overwrite(ham_env_t *env, ham_db_t *db, ham_offset_t old_blobid, 
        ham_u8_t *data, ham_size_t size, ham_u32_t flags, 
        ham_offset_t *new_blobid)
{
    ham_status_t st;
    ham_size_t alloc_size;
    blob_t old_hdr;
    blob_t new_hdr;
    ham_page_t *page;

    /*
     * inmemory-databases: free the old blob, 
     * allocate a new blob (but if both sizes are equal, just overwrite
     * the data)
     */
    if (env_get_rt_flags(env)&HAM_IN_MEMORY_DB) 
    {
        blob_t *nhdr, *phdr=(blob_t *)old_blobid;

        if (blob_get_size(phdr)==size) {
            ham_u8_t *p=(ham_u8_t *)phdr;
            memmove(p+sizeof(blob_t), data, size);
            *new_blobid=(ham_offset_t)phdr;
        }
        else {
            st=blob_allocate(env, db, data, size, flags, new_blobid);
            if (st)
                return (st);
            nhdr=(blob_t *)*new_blobid;
            blob_set_flags(nhdr, blob_get_flags(phdr));

            allocator_free(env_get_allocator(env), phdr);
        }

        return HAM_SUCCESS;
    }

    ham_assert(old_blobid%DB_CHUNKSIZE==0, (0));

    /*
     * blobs are CHUNKSIZE-allocated 
     */
    alloc_size=sizeof(blob_t)+size;
    alloc_size += DB_CHUNKSIZE - 1;
    alloc_size -= alloc_size % DB_CHUNKSIZE;

    /*
     * first, read the blob header; if the new blob fits into the 
     * old blob, we overwrite the old blob (and add the remaining
     * space to the freelist, if there is any)
     */
    st=__read_chunk(env, 0, &page, old_blobid, (ham_u8_t *)&old_hdr, 
            sizeof(old_hdr));
    if (st)
        return (st);

    ham_assert(blob_get_alloc_size(&old_hdr)%DB_CHUNKSIZE==0, (0));

    /*
     * sanity check
     */
    ham_assert(blob_get_self(&old_hdr)==old_blobid, 
            ("invalid blobid %llu != %llu", blob_get_self(&old_hdr), 
            old_blobid));
    if (blob_get_self(&old_hdr)!=old_blobid)
        return (HAM_BLOB_NOT_FOUND);

    /*
     * now compare the sizes; does the new data fit in the old allocated
     * space?
     */
    if (alloc_size<=blob_get_alloc_size(&old_hdr)) 
    {
        ham_u8_t *chunk_data[2];
        ham_size_t chunk_size[2];

        chunk_data[0]=(ham_u8_t *)&new_hdr;
        chunk_size[0]=sizeof(new_hdr);
        chunk_data[1]=data;
        chunk_size[1]=size;

        /* 
         * setup the new blob header
         */
        blob_set_self(&new_hdr, blob_get_self(&old_hdr));
        blob_set_size(&new_hdr, size);
        blob_set_flags(&new_hdr, blob_get_flags(&old_hdr));
        if (blob_get_alloc_size(&old_hdr)-alloc_size>SMALLEST_CHUNK_SIZE)
            blob_set_alloc_size(&new_hdr, alloc_size);
        else
            blob_set_alloc_size(&new_hdr, blob_get_alloc_size(&old_hdr));

        st=__write_chunks(env, page, blob_get_self(&new_hdr), HAM_FALSE,
                HAM_FALSE, chunk_data, chunk_size, 2);
        if (st)
            return (st);

        /*
         * move remaining data to the freelist
         */
        if (blob_get_alloc_size(&old_hdr)!=blob_get_alloc_size(&new_hdr)) {
            (void)freel_mark_free(env, db,
                  blob_get_self(&new_hdr)+blob_get_alloc_size(&new_hdr), 
                  (ham_size_t)(blob_get_alloc_size(&old_hdr)-
                  blob_get_alloc_size(&new_hdr)), HAM_FALSE);
        }

        /*
         * the old rid is the new rid
         */
        *new_blobid=blob_get_self(&new_hdr);

        return HAM_SUCCESS;
    }
    else {
        /* 
        when the new data is larger, allocate a fresh space for it and discard the old;
        'overwrite' has become (delete + insert) now.
        */
        st=blob_allocate(env, db, data, size, flags, new_blobid);
        if (st)
            return (st);

        (void)freel_mark_free(env, db, old_blobid, 
                (ham_size_t)blob_get_alloc_size(&old_hdr), HAM_FALSE);
    }

    return HAM_SUCCESS;
}

ham_status_t
blob_free(ham_env_t *env, ham_db_t *db, ham_offset_t blobid, ham_u32_t flags)
{
    ham_status_t st;
    blob_t hdr;

    /*
     * in-memory-database: the blobid is actually a pointer to the memory
     * buffer, in which the blob is stored
     */
    if (env_get_rt_flags(env)&HAM_IN_MEMORY_DB) {
        allocator_free(env_get_allocator(env), (blob_t *)blobid);
        return (0);
    }

    ham_assert(blobid%DB_CHUNKSIZE==0, (0));

    /*
     * fetch the blob header 
     */
    st=__read_chunk(env, 0, 0, blobid, (ham_u8_t *)&hdr, sizeof(hdr));
    if (st)
        return (st);

    ham_assert(blob_get_alloc_size(&hdr)%DB_CHUNKSIZE==0, (0));

    /*
     * sanity check
     */
    ham_assert(blob_get_self(&hdr)==blobid, 
            ("invalid blobid %llu != %llu", blob_get_self(&hdr), blobid));
    if (blob_get_self(&hdr)!=blobid)
        return (HAM_BLOB_NOT_FOUND);

    /*
     * move the blob to the freelist
     */
    st = freel_mark_free(env, db, blobid, 
            (ham_size_t)blob_get_alloc_size(&hdr), HAM_FALSE);
	ham_assert(!st, ("unexpected error, at least not covered in the old code"));

    return st;
}

static ham_size_t
__get_sorted_position(ham_db_t *db, dupe_table_t *table, ham_record_t *record,
                ham_u32_t flags)
{
    ham_duplicate_compare_func_t foo = db_get_duplicate_compare_func(db);
    ham_size_t l, r, m;
    int cmp;
    dupe_entry_t *e;
    ham_record_t item_record;
    ham_u16_t dam;
    ham_status_t st=0;

    /*
     * Use a slightly adapted form of binary search: as we already have our 
     * initial position (as was stored in the cursor), we take that as our
     * first 'median' value and go from there.
     */
    l = 0;
    r = dupe_table_get_count(table) - 1; /* get_count() is 1 too many! */

    /*
     * Maybe Wrong Idea: sequential access/insert doesn't mean the RECORD 
     * values are sequential too! They MAY be, but don't have to!
     *
     * For now, we assume they are also sequential when you're storing records
     * in duplicate-key tables (probably a secondary index table for another
     * table, this one).
     */
    dam = db_get_data_access_mode(db);
    if (dam & HAM_DAM_SEQUENTIAL_INSERT) {
        /* assume the insertion point sits at the end of the dupe table */
        m = r;
    }
    else if (dam & HAM_DAM_FAST_INSERT) {
        /*
         * can't really say how to speed these buggers up, apart from maybe
         * assuming the insertion point is at the previously known position.
         */
        m = dupe_table_get_count(table);
        if (m > r)
            m = r;
        /* 
         * as this is only a split point, we check which side is shortest 
         * and adjust the split point to the other side, so the reduction 
         * can be quickened when all assumptions match.
         */
        if (r - m > m - l) {
            m++;
        }
        else if (m > 0) {
            m--;
        }
    }
    else {
        m = (l + r) / 2;
    }
    ham_assert(m <= r, (0));
    //ham_assert(r >= 1, (0));
        
    while (l <= r) {
        ham_assert(m<dupe_table_get_count(table), (""));

        e = dupe_table_get_entry(table, m);

        memset(&item_record, 0, sizeof(item_record));
        item_record._rid=dupe_entry_get_rid(e);
        item_record._intflags = dupe_entry_get_flags(e)&(KEY_BLOB_SIZE_SMALL
                                                         |KEY_BLOB_SIZE_TINY
                                                         |KEY_BLOB_SIZE_EMPTY);
        st=util_read_record(db, &item_record, flags);
        if (st)
            return (st);

        cmp = foo(db, record->data, record->size, 
                        item_record.data, item_record.size);
        /* item is lower than the left-most item of our range */
        if (m == l) {
            if (cmp < 0)
                break;
        }
        if (l == r) {
            if (cmp >= 0) {
                /* write GEQ record value in NEXT slot */
                m++;
            }
            else /* if (cmp < 0) */ {
                ham_assert(m == r, (0));
            }
            break;
        }
        else if (cmp == 0) {
            /* write equal record value in NEXT slot */
            m++;
            break;
        }
        else if (cmp < 0) {
            if (m == 0) /* new item will be smallest item in the list */
                break;
            r = m - 1;
        }
        else {
            /* write GE record value in NEXT slot, when we have nothing 
             * left to search */
            m++;
            l = m;
        }
        m = (l + r) / 2;
    }

    /* now 'm' points at the insertion point in the table */
    return (m);
}

ham_status_t
blob_duplicate_insert(ham_db_t *db, ham_offset_t table_id, 
        ham_record_t *record, ham_size_t position, ham_u32_t flags, 
        dupe_entry_t *entries, ham_size_t num_entries, 
        ham_offset_t *rid, ham_size_t *new_position)
{
    ham_status_t st=0;
    dupe_table_t *table=0;
    ham_bool_t alloc_table=0;
	ham_bool_t resize=0;
    ham_page_t *page=0;
    ham_env_t *env=db_get_env(db);

    /*
     * create a new duplicate table if none existed, and insert
     * the first entry
     */
    if (!table_id) {
        ham_assert(num_entries==2, (""));
        /* allocates space for 8 (!) entries */
        table=allocator_calloc(env_get_allocator(env), 
                        sizeof(dupe_table_t)+7*sizeof(dupe_entry_t));
        if (!table)
            return HAM_OUT_OF_MEMORY;
        dupe_table_set_capacity(table, 8);
        dupe_table_set_count(table, 1);
        memcpy(dupe_table_get_entry(table, 0), &entries[0], 
                        sizeof(entries[0]));

        /* skip the first entry */
        entries++;
        num_entries--;
        alloc_table=1;
    }
    else {
        /*
         * otherwise load the existing table 
         */
        st=__get_duplicate_table(&table, &page, env, table_id);
		ham_assert(st ? table == NULL : 1, (0));
		ham_assert(st ? page == NULL : 1, (0));
        if (!table)
			return st ? st : HAM_INTERNAL_ERROR;
        if (!page && !(env_get_rt_flags(env)&HAM_IN_MEMORY_DB))
            alloc_table=1;
    }

    if (page)
        if ((st=ham_log_add_page_before(page)))
            return (st);

    ham_assert(num_entries==1, (""));

    /*
     * resize the table, if necessary
     */ 
    if (!(flags & HAM_OVERWRITE)
            && dupe_table_get_count(table)+1>=dupe_table_get_capacity(table)) {
        dupe_table_t *old=table;
        ham_size_t new_cap=dupe_table_get_capacity(table);

        if (new_cap < 3*8)
            new_cap += 8;
        else
            new_cap += new_cap/3;

        table=allocator_calloc(env_get_allocator(env), sizeof(dupe_table_t)+
                        (new_cap-1)*sizeof(dupe_entry_t));
        if (!table)
            return (HAM_OUT_OF_MEMORY);
        dupe_table_set_capacity(table, new_cap);
        dupe_table_set_count(table, dupe_table_get_count(old));
        memcpy(dupe_table_get_entry(table, 0), dupe_table_get_entry(old, 0),
                       dupe_table_get_count(old)*sizeof(dupe_entry_t));
        if (alloc_table)
            allocator_free(env_get_allocator(env), old);

        alloc_table=1;
        resize=1;
    }

    /*
     * insert sorted, unsorted or overwrite the entry at the requested position
     */
    if (flags&HAM_OVERWRITE) {
        dupe_entry_t *e=dupe_table_get_entry(table, position);

        if (!(dupe_entry_get_flags(e)&(KEY_BLOB_SIZE_SMALL
                                    |KEY_BLOB_SIZE_TINY
                                    |KEY_BLOB_SIZE_EMPTY))) {
            (void)blob_free(env, db, dupe_entry_get_rid(e), 0);
        }

        memcpy(dupe_table_get_entry(table, position), 
                        &entries[0], sizeof(entries[0]));
    }
    else {
        if (db_get_rt_flags(db)&HAM_SORT_DUPLICATES) {
            if (page)
                page_add_ref(page);
            position=__get_sorted_position(db, table, record, flags);
            if (page)
                page_release_ref(page);
        }
        else if (flags&HAM_DUPLICATE_INSERT_BEFORE) {
            /* do nothing, insert at the current position */
        }
        else if (flags&HAM_DUPLICATE_INSERT_AFTER) {
            position++;
            if (position > dupe_table_get_count(table))
                position=dupe_table_get_count(table);
        }
        else if (flags&HAM_DUPLICATE_INSERT_FIRST) {
            position=0;
        }
        else if (flags&HAM_DUPLICATE_INSERT_LAST) {
            position=dupe_table_get_count(table);
        }
        else {
            position=dupe_table_get_count(table);
        }

        if (position != dupe_table_get_count(table)) {
            memmove(dupe_table_get_entry(table, position+1), 
                dupe_table_get_entry(table, position), 
                sizeof(entries[0])*(dupe_table_get_count(table)-position));
        }

        memcpy(dupe_table_get_entry(table, position), 
                &entries[0], sizeof(entries[0]));

        dupe_table_set_count(table, dupe_table_get_count(table)+1);
    }

    /*
     * write the table back to disk and return the blobid of the table
     */
    if ((table_id && !page) || resize) 
    {
        st=blob_overwrite(env, db, table_id, (ham_u8_t *)table,
                sizeof(dupe_table_t)
                    +(dupe_table_get_capacity(table)-1)*sizeof(dupe_entry_t),
                0, rid);
    }
    else if (!table_id) 
    {
        st=blob_allocate(env, db, (ham_u8_t *)table,
                sizeof(dupe_table_t)
                    +(dupe_table_get_capacity(table)-1)*sizeof(dupe_entry_t),
                0, rid);
    }
    else if (table_id && page) 
    {
        page_set_dirty(page, env);
    }
    else
	{
        ham_assert(!"shouldn't be here", (0));
	}

    if (alloc_table)
        allocator_free(env_get_allocator(env), table);

    if (new_position)
        *new_position=position;

    return (st);
}

ham_status_t
blob_duplicate_erase(ham_db_t *db, ham_offset_t table_id,
        ham_size_t position, ham_u32_t flags, ham_offset_t *new_table_id)
{
    ham_status_t st;
    ham_record_t rec;
    ham_size_t i;
    dupe_table_t *table;
    ham_offset_t rid;
	ham_env_t *env = db_get_env(db);

    /* store the public record pointer, otherwise it's destroyed */
    ham_size_t rs=db_get_record_allocsize(db);
    void      *rp=db_get_record_allocdata(db);
    db_set_record_allocdata(db, 0);
    db_set_record_allocsize(db, 0);

    memset(&rec, 0, sizeof(rec));

    if (new_table_id)
        *new_table_id=table_id;

    st=blob_read(db, table_id, &rec, 0);
    if (st)
        return (st);

    /* restore the public record pointer */
    db_set_record_allocsize(db, rs);
    db_set_record_allocdata(db, rp);

    table=(dupe_table_t *)rec.data;

    /*
     * if BLOB_FREE_ALL_DUPES is set *OR* if the last duplicate is deleted:
     * free the whole duplicate table
     */
    if (flags&BLOB_FREE_ALL_DUPES
            || (position==0 && dupe_table_get_count(table)==1)) {
        for (i=0; i<dupe_table_get_count(table); i++) {
            dupe_entry_t *e=dupe_table_get_entry(table, i);
            if (!(dupe_entry_get_flags(e)&(KEY_BLOB_SIZE_SMALL
                                        |KEY_BLOB_SIZE_TINY
                                        |KEY_BLOB_SIZE_EMPTY))) {
                st=blob_free(env, db, dupe_entry_get_rid(e), 0);
                if (st) {
                    allocator_free(env_get_allocator(env), table);
                    return (st);
                }
            }
        }
        st=blob_free(env, db, table_id, 0); /* [i_a] isn't this superfluous (& 
                                        * dangerous), thanks to the 
                                        * free_all_dupes loop above??? */
        allocator_free(env_get_allocator(env), table);
        if (st)
            return (st);

        if (new_table_id)
            *new_table_id=0;

        return (0);
    }
    else {
        dupe_entry_t *e=dupe_table_get_entry(table, position);
        if (!(dupe_entry_get_flags(e)&(KEY_BLOB_SIZE_SMALL
                                    |KEY_BLOB_SIZE_TINY
                                    |KEY_BLOB_SIZE_EMPTY))) {
            st=blob_free(env, db, dupe_entry_get_rid(e), 0);
            if (st) {
                allocator_free(env_get_allocator(env), table);
                return (st);
            }
        }
        memmove(e, e+1,
            ((dupe_table_get_count(table)-position)-1)*sizeof(dupe_entry_t));
        dupe_table_set_count(table, dupe_table_get_count(table)-1);

        st=blob_overwrite(env, db, table_id, (ham_u8_t *)table,
                sizeof(dupe_table_t)
                    +(dupe_table_get_capacity(table)-1)*sizeof(dupe_entry_t),
                0, &rid);
        if (st) {
            allocator_free(env_get_allocator(env), table);
            return (st);
        }
        if (new_table_id)
            *new_table_id=rid;
    }

    /*
     * return 0 as a rid if the table is empty
     */
    if (dupe_table_get_count(table)==0)
        if (new_table_id)
            *new_table_id=0;

    allocator_free(env_get_allocator(env), table);
    return (0);
}

ham_status_t
blob_duplicate_get_count(ham_env_t *env, ham_offset_t table_id,
        ham_size_t *count, dupe_entry_t *entry)
{
	ham_status_t st;
    dupe_table_t *table;
    ham_page_t *page=0;

    st=__get_duplicate_table(&table, &page, env, table_id);
	ham_assert(st ? table == NULL : 1, (0));
	ham_assert(st ? page == NULL : 1, (0));
    if (!table)
		return st ? st : HAM_INTERNAL_ERROR;

    *count=dupe_table_get_count(table);
    if (entry)
        memcpy(entry, dupe_table_get_entry(table, (*count)-1), sizeof(*entry));

    if (!(env_get_rt_flags(env)&HAM_IN_MEMORY_DB))
	{
        if (!page)
            allocator_free(env_get_allocator(env), table);
	}
    return (0);
}

ham_status_t 
blob_duplicate_get(ham_env_t *env, ham_offset_t table_id,
        ham_size_t position, dupe_entry_t *entry)
{
	ham_status_t st;
    dupe_table_t *table;
    ham_page_t *page=0;

    st = __get_duplicate_table(&table, &page, env, table_id);
	ham_assert(st ? table == NULL : 1, (0));
	ham_assert(st ? page == NULL : 1, (0));
    if (!table)
		return st ? st : HAM_INTERNAL_ERROR;

    if (position>=dupe_table_get_count(table)) 
	{
        if (!(env_get_rt_flags(env)&HAM_IN_MEMORY_DB))
            if (!page)
                allocator_free(env_get_allocator(env), table);
        return HAM_KEY_NOT_FOUND;
    }
    memcpy(entry, dupe_table_get_entry(table, position), sizeof(*entry));

    if (!(env_get_rt_flags(env)&HAM_IN_MEMORY_DB))
	{
        if (!page)
            allocator_free(env_get_allocator(env), table);
	}
    return (0);
}

