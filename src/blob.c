/**
 * Copyright (C) 2005-2007 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See file COPYING.GPL2 and COPYING.GPL3 for License information.
 *
 */

#include <string.h>
#include <ham/hamsterdb.h>
#include "os.h"
#include "db.h"
#include "blob.h"
#include "error.h"
#include "freelist.h"
#include "mem.h"
#include "page.h"

#define SMALLEST_CHUNK_SIZE  (sizeof(ham_offset_t)+sizeof(blob_t)+1)

#define my_blob_is_small(db, size)  (size<(ham_size_t)(db_get_pagesize(db)>>3))

static ham_status_t
__write_chunks(ham_db_t *db, ham_page_t *page, 
        ham_offset_t addr, ham_u8_t **chunk_data, ham_size_t *chunk_size, 
        ham_size_t chunks)
{
    ham_size_t i;
    ham_status_t st;
    ham_offset_t pageid;
    ham_device_t *device=db_get_device(db);

    /*
     * for each chunk...
     */
    for (i=0; i<chunks; i++) {
        while (chunk_size[i]) {
            /*
             * get the page-ID from this chunk
             */
            pageid=(addr/db_get_pagesize(db))*db_get_pagesize(db);

            /*
             * is this the current page?
             */
            if (page && page_get_self(page)!=pageid)
                page=0;

            /*
             * fetch the page from the cache, if it's in the cache
             */
            if (!page) {
                page=db_fetch_page(db, pageid, 
                        my_blob_is_small(db, chunk_size[i]) 
                        ? 0 : DB_ONLY_FROM_CACHE);
                /* blob pages don't have a page header */
                if (page)
                    page_set_npers_flags(page, 
                        page_get_npers_flags(page)|PAGE_NPERS_NO_HEADER);
            }

            /*
             * if we have a page pointer: use it; otherwise write directly
             * to the device
             */
            if (page) {
                ham_size_t writestart=
                        (ham_size_t)(addr-page_get_self(page));
                ham_size_t writesize =
                        (ham_size_t)(db_get_pagesize(db)-writestart);
                if (writesize>chunk_size[i])
                    writesize=chunk_size[i];
                memcpy(&page_get_raw_payload(page)[writestart], chunk_data[i],
                            writesize);
                page_set_dirty(page, 1);
                addr+=writesize;
                chunk_data[i]+=writesize;
                chunk_size[i]-=writesize;
            }
            else {
                ham_size_t s=chunk_size[i]<db_get_pagesize(db) 
                        ? chunk_size[i] : db_get_pagesize(db);
                /* limit to the next page boundary */
                if (s>pageid+db_get_pagesize(db)-addr)
                    s=(ham_size_t)(pageid+db_get_pagesize(db)-addr);

                st=device->write(db, device, addr, chunk_data[i], s);
                if (st)
                    return (st);
                addr+=s;
                chunk_data[i]+=s;
                chunk_size[i]-=s;
            }
        }
    }

    return (0);
}

static ham_status_t
__read_chunk(ham_db_t *db, ham_page_t *page, ham_page_t **fpage, 
        ham_offset_t addr, ham_u8_t *data, ham_size_t size)
{
    ham_status_t st;
    ham_offset_t pageid;
    ham_device_t *device=db_get_device(db);

    while (size) {
        /*
         * get the page-ID from this chunk
         */
        pageid=(addr/db_get_pagesize(db))*db_get_pagesize(db);

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
            page=db_fetch_page(db, pageid, 
                    my_blob_is_small(db, size) ? 0 : DB_ONLY_FROM_CACHE);
            /* blob pages don't have a page header */
            if (page)
                page_set_npers_flags(page, 
                    page_get_npers_flags(page)|PAGE_NPERS_NO_HEADER);
        }

        /*
         * if we have a page pointer: use it; otherwise read directly
         * from the device
         */
        if (page) {
            ham_size_t readstart=
                    (ham_size_t)(addr-page_get_self(page));
            ham_size_t readsize =
                    (ham_size_t)(db_get_pagesize(db)-readstart);
            if (readsize>size)
                readsize=size;
            memcpy(data, &page_get_raw_payload(page)[readstart], readsize);
            addr+=readsize;
            data+=readsize;
            size-=readsize;
        }
        else {
            ham_size_t s=size<db_get_pagesize(db) 
                    ? size : db_get_pagesize(db);
            /* limit to the next page boundary */
            if (s>pageid+db_get_pagesize(db)-addr)
                s=(ham_size_t)(pageid+db_get_pagesize(db)-addr);

            st=device->read(db, device, addr, data, s);
            if (st) 
                return (st);
            addr+=s;
            data+=s;
            size-=s;
        }
    }

    if (fpage)
        *fpage=page;

    return (0);
}

static dupe_table_t *
__get_duplicate_table(ham_db_t *db, ham_offset_t table_id, ham_page_t **page)
{
    ham_status_t st;
    blob_t hdr;
    ham_page_t *hdrpage=0;
    dupe_table_t *table;

    if (db_get_rt_flags(db)&HAM_IN_MEMORY_DB) {
        ham_u8_t *p=(ham_u8_t *)table_id;
        return ((dupe_table_t *)(p+sizeof(hdr)));
    }

    /*
     * load the blob header
     */
    st=__read_chunk(db, 0, &hdrpage, table_id, (ham_u8_t *)&hdr, sizeof(hdr));
    if (st) {
        db_set_error(db, st);
        return (0);
    }

    /*
     * if the whole table is in a page (and not split between several
     * pages), just return a pointer directly in the page
     */
    if (page_get_self(hdrpage)+db_get_usable_pagesize(db)>=
            table_id+blob_get_size(&hdr)) {
        ham_u8_t *p=page_get_raw_payload(hdrpage);
        /* yes, table is in the page */
        *page=hdrpage;
        return ((dupe_table_t *)
                &p[table_id-page_get_self(hdrpage)+sizeof(hdr)]);
    }

    /*
     * otherwise allocate memory for the table
     */
    table=ham_mem_alloc(db, (ham_size_t)blob_get_size(&hdr));
    if (!table) {
        db_set_error(db, HAM_OUT_OF_MEMORY);
        return (0);
    }

    /*
     * then read the rest of the blob
     */
    st=__read_chunk(db, hdrpage, 0, table_id+sizeof(hdr), 
            (ham_u8_t *)table, (ham_size_t)blob_get_size(&hdr));
    if (st) {
        db_set_error(db, st);
        return (0);
    }

    return (table);
}

ham_status_t
blob_allocate(ham_db_t *db, ham_u8_t *data, ham_size_t size, 
        ham_u32_t flags, ham_offset_t *blobid)
{
    ham_status_t st;
    ham_page_t *page=0;
    ham_offset_t addr;
    blob_t hdr;
    ham_u8_t *chunk_data[2];
    ham_size_t alloc_size, chunk_size[2];
    ham_device_t *device=db_get_device(db);
   
    *blobid=0;

    /*
     * in-memory-database: the blobid is actually a pointer to the memory
     * buffer, in which the blob (with the blob-header) is stored
     */
    if (db_get_rt_flags(db)&HAM_IN_MEMORY_DB) {
        blob_t *hdr;
        ham_u8_t *p=(ham_u8_t *)ham_mem_alloc(db, size+sizeof(blob_t));
        if (!p) {
            db_set_error(db, HAM_OUT_OF_MEMORY);
            return (HAM_OUT_OF_MEMORY);
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
    if (alloc_size%DB_CHUNKSIZE!=0)
        alloc_size=((alloc_size/DB_CHUNKSIZE)*DB_CHUNKSIZE)+DB_CHUNKSIZE;

    /* 
     * check if we have space in the freelist 
     */
    addr=freel_alloc_area(db, alloc_size);
    if (!addr) {
        if (db_get_error(db))
            return (db_get_error(db));

        /*
         * if the blob is small, we load the page through the cache
         */
        if (my_blob_is_small(db, alloc_size)) {
            page=db_alloc_page(db, PAGE_TYPE_B_INDEX, PAGE_IGNORE_FREELIST);
            if (!page)
                return (db_get_error(db));
            /* blob pages don't have a page header */
            page_set_npers_flags(page, 
                    page_get_npers_flags(page)|PAGE_NPERS_NO_HEADER);
            addr=page_get_self(page);
            /* move the remaining space to the freelist */
            (void)freel_mark_free(db, addr+alloc_size,
                    db_get_pagesize(db)-alloc_size);
            blob_set_alloc_size(&hdr, alloc_size);
        }
        /*
         * otherwise use direct IO to allocate the space
         */
        else {
            ham_size_t aligned=alloc_size;
            if (aligned%db_get_pagesize(db)) {
                aligned+=db_get_pagesize(db);
                aligned/=db_get_pagesize(db);
                aligned*=db_get_pagesize(db);
            }

            st=device->alloc(device, aligned, &addr);
            if (st) 
                return (st);

            /* if aligned!=size, and the remaining chunk is large enough:
             * move it to the freelist */
            if (aligned!=alloc_size) {
                ham_size_t diff=aligned-alloc_size;
                if (diff>SMALLEST_CHUNK_SIZE) {
                    (void)freel_mark_free(db, addr+alloc_size, diff);
                    blob_set_alloc_size(&hdr, aligned-diff);
                }
                else 
                    blob_set_alloc_size(&hdr, aligned);
            }
            else
                blob_set_alloc_size(&hdr, aligned);
        }
    }
    else
        blob_set_alloc_size(&hdr, alloc_size);

    blob_set_size(&hdr, size);
    blob_set_self(&hdr, addr);

    /* 
     * write header and data 
     */
    chunk_data[0]=(ham_u8_t *)&hdr;
    chunk_size[0]=sizeof(hdr);
    chunk_data[1]=(ham_u8_t *)data;
    chunk_size[1]=size;

    st=__write_chunks(db, page, addr, chunk_data, chunk_size, 2);
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

    record->size=0;

    /*
     * in-memory-database: the blobid is actually a pointer to the memory
     * buffer, in which the blob is stored
     */
    if (db_get_rt_flags(db)&HAM_IN_MEMORY_DB) {
        blob_t *hdr=(blob_t *)blobid;
        ham_u8_t *data=((ham_u8_t *)blobid)+sizeof(blob_t);

        /* when the database is closing, the header is already deleted */
        if (!hdr)
            return (0);

        record->size=(ham_size_t)blob_get_size(hdr);
        if (!record->size) {
            /* empty blob? */
            record->data=0;
        }
        else {
            /* resize buffer, if necessary */
            if (!(record->flags & HAM_RECORD_USER_ALLOC)) {
                st=db_resize_allocdata(db, (ham_size_t)blob_get_size(hdr));
                if (st)
                    return (st);
                record->data=db_get_record_allocdata(db);
            }
            /* and copy the data */
            memcpy(record->data, data, record->size);
        }

        return (0);
    }

    ham_assert(blobid%DB_CHUNKSIZE==0, ("blobid is %llu", blobid));

    /*
     * first step: read the blob header 
     */
    st=__read_chunk(db, 0, &page, blobid, (ham_u8_t *)&hdr, sizeof(hdr));
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
     * empty blob? 
     */
    record->size=(ham_size_t)blob_get_size(&hdr);
    if (!record->size) {
        record->data=0;
        return (0);
    }

    /*
     * second step: resize the blob buffer
     */
    if (!(record->flags & HAM_RECORD_USER_ALLOC)) {
        st=db_resize_allocdata(db, (ham_size_t)blob_get_size(&hdr));
        if (st)
            return (st);
        record->data=db_get_record_allocdata(db);
    }

    /*
     * third step: read the blob data
     */
    st=__read_chunk(db, page, 0, blobid+sizeof(blob_t), record->data, 
            (ham_size_t)blob_get_size(&hdr));
    if (st)
        return (st);

    record->size=(ham_size_t)blob_get_size(&hdr);

    return (0);
}

ham_status_t
blob_overwrite(ham_db_t *db, ham_offset_t old_blobid, 
        ham_u8_t *data, ham_size_t size, ham_u32_t flags, 
        ham_offset_t *new_blobid)
{
    ham_status_t st;
    ham_size_t alloc_size;
    blob_t old_hdr, new_hdr;
    ham_page_t *page;

    /*
     * inmemory-databases: free the old blob, 
     * allocate a new blob (but if both sizes are equal, just overwrite
     * the data)
     */
    if (db_get_rt_flags(db)&HAM_IN_MEMORY_DB) {
        blob_t *nhdr, *phdr=(blob_t *)old_blobid;

        if (blob_get_size(phdr)==size) {
            ham_u8_t *p=(ham_u8_t *)phdr;
            memmove(p+sizeof(blob_t), data, size);
            *new_blobid=(ham_offset_t)phdr;
        }
        else {
            st=blob_allocate(db, data, size, flags, new_blobid);
            if (st)
                return (st);
            nhdr=(blob_t *)*new_blobid;
            blob_set_flags(nhdr, blob_get_flags(phdr));

            ham_mem_free(db, phdr);
        }

        return (0);
    }

    ham_assert(old_blobid%DB_CHUNKSIZE==0, (0));

    /*
     * blobs are CHUNKSIZE-allocated 
     */
    alloc_size=sizeof(blob_t)+size;
    if (alloc_size%DB_CHUNKSIZE!=0)
        alloc_size=((alloc_size/DB_CHUNKSIZE)*DB_CHUNKSIZE)+DB_CHUNKSIZE;

    /*
     * first, read the blob header; if the new blob fits into the 
     * old blob, we overwrite the old blob (and add the remaining
     * space to the freelist, if there is any)
     */
    st=__read_chunk(db, 0, &page, old_blobid, (ham_u8_t *)&old_hdr, 
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
     * now compare the sizes
     */
    if (alloc_size<=blob_get_alloc_size(&old_hdr)) {
        ham_u8_t *chunk_data[2]={(ham_u8_t *)&new_hdr, data};
        ham_size_t chunk_size[2]={sizeof(new_hdr), size};

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

        st=__write_chunks(db, page, blob_get_self(&new_hdr),
                chunk_data, chunk_size, 2);
        if (st)
            return (st);

        /*
         * move remaining data to the freelist
         */
        if (blob_get_alloc_size(&old_hdr)!=blob_get_alloc_size(&new_hdr)) {
            (void)freel_mark_free(db, 
                  blob_get_self(&new_hdr)+blob_get_alloc_size(&new_hdr), 
                  (ham_size_t)(blob_get_alloc_size(&old_hdr)-
                    blob_get_alloc_size(&new_hdr)));
        }

        /*
         * the old rid is the new rid
         */
        *new_blobid=blob_get_self(&new_hdr);

        return (0);
    }
    else {
        st=blob_allocate(db, data, size, flags, new_blobid);
        if (st)
            return (st);

        (void)freel_mark_free(db, old_blobid, 
                (ham_size_t)blob_get_alloc_size(&old_hdr));
    }

    return (0);
}

ham_status_t
blob_free(ham_db_t *db, ham_offset_t blobid, ham_u32_t flags)
{
    ham_status_t st;
    blob_t hdr;

    /*
     * in-memory-database: the blobid is actually a pointer to the memory
     * buffer, in which the blob is stored
     */
    if (db_get_rt_flags(db)&HAM_IN_MEMORY_DB) {
        ham_mem_free(db, (blob_t *)blobid);
        return (0);
    }

    ham_assert(blobid%DB_CHUNKSIZE==0, (0));

    /*
     * fetch the blob header 
     */
    st=__read_chunk(db, 0, 0, blobid, (ham_u8_t *)&hdr, sizeof(hdr));
    if (st)
        return (st);

    ham_assert(blob_get_alloc_size(&hdr)%DB_CHUNKSIZE==0, (0));

    /*
     * sanity check
     */
    ham_assert(blob_get_self(&hdr)==blobid, 
            ("invalid blobid %llu != %llu", blob_get_self(&hdr), blobid);)
    if (blob_get_self(&hdr)!=blobid)
        return (HAM_BLOB_NOT_FOUND);

    /*
     * move the blob to the freelist
     */
    (void)freel_mark_free(db, blobid, 
            (ham_size_t)blob_get_alloc_size(&hdr));

    return (0);
}

ham_status_t
blob_duplicate_insert(ham_db_t *db, ham_offset_t table_id, 
        ham_size_t position, ham_u32_t flags, 
        dupe_entry_t *entries, ham_size_t num_entries, 
        ham_offset_t *rid, ham_size_t *new_position)
{
    ham_status_t st=0;
    dupe_table_t *table=0;
    ham_bool_t alloc_table=0, resize=0;
    ham_page_t *page=0;

    /*
     * create a new duplicate table if none existed, and insert
     * the first entry
     */
    if (!table_id) {
        ham_assert(num_entries==2, (""));
        /* allocates space for 8 (!) entries */
        table=ham_mem_calloc(db, sizeof(dupe_table_t)+7*sizeof(dupe_entry_t));
        if (!table)
            return (db_set_error(db, HAM_OUT_OF_MEMORY));
        dupe_table_set_capacity(table, 8);
        dupe_table_set_count(table, 1);
        memcpy(dupe_table_get_entry(table, 0), &entries[0], sizeof(entries[0]));

        /* skip the first entry */
        entries++;
        num_entries--;
        alloc_table=1;
    }
    /*
     * otherwise load the existing table 
     */
    else {
        table=__get_duplicate_table(db, table_id, &page);
        if (!table)
            return (db_get_error(db));
        if (!page && !(db_get_rt_flags(db)&HAM_IN_MEMORY_DB))
            alloc_table=1;
    }

    ham_assert(num_entries==1, (""));

    /*
     * resize the table, if necessary
     */ 
    if (!(flags&HAM_OVERWRITE)
            && dupe_table_get_count(table)+1>=dupe_table_get_capacity(table)) {
        dupe_table_t *old=table;
        ham_size_t new_cap=dupe_table_get_capacity(table);

        if (new_cap/3<8)
            new_cap+=8;
        else
            new_cap+=new_cap/3;

        table=ham_mem_calloc(db, sizeof(dupe_table_t)+
                        (new_cap-1)*sizeof(dupe_entry_t));
        if (!table)
            return (db_set_error(db, HAM_OUT_OF_MEMORY));
        dupe_table_set_capacity(table, new_cap);
        dupe_table_set_count(table, dupe_table_get_count(old));
        memcpy(dupe_table_get_entry(table, 0), dupe_table_get_entry(old, 0),
                       dupe_table_get_count(old)*sizeof(dupe_entry_t));
        if (alloc_table)
            ham_mem_free(db, old);

        alloc_table=1;
        resize=1;
    }

    /*
     * insert (or overwrite) the entry at the requested position
     */
    if (flags&HAM_OVERWRITE) {
        dupe_entry_t *e=dupe_table_get_entry(table, position);

        if (!((dupe_entry_get_flags(e)&KEY_BLOB_SIZE_SMALL)
                || (dupe_entry_get_flags(e)&KEY_BLOB_SIZE_TINY)
                || (dupe_entry_get_flags(e)&KEY_BLOB_SIZE_EMPTY)))
            (void)blob_free(db, dupe_entry_get_rid(e), 0);

        memcpy(dupe_table_get_entry(table, position), 
                        &entries[0], sizeof(entries[0]));
    }
    else {
        if (flags&HAM_DUPLICATE_INSERT_BEFORE) {
            /* do nothing, insert at the current position */
        }
        else if (flags&HAM_DUPLICATE_INSERT_AFTER) {
            position++;
            if (position>=dupe_table_get_count(table))
                position=dupe_table_get_count(table);
        }
        else if (flags&HAM_DUPLICATE_INSERT_FIRST) {
            position=0;
        }
        else { /* HAM_DUPLICATE_INSERT_LAST and default */
            position=dupe_table_get_count(table);
        }

        if (position!=dupe_table_get_count(table))
            memmove(dupe_table_get_entry(table, position+1), 
                dupe_table_get_entry(table, position), 
                sizeof(entries[0])*(dupe_table_get_count(table)-position));

        memcpy(dupe_table_get_entry(table, position), 
                &entries[0], sizeof(entries[0]));

        dupe_table_set_count(table, dupe_table_get_count(table)+1);
    }

    /*
     * write the table back to disk and return the blobid of the table
     */
    if ((table_id && !page) || resize) {
        st=blob_overwrite(db, table_id, (ham_u8_t *)table,
                sizeof(dupe_table_t)
                    +(dupe_table_get_capacity(table)-1)*sizeof(dupe_entry_t),
                0, rid);
    }
    else if (!table_id) {
        st=blob_allocate(db, (ham_u8_t *)table,
                sizeof(dupe_table_t)
                    +(dupe_table_get_capacity(table)-1)*sizeof(dupe_entry_t),
                0, rid);
    }
    else if (table_id && page) {
        page_set_dirty(page, 1);
    }
    else
        ham_assert(!"shouldn't be here", (""));

    if (alloc_table)
        ham_mem_free(db, table);

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
            if (!((dupe_entry_get_flags(e)&KEY_BLOB_SIZE_SMALL)
                    || (dupe_entry_get_flags(e)&KEY_BLOB_SIZE_TINY)
                    || (dupe_entry_get_flags(e)&KEY_BLOB_SIZE_EMPTY))) {
                st=blob_free(db, dupe_entry_get_rid(e), 0);
                if (st) {
                    ham_mem_free(db, table);
                    return (st);
                }
            }
        }
        st=blob_free(db, table_id, 0);
        ham_mem_free(db, table);
        if (st)
            return (st);

        if (new_table_id)
            *new_table_id=0;

        return (0);
    }
    else {
        dupe_entry_t *e=dupe_table_get_entry(table, position);
        if (!((dupe_entry_get_flags(e)&KEY_BLOB_SIZE_SMALL)
                || (dupe_entry_get_flags(e)&KEY_BLOB_SIZE_TINY)
                || (dupe_entry_get_flags(e)&KEY_BLOB_SIZE_EMPTY))) {
            st=blob_free(db, dupe_entry_get_rid(e), 0);
            if (st) {
                ham_mem_free(db, table);
                return (st);
            }
        }
        memmove(e, e+1,
            ((dupe_table_get_count(table)-position)-1)*sizeof(dupe_entry_t));
        dupe_table_set_count(table, dupe_table_get_count(table)-1);

        st=blob_overwrite(db, table_id, (ham_u8_t *)table,
                sizeof(dupe_table_t)
                    +(dupe_table_get_capacity(table)-1)*sizeof(dupe_entry_t),
                0, &rid);
        if (st) {
            ham_mem_free(db, table);
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

    ham_mem_free(db, table);
    return (0);
}

ham_status_t
blob_duplicate_get_count(ham_db_t *db, ham_offset_t table_id,
        ham_size_t *count, dupe_entry_t *entry)
{
    dupe_table_t *table;
    ham_page_t *page=0;

    table=__get_duplicate_table(db, table_id, &page);
    if (!table)
        return (db_get_error(db));

    *count=dupe_table_get_count(table);
    if (entry)
        memcpy(entry, dupe_table_get_entry(table, (*count)-1), sizeof(*entry));

    if (!(db_get_rt_flags(db)&HAM_IN_MEMORY_DB))
        if (!page)
            ham_mem_free(db, table);
    return (0);
}

ham_status_t 
blob_duplicate_get(ham_db_t *db, ham_offset_t table_id,
        ham_size_t position, dupe_entry_t *entry)
{
    dupe_table_t *table;
    ham_page_t *page=0;

    table=__get_duplicate_table(db, table_id, &page);
    if (!table)
        return (db_get_error(db));

    if (position>=dupe_table_get_count(table)) {
        if (!(db_get_rt_flags(db)&HAM_IN_MEMORY_DB))
            if (!page)
                ham_mem_free(db, table);
        return (db_set_error(db, HAM_KEY_NOT_FOUND));
    }
    memcpy(entry, dupe_table_get_entry(table, position), sizeof(*entry));

    if (!(db_get_rt_flags(db)&HAM_IN_MEMORY_DB))
        if (!page)
            ham_mem_free(db, table);
    return (0);
}

