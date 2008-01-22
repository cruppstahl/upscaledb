/**
 * Copyright (C) 2005-2008 Christoph Rupp (chris@crupp.de).
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
#include "db.h"
#include "endian.h"
#include "freelist.h"
#include "error.h"

static ham_status_t
__freel_cache_resize(ham_db_t *db, freelist_cache_t *cache, 
        ham_size_t new_count)
{
    ham_size_t i;
    freelist_entry_t *entries;
    ham_size_t size;
    size=db_get_usable_pagesize(db)-
        sizeof(freelist_payload_t)+1; /* +1 for bitmap[1] */

    entries=ham_mem_alloc(db, sizeof(*entries)*new_count);
    if (!entries)
        return (db_set_error(db, HAM_OUT_OF_MEMORY));
    memcpy(entries, freel_cache_get_entries(cache),
            sizeof(*entries)*freel_cache_get_count(cache));
    
    for (i=freel_cache_get_count(cache); i<new_count; i++) {
        freelist_entry_t *entry=&entries[i];

        memset(entry, 0, sizeof(*entry));

        freel_entry_set_start_address(entry, 
                freel_entry_get_start_address(&entries[i-1])+
                    freel_entry_get_max_bits(&entries[i-1])*DB_CHUNKSIZE);
        freel_entry_set_max_bits(entry, size*8);
    }

    ham_mem_free(db, freel_cache_get_entries(cache));
    freel_cache_set_entries(cache, entries);
    freel_cache_set_count(cache, new_count);

    return (0);
}

static freelist_entry_t *
__freel_cache_get_entry(ham_db_t *db, freelist_cache_t *cache, 
        ham_offset_t address)
{
    ham_size_t i=0;
    ham_status_t st=0;
    freelist_entry_t *entries;
    
    do {
        entries=freel_cache_get_entries(cache);

        for (; i<freel_cache_get_count(cache); i++) {
            freelist_entry_t *entry=&entries[i];
    
            ham_assert(!(address<freel_entry_get_start_address(entry)), (""));

            if (address>=freel_entry_get_start_address(entry)
                    && address<freel_entry_get_start_address(entry)+
                        freel_entry_get_max_bits(entry)*DB_CHUNKSIZE)
                return (entry);
        }

        /*
         * not found? resize the table
         */
        st=__freel_cache_resize(db, cache, i+8);
        if (st) {
            db_set_error(db, st);
            return (0);
        }
    } while (1);

    ham_assert(!"shouldn't be here", (""));
    return (0);
}

static ham_size_t
__freel_set_bits(freelist_payload_t *fp, ham_size_t start_bit, 
        ham_size_t size_bits, ham_bool_t set)
{
    ham_size_t i;
    ham_u8_t *p=freel_get_bitmap(fp);

    ham_assert(start_bit<freel_get_max_bits(fp), (""));

    if (start_bit+size_bits>freel_get_max_bits(fp))
        size_bits=freel_get_max_bits(fp)-start_bit;

    if (set) {
        for (i=0; i<size_bits; i++, start_bit++) {
            ham_assert(!(p[start_bit/8] & 1 << (start_bit%8)), 
                    ("bit is already set!"));
            p[start_bit>>3] |= 1 << (start_bit%8);
        }
    }
    else {
        for (i=0; i<size_bits; i++, start_bit++) {
            ham_assert((p[start_bit/8] & 1 << (start_bit%8)), 
                    ("bit is already deleted!"));
            p[start_bit>>3] &= ~(1 << (start_bit%8));
        }
    }

    return (size_bits*DB_CHUNKSIZE);
}

static ham_s32_t
__freel_search_bits(ham_db_t *db, freelist_payload_t *f, ham_size_t size_bits)
{
    ham_size_t bit=0, i, j, max=freel_get_max_bits(f), 
               max64=(freel_get_max_bits(f)>>3)>>3;
    ham_u64_t *p64=(ham_u64_t *)freel_get_bitmap(f);
    ham_u8_t *p=(ham_u8_t *)p64;
    ham_size_t found=0, start=0;

    for (i=0; i<max64; i++) {
        if (p64[i]) {
            bit=i*64;
            for (j=0; j<64 && bit<max; j++) {
                if (p[bit/8] & 1 << (bit%8)) {
                    if (!found)
                        start=bit;
                    found++;
                    if (found==size_bits)
                        return (start);
                }
                else
                    found=0;
                bit++;
            }
        }
        else 
            found=0;
    }

    return (-1);
}

static ham_s32_t
__freel_search_aligned_bits(ham_db_t *db, freelist_payload_t *fp, 
        ham_size_t size_bits)
{
    ham_size_t i=0, j, start, max=freel_get_max_bits(fp);
    ham_u64_t *p64=(ham_u64_t *)freel_get_bitmap(fp);
    ham_u8_t *p=(ham_u8_t *)p64;

    /* fix the start position, if the start-address of this page is 
     * not page-aligned */
    if (freel_get_start_address(fp)%db_get_pagesize(db)) {
        ham_offset_t start=((freel_get_start_address(fp)+db_get_pagesize(db))
                /db_get_pagesize(db))*db_get_pagesize(db);
        i=(ham_size_t)((start-freel_get_start_address(fp)/DB_CHUNKSIZE));
        max-=db_get_pagesize(db)/DB_CHUNKSIZE;
    }

    for (; i<max/size_bits; i+=db_get_pagesize(db)/DB_CHUNKSIZE) {
        if (p[i/8] & 1 << (i%8)) {
            start=i;
            for (j=0; j<size_bits; j++) {
                if (!(p[(start+j)/8] & 1 << ((start+j)%8)))
                    break;
            }
            if (j==size_bits)
                return (start);
        }
    }

    return (-1);
}

static ham_page_t *
__freel_alloc_page(ham_db_t *db, freelist_cache_t *cache, 
        freelist_entry_t *entry)
{
    ham_size_t i;
    freelist_entry_t *entries=freel_cache_get_entries(cache);
    ham_page_t *page=0;
    freelist_payload_t *fp;
    ham_size_t size=db_get_usable_pagesize(db)-
        sizeof(freelist_payload_t)+1; /* +1 for bitmap[1] */

    /* 
     * it's not enough just to allocate the page - we have to make sure
     * that the freelist pages build a linked list; therefore we
     * might have to allocate more than just one page...
     *
     * we can skip the first element - it's the root page and always exists
     */
    for (i=1; i<freel_cache_get_count(cache); i++) {
        if (!freel_entry_get_page_id(&entries[i])) {
            /*
             * load the previous page and the payload object; 
             * make the page dirty
             */
            if (i==1) {
                fp=db_get_freelist(db);
                db_set_dirty(db, HAM_TRUE);
            }
            else {
                page=db_fetch_page(db, 
                        freel_entry_get_page_id(&entries[i-1]), 0);
                if (!page)
                    return (0);
                page_set_dirty(page, HAM_TRUE);
                fp=page_get_freelist(page);
            }

            /*
             * allocate a new page, fixed the linked list
             */
            page=db_alloc_page(db, PAGE_TYPE_FREELIST, 
                    PAGE_IGNORE_FREELIST|PAGE_CLEAR_WITH_ZERO);
            if (!page)
                return (0);
            freel_set_overflow(fp, page_get_self(page));

            fp=page_get_freelist(page);
            freel_set_start_address(fp, 
                    freel_entry_get_start_address(&entries[i]));
            freel_set_max_bits(fp, size*8);
            page_set_dirty(page, HAM_TRUE);
            ham_assert(freel_entry_get_max_bits(&entries[i])==
                    freel_get_max_bits(fp), (""));
            freel_entry_set_page_id(&entries[i], page_get_self(page));
        }

        if (&entries[i]==entry)
            return (page);
    }

    ham_assert(!"shouldn't be here", (""));

    return (page);
}

static ham_offset_t
__freel_alloc_area(ham_db_t *db, ham_size_t size, ham_bool_t aligned)
{
    ham_status_t st;
    ham_size_t i;
    freelist_entry_t *entry;
    freelist_payload_t *fp;
    freelist_cache_t *cache=db_get_freelist_cache(db);
    ham_txn_t txn, *old_txn=db_get_txn(db);
    ham_page_t *page=0;
    ham_s32_t s=-1;

    ham_assert(size%DB_CHUNKSIZE==0, (0));

    if ((st=ham_txn_begin(&txn, db)))
        return (db_set_error(db, st));

    for (i=0; i<freel_cache_get_count(cache); i++) {
        entry=freel_cache_get_entries(cache)+i;

        /*
         * does theis freelist entry have enough allocated blocks to satisfy
         * the request?
         */
        if (freel_entry_get_allocated_bits(entry)>=size/DB_CHUNKSIZE) {
            /*
             * yes, load the payload structure
             */
            if (i==0) {
                fp=db_get_freelist(db);
            }
            else {
                page=db_fetch_page(db, freel_entry_get_page_id(entry), 0);
                if (!page) {
                    (void)ham_txn_abort(&txn);
                    db_set_txn(db, old_txn);
                    return (0);
                }
                fp=page_get_freelist(page);
            }

            /*
             * now try to allocate from this payload
             */
            if (aligned)
                s=__freel_search_aligned_bits(db, fp, size/DB_CHUNKSIZE);
            else
                s=__freel_search_bits(db, fp, size/DB_CHUNKSIZE);
            if (s!=-1) {
                __freel_set_bits(fp, s, size/DB_CHUNKSIZE, HAM_FALSE);
                if (page)
                    page_set_dirty(page, HAM_TRUE);
                else
                    db_set_dirty(db, HAM_TRUE);
                break;
            }
        }
    }

    if (s!=-1) {
        freel_set_allocated_bits(fp, 
                freel_get_allocated_bits(fp)-size/DB_CHUNKSIZE);
        freel_entry_set_allocated_bits(entry,
                freel_get_allocated_bits(fp));

        st=ham_txn_commit(&txn, 
                db_get_rt_flags(db)&HAM_DISABLE_FREELIST_FLUSH 
                ? 0 
                : TXN_FORCE_WRITE);
        db_set_txn(db, old_txn);
        if (st) {
            db_set_error(db, st);
            return (0);
        }

        return (freel_get_start_address(fp)+(s*DB_CHUNKSIZE));
    }

    (void)ham_txn_abort(&txn);
    db_set_txn(db, old_txn);
    return (0);
}

static ham_status_t
__freel_lazy_create(ham_db_t *db)
{
    ham_status_t st;
    ham_size_t size, entry_pos=1;
    freelist_cache_t *cache;
    freelist_entry_t *entry;
    freelist_payload_t *fp=db_get_freelist(db);
    ham_txn_t txn, *old_txn=db_get_txn(db);
    
    ham_assert(db_get_freelist_cache(db)==0, (""));

    cache=ham_mem_calloc(db, sizeof(*cache));
    if (!cache)
        return (db_set_error(db, HAM_OUT_OF_MEMORY));

    entry=ham_mem_calloc(db, sizeof(*entry)*8);
    if (!entry)
        return (db_set_error(db, HAM_OUT_OF_MEMORY));

    /*
     * add the header page to the freelist
     */
    freel_entry_set_start_address(&entry[0], db_get_pagesize(db));
    size=db_get_usable_pagesize(db)-
        ((char *)db_get_freelist(db)-
            (char *)page_get_payload(db_get_header_page(db)));
    size-=sizeof(freelist_payload_t);
    freel_entry_set_max_bits(&entry[0], size*8);

    /*
     * initialize the header page, if we have read/write access
     */
    if (!(db_get_rt_flags(db)&HAM_READ_ONLY)) {
        freel_set_start_address(fp, db_get_pagesize(db));
        freel_set_max_bits(fp, size*8);
    }

    freel_cache_set_count(cache, 1);
    freel_cache_set_entries(cache, entry);

    if (db_get_env(db))
        env_set_freelist_cache(db_get_env(db), cache);
    else
        db_set_freelist_cache(db, cache);

    /*
     * now load all other freelist pages
     */
    if ((st=ham_txn_begin(&txn, db)))
        return (db_set_error(db, st));

    while (1) {
        ham_page_t *page;
        if (!freel_get_overflow(fp))
            break;

        st=__freel_cache_resize(db, cache, freel_cache_get_count(cache)+1);
        if (st)
            return (db_set_error(db, st));

        page=db_fetch_page(db, freel_get_overflow(fp), 0);
        if (!page) {
            (void)ham_txn_abort(&txn);
            db_set_txn(db, old_txn);
            return (db_get_error(db));
        }

        fp=page_get_freelist(page);
        entry=freel_cache_get_entries(cache)+entry_pos;
        ham_assert(freel_entry_get_start_address(entry)==
                freel_get_start_address(fp), (""));
        freel_entry_set_allocated_bits(entry, 
                freel_get_allocated_bits(fp));
        freel_entry_set_page_id(entry, 
                page_get_self(page));

        entry_pos++;
    }

    st=ham_txn_commit(&txn, 0);
    db_set_txn(db, old_txn);
    return (st);
}

ham_status_t
freel_create(ham_db_t *db)
{
    (void)db;
    /*
     * when freel_create() is called, the db structure is not yet fully 
     * initialized, therefore the initialization of the freelist is deferred.
     */
    return (0);
}

ham_status_t
freel_shutdown(ham_db_t *db)
{
    freelist_cache_t *cache;

    if (db_get_rt_flags(db)&HAM_IN_MEMORY_DB)
        return (0);

    cache=db_get_freelist_cache(db);
    if (!cache)
        return (0);

    if (freel_cache_get_entries(cache))
        ham_mem_free(db, freel_cache_get_entries(cache));

    ham_mem_free(db, cache);
    if (db_get_env(db))
        env_set_freelist_cache(db_get_env(db), 0);
    else
        db_set_freelist_cache(db, 0);

    return (0);
}

ham_status_t
freel_mark_free(ham_db_t *db, ham_offset_t address, ham_size_t size)
{
    ham_status_t st;
    ham_txn_t txn, *old_txn=db_get_txn(db);
    ham_page_t *page=0;
    freelist_cache_t *cache;
    freelist_entry_t *entry;
    freelist_payload_t *fp;
    ham_size_t s;

    if (db_get_rt_flags(db)&HAM_IN_MEMORY_DB)
        return (0);

    ham_assert(size%DB_CHUNKSIZE==0, (0));
    ham_assert(address%DB_CHUNKSIZE==0, (0));

    if (!db_get_freelist_cache(db))
        __freel_lazy_create(db);
    cache=db_get_freelist_cache(db);

    if ((st=ham_txn_begin(&txn, db)))
        return (db_set_error(db, st));

    /*
     * split the chunk if it doesn't fit in one freelist page
     */
    while (size) {
        /*
         * get the cache entry of this address
         */
        entry=__freel_cache_get_entry(db, cache, address);

        /*
         * allocate a page if necessary
         */
        if (!freel_entry_get_page_id(entry)) {
            if (freel_entry_get_start_address(entry)==db_get_pagesize(db)) {
                fp=db_get_freelist(db);
            }
            else {
                page=__freel_alloc_page(db, cache, entry);
                if (!page) {
                    (void)ham_txn_abort(&txn);
                    db_set_txn(db, old_txn);
                    return (db_get_error(db));
                }
                fp=page_get_freelist(page);
            }
        }
        /*
         * otherwise just fetch the page from the cache or the disk
         */
        else {
            page=db_fetch_page(db, freel_entry_get_page_id(entry), 0);
            if (!page) {
                (void)ham_txn_abort(&txn);
                db_set_txn(db, old_txn);
                return (db_get_error(db));
            }
            fp=page_get_freelist(page);
        }

        ham_assert(address>=freel_get_start_address(fp), (0));

        /*
         * set the bits and update the values in the cache and
         * the fp
         */
        s=__freel_set_bits(fp, 
                (ham_size_t)(address-freel_get_start_address(fp))/
                    DB_CHUNKSIZE, size/DB_CHUNKSIZE, HAM_TRUE);

        freel_set_allocated_bits(fp, 
                freel_get_allocated_bits(fp)+s/DB_CHUNKSIZE);
        freel_entry_set_allocated_bits(entry, 
                freel_get_allocated_bits(fp));

        if (page)
            page_set_dirty(page, HAM_TRUE);
        else
            db_set_dirty(db, HAM_TRUE);

        size-=s;
        address+=s;
    }

    st=ham_txn_commit(&txn, 
            db_get_rt_flags(db)&HAM_DISABLE_FREELIST_FLUSH 
                ? 0 
                : TXN_FORCE_WRITE);
    db_set_txn(db, old_txn);
    return (st);
}

ham_offset_t
freel_alloc_area(ham_db_t *db, ham_size_t size)
{
    if (db_get_rt_flags(db)&HAM_IN_MEMORY_DB)
        return (0);

    if (!db_get_freelist_cache(db))
        __freel_lazy_create(db);

    return (__freel_alloc_area(db, size, HAM_FALSE));
}

ham_offset_t
freel_alloc_page(ham_db_t *db)
{
    if (db_get_rt_flags(db)&HAM_IN_MEMORY_DB)
        return (0);

    if (!db_get_freelist_cache(db))
        __freel_lazy_create(db);

    return (__freel_alloc_area(db, db_get_pagesize(db), HAM_TRUE));
}

