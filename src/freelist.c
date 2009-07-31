/**
 * Copyright (C) 2005-2008 Christoph Rupp (chris@crupp.de).
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
#include <ham/hamsterdb.h>
#include "db.h"
#include "endian.h"
#include "freelist.h"
#include "error.h"

#define FORCE_UNITTEST_PASS

static ham_status_t
__freel_cache_resize(ham_db_t *db, freelist_cache_t *cache, 
        ham_size_t new_count)
{
    ham_size_t i;
    freelist_entry_t *entries;
    ham_size_t size;
    size = db_get_usable_pagesize(db) - db_get_freelist_header_size16();
	ham_assert((size % sizeof(ham_u64_t)) == 0, ("freelist bitarray size must be == 0 MOD sizeof(ham_u64_t) due to the scan algorithm"));
	size -= size % sizeof(ham_u64_t);

	ham_assert(new_count > freel_cache_get_count(cache), (0));
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
		ham_assert((size % sizeof(ham_u64_t)) == 0, ("freelist bitarray size must be == 0 MOD sizeof(ham_u64_t) due to the scan algorithm"));
		// ham_assert(size < (1ULL << 8*sizeof(entry->_max_bits)), ("size must fit in the _max_bits type"));
		// ham_assert(size*8 < (1ULL << 8*sizeof(entry->_max_bits)), ("size must fit in the _max_bits type"));
#if !defined(FORCE_UNITTEST_PASS)
		ham_assert(size < (1ULL << 8*sizeof(ham_u16_t)), ("size must fit in the persistent _max_bits type"));
		ham_assert(size*8 < (1ULL << 8*sizeof(ham_u16_t)), ("size must fit in the persistent _max_bits type"));
#endif
        freel_entry_set_max_bits(entry, (ham_u16_t)(size*8));
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
    
            ham_assert(!(address<freel_entry_get_start_address(entry)), 
                            (""));

            if (address>=freel_entry_get_start_address(entry)
                    && address<freel_entry_get_start_address(entry)+
                        freel_entry_get_max_bits(entry)*DB_CHUNKSIZE)
                return (entry);
        }

        /*
         * not found? resize the table
         */
        st=__freel_cache_resize(db, cache, i+8); // i+1 ???
        if (st) {
            db_set_error(db, st);
            return (0);
        }
    } while (i<freel_cache_get_count(cache));

    ham_assert(!"shouldn't be here", (""));
    return (0);
}

static ham_size_t
__freel_set_bits(freelist_payload_t *fp, ham_bool_t overwrite, 
        ham_size_t start_bit, ham_size_t size_bits, ham_bool_t set)
{
    ham_size_t i;
    ham_u8_t *p=freel_get_bitmap16(fp);

#if !defined(FORCE_UNITTEST_PASS) || 01
    ham_assert(start_bit<freel_get_max_bits16(fp), (""));
#endif

    if (start_bit+size_bits>freel_get_max_bits16(fp))
        size_bits=freel_get_max_bits16(fp)-start_bit;

    if (set) {
        for (i=0; i<size_bits; i++, start_bit++) {
			if (!overwrite) {   // [i_a] mightly important to {} brace this as the assert macro 'disappears' in release builds thus corrupting the code.
#if 0
				ham_assert(!(p[start_bit/8] & 1 << (start_bit%8)), 
                    ("bit is already set!"));
#endif
			}
			else
			{
#if 0 // happens when in rollback
				ham_assert(!(p[start_bit/8] & 1 << (start_bit%8)), 
                    ("bit is already set! [OVERWRITE]"));
#endif
			}
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
    ham_size_t bit=0, i, j, max=freel_get_max_bits16(f), 
               max64=(freel_get_max_bits16(f)>>3)>>3;
    ham_u64_t *p64=(ham_u64_t *)freel_get_bitmap16(f);
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
    ham_size_t i=0, j, start, max=freel_get_max_bits16(fp);
    ham_u64_t *p64=(ham_u64_t *)freel_get_bitmap16(fp);
    ham_u8_t *p=(ham_u8_t *)p64;

    /* fix the start position, if the start-address of this page is 
     * not page-aligned */
    if (freel_get_start_address(fp)%db_get_pagesize(db)) {
        ham_offset_t start=((freel_get_start_address(fp)+db_get_pagesize(db))
                /db_get_pagesize(db))*db_get_pagesize(db);
        i=(ham_size_t)((start-freel_get_start_address(fp)/DB_CHUNKSIZE));
        max-=db_get_pagesize(db)/DB_CHUNKSIZE;
    }

    for (; i<max; i+=db_get_pagesize(db)/DB_CHUNKSIZE) {
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
    ham_size_t size = db_get_usable_pagesize(db) - db_get_freelist_header_size16();
	ham_assert((size % sizeof(ham_u64_t)) == 0, ("freelist bitarray size must be == 0 MOD sizeof(ham_u64_t) due to the scan algorithm"));
	size -= size % sizeof(ham_u64_t);

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
                page_set_dirty(page);
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
			ham_assert((size % sizeof(ham_u64_t)) == 0, ("freelist bitarray size must be == 0 MOD sizeof(ham_u64_t) due to the scan algorithm"));
#if !defined(FORCE_UNITTEST_PASS)
			ham_assert(size < (1ULL << 8*sizeof(freel_get_max_bits16(fp))), ("size must fit in the _max_bits type"));
			ham_assert(size*8 < (1ULL << 8*sizeof(freel_get_max_bits16(fp))), ("size must fit in the _max_bits type"));
#endif
			freel_set_max_bits16(fp, (ham_u16_t)(size*8));
            page_set_dirty(page);
            ham_assert(freel_entry_get_max_bits(&entries[i])==
                    freel_get_max_bits16(fp), (""));
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
    ham_size_t i;
    freelist_entry_t *entry = NULL;
    freelist_payload_t *fp = NULL;
    freelist_cache_t *cache=db_get_freelist_cache(db);
    ham_page_t *page=0;
    ham_s32_t s=-1;

    ham_assert(!(db_get_rt_flags(db)&HAM_IN_MEMORY_DB), (0));
    ham_assert(db_get_freelist_cache(db), (0));

    ham_assert(size%DB_CHUNKSIZE==0, (0));

    for (i=0; i<freel_cache_get_count(cache); i++) {
        entry=freel_cache_get_entries(cache)+i;

        /*
         * does this freelist entry have enough allocated blocks to satisfy
         * the request?
         */
        if (freel_entry_get_allocated_bits(entry) >= size/DB_CHUNKSIZE) {
            /*
             * yes, load the payload structure
             */
            if (i==0) {
                fp=db_get_freelist(db);
            }
            else {
                page=db_fetch_page(db, freel_entry_get_page_id(entry), 0);
                if (!page)
                    return (0);
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
                __freel_set_bits(fp, 0, s, size/DB_CHUNKSIZE, HAM_FALSE);
                if (page)
                    page_set_dirty(page);
                else
                    db_set_dirty(db, HAM_TRUE);
                break;
            }
        }
    }

    ham_assert(s != -1 ? fp != NULL : 1, (0));

    if (s!=-1) {
		ham_assert((freel_get_allocated_bits16(fp) - size/DB_CHUNKSIZE) <
					(1ULL << 8*sizeof(freel_get_allocated_bits16(fp))), 
				("size must fit in the _allocated_bits type"));
        freel_set_allocated_bits16(fp, 
                (ham_u16_t)(freel_get_allocated_bits16(fp) - size/DB_CHUNKSIZE));
		// ham_assert(freel_get_allocated_bits16(fp) <
					// (1ULL << 8*sizeof(entry->_allocated_bits)), 
				// ("size must fit in the _allocated_bits type"));
		// ham_assert(freel_get_allocated_bits16(fp) < 
					// (1ULL << 8*sizeof(ham_u16_t)), 
				// ("size must fit in the persistent _allocated_bits type"));
        freel_entry_set_allocated_bits(entry,
                freel_get_allocated_bits16(fp));

        return (freel_get_start_address(fp)+(s*DB_CHUNKSIZE));
    }

    return (0);
}

static ham_status_t
__freel_lazy_create(freelist_cache_t *cache, ham_db_t *db, ham_u16_t mode)
{
    ham_status_t st;
    ham_size_t size, entry_pos=1;
    freelist_entry_t *entry;
    freelist_payload_t *fp=db_get_freelist(db);
    
    ham_assert(db_get_freelist_cache(db) == 0, (""));
    ham_assert(cache != 0, (""));
    ham_assert(!freel_cache_get_entries(cache), (""));

    entry=ham_mem_calloc(db, sizeof(*entry)*8);
    if (!entry)
        return (db_set_error(db, HAM_OUT_OF_MEMORY));

    /*
     * add the header page to the freelist
     */
    freel_entry_set_start_address(&entry[0], db_get_pagesize(db));
#if 0
	size=db_get_usable_pagesize(db)-
        ((char *)db_get_freelist(db)-
            (char *)page_get_payload(db_get_header_page(db)));
#else
	size = db_get_usable_pagesize(db);
	size -= SIZEOF_FULL_HEADER(db);
#endif
    size -= db_get_freelist_header_size16();
	size -= size % sizeof(ham_u64_t);
	ham_assert((size % sizeof(ham_u64_t)) == 0, ("freelist entry bitarray == 0 MOD sizeof(ham_u64_t) due to the scan algorithm"));
	// ham_assert(size < (1ULL << 8*sizeof(entry->_max_bits)), ("size must fit in the _max_bits type"));
	// ham_assert(size*8 < (1ULL << 8*sizeof(entry->_max_bits)), ("size must fit in the _max_bits type"));
#if !defined(FORCE_UNITTEST_PASS)
	ham_assert(size < (1ULL << 8*sizeof(ham_u16_t)), ("size must fit in the persistent _max_bits type"));
	ham_assert(size*8 < (1ULL << 8*sizeof(ham_u16_t)), ("size must fit in the persistent _max_bits type"));
#endif
	freel_entry_set_max_bits(&entry[0], (ham_u16_t)(size*8));

    /*
     * initialize the header page, if we have read/write access
     */
    if (!(db_get_rt_flags(db)&HAM_READ_ONLY)) {
        freel_set_start_address(fp, db_get_pagesize(db));
		ham_assert((size*8 % sizeof(ham_u64_t)) == 0, ("freelist bitarray size must be == 0 MOD sizeof(ham_u64_t) due to the scan algorithm"));
#if !defined(FORCE_UNITTEST_PASS)
		ham_assert(size < (1ULL << 8*sizeof(freel_get_max_bits16(fp))), ("size must fit in the _max_bits type"));
		ham_assert(size*8 < (1ULL << 8*sizeof(freel_get_max_bits16(fp))), ("size must fit in the _max_bits type"));
#endif
		freel_set_max_bits16(fp, (ham_u16_t)(size*8));
    }

    freel_cache_set_count(cache, 1); // 8 ???
    freel_cache_set_entries(cache, entry);

    if (db_get_env(db))
        env_set_freelist_cache(db_get_env(db), cache);
    else
        db_set_freelist_cache(db, cache);

    /*
     * now load all other freelist pages
     */
    while (1) {
        ham_page_t *page;
        if (!freel_get_overflow(fp))
            break;

        st=__freel_cache_resize(db, cache, freel_cache_get_count(cache)+1); // +8 ???
        if (st)
            return (db_set_error(db, st));

        page=db_fetch_page(db, freel_get_overflow(fp), 0);
        if (!page)
            return (db_get_error(db));

        fp=page_get_freelist(page);
        ham_assert(entry_pos<freel_cache_get_count(cache), (0));
        entry=freel_cache_get_entries(cache)+entry_pos;
        ham_assert(freel_entry_get_start_address(entry) == freel_get_start_address(fp), (""));
		// ham_assert(freel_get_allocated_bits16(fp) < (1ULL << 8*sizeof(entry->_allocated_bits)), ("size must fit in the _allocated_bits type"));
		// ham_assert(freel_get_allocated_bits16(fp) < (1ULL << 8*sizeof(ham_u16_t)), ("size must fit in the persistent _allocated_bits type"));
        freel_entry_set_allocated_bits(entry, 
                freel_get_allocated_bits16(fp));
        freel_entry_set_page_id(entry, 
                page_get_self(page));

        entry_pos++;
    }

    return (0);
}

static ham_status_t
__freel_destructor(ham_db_t *db)
{
    freelist_cache_t *cache;

    ham_assert(!(db_get_rt_flags(db)&HAM_IN_MEMORY_DB), (0));

    cache=db_get_freelist_cache(db);
	ham_assert(cache, (0));

    if (freel_cache_get_entries(cache))
        ham_mem_free(db, freel_cache_get_entries(cache));

	return (0);
}


static ham_status_t
__freel_mark_free(ham_db_t *db, ham_offset_t address, ham_size_t size, 
        ham_bool_t overwrite)
{
    ham_page_t *page=0;
    freelist_cache_t *cache;
    freelist_entry_t *entry;
    freelist_payload_t *fp;
    ham_size_t s;

    ham_assert(!(db_get_rt_flags(db)&HAM_IN_MEMORY_DB), (0));

    ham_assert(size%DB_CHUNKSIZE==0, (0));
    ham_assert(address%DB_CHUNKSIZE==0, (0));

	cache = db_get_freelist_cache(db);
    ham_assert(cache, (0));

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
				ham_assert(freel_get_start_address(fp) != 0, (0));
            }
            else {
                page=__freel_alloc_page(db, cache, entry);
                if (!page)
                    return (db_get_error(db));
                fp=page_get_freelist(page);
				ham_assert(freel_get_start_address(fp) != 0, (0));
            }
        }
        /*
         * otherwise just fetch the page from the cache or the disk
         */
        else {
            page=db_fetch_page(db, freel_entry_get_page_id(entry), 0);
            if (!page)
                return (db_get_error(db));
            fp=page_get_freelist(page);
			ham_assert(freel_get_start_address(fp) != 0, (0));
        }

        ham_assert(address>=freel_get_start_address(fp), (0));

        /*
         * set the bits and update the values in the cache and
         * the fp
         */
        s=__freel_set_bits(fp, overwrite,
                (ham_size_t)(address-freel_get_start_address(fp))/
                    DB_CHUNKSIZE, size/DB_CHUNKSIZE, HAM_TRUE);

		// ham_assert(freel_get_allocated_bits16(fp)+s/DB_CHUNKSIZE < (1ULL << 8*sizeof(entry->_allocated_bits)), ("size must fit in the _allocated_bits type"));
		ham_assert(freel_get_allocated_bits16(fp)+s/DB_CHUNKSIZE < (1ULL << 8*sizeof(ham_u16_t)), ("size must fit in the persistent _allocated_bits type"));
        freel_set_allocated_bits16(fp, 
                (ham_u16_t)(freel_get_allocated_bits16(fp)+s/DB_CHUNKSIZE));
        freel_entry_set_allocated_bits(entry, 
                freel_get_allocated_bits16(fp));

        if (page)
            page_set_dirty(page);
        else
            db_set_dirty(db, HAM_TRUE);

        size-=s;
        address+=s;
    }

    return (0);
}

static ham_status_t
__freel_constructor(ham_db_t *db)
{
    ham_status_t st;
    freelist_cache_t *cache;
    
    ham_assert(db_get_freelist_cache(db)==0, (""));

    cache=ham_mem_calloc(db, sizeof(*cache));
    if (!cache)
        return (db_set_error(db, HAM_OUT_OF_MEMORY));

	ham_assert(db_get_header_page(db), (0));
	ham_assert(db_get_header(db), (0));
	ham_assert(db_get_data_access_mode(db) == 0, (0));
	cache->_mgt_mode = db_get_data_access_mode(db); /* HAM_DAM_CLASSIC */

	cache->_constructor = __freel_lazy_create;
	cache->_destructor = __freel_destructor;
	cache->_alloc_area = __freel_alloc_area;
	cache->_mark_free = __freel_mark_free;

	st = cache->_constructor(cache, db, cache->_mgt_mode);
    return (st);
}


/* ------------------------------------------------ */

ham_status_t
freel_shutdown(ham_db_t *db)
{
    freelist_cache_t *cache;
	ham_status_t st;

    if (db_get_rt_flags(db)&HAM_IN_MEMORY_DB)
        return (0);

    cache=db_get_freelist_cache(db);
    if (!cache)
        return (0);

	ham_assert(cache->_destructor, (0));
	st = cache->_destructor(db);

    ham_mem_free(db, cache);
    if (db_get_env(db))
        env_set_freelist_cache(db_get_env(db), 0);
    else
        db_set_freelist_cache(db, 0);

    return (st);
}

ham_status_t
freel_mark_free(ham_db_t *db, ham_offset_t address, ham_size_t size, 
        ham_bool_t overwrite)
{
    freelist_cache_t *cache;
	ham_status_t st;

    if (db_get_rt_flags(db)&HAM_IN_MEMORY_DB)
        return (0);

    ham_assert(size%DB_CHUNKSIZE==0, (0));
    ham_assert(address%DB_CHUNKSIZE==0, (0));

	if (!db_get_freelist_cache(db))
        __freel_constructor(db);
    cache=db_get_freelist_cache(db);

	ham_assert(cache->_mark_free, (0));
	st = cache->_mark_free(db, address, size, overwrite);

    return (st);
}

ham_offset_t
freel_alloc_area(ham_db_t *db, ham_size_t size)
{
    freelist_cache_t *cache;
	ham_offset_t offset;

    if (db_get_rt_flags(db)&HAM_IN_MEMORY_DB)
        return (0);

    if (!db_get_freelist_cache(db))
        __freel_constructor(db);
    cache=db_get_freelist_cache(db);

	ham_assert(cache->_alloc_area, (0));
    offset = cache->_alloc_area(db, size, HAM_FALSE);
	return (offset);
}

ham_offset_t
freel_alloc_page(ham_db_t *db)
{
    freelist_cache_t *cache;
	ham_offset_t offset;

    if (db_get_rt_flags(db)&HAM_IN_MEMORY_DB)
        return (0);

    if (!db_get_freelist_cache(db))
        __freel_constructor(db);
    cache=db_get_freelist_cache(db);

	ham_assert(cache->_alloc_area, (0));
	offset = cache->_alloc_area(db, db_get_pagesize(db), HAM_TRUE);
	return (offset);
}


