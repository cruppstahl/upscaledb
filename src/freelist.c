/**
 * Copyright (C) 2005-2007 Christoph Rupp (chris@crupp.de).
 * All rights reserved. See file LICENSE for licence and copyright
 * information.
 *
 */

#include <string.h>
#include <ham/hamsterdb.h>
#include "db.h"
#include "endian.h"
#include "freelist.h"
#include "error.h"

static void
__freel_set_bits(freelist_t *fl, ham_size_t start_bit, ham_size_t size_bits, 
        ham_bool_t set)
{
    ham_size_t i;
    ham_u8_t *p=freel_get_bitmap(fl);

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
}

static ham_s32_t
__freel_search_bits(freelist_t *f, ham_size_t size_bits)
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
__freel_search_aligned_bits(ham_db_t *db, freelist_t *fl, ham_size_t size_bits)
{
    ham_size_t i=0, j, start, max=freel_get_max_bits(fl);
    ham_u64_t *p64=(ham_u64_t *)freel_get_bitmap(fl);
    ham_u8_t *p=(ham_u8_t *)p64;

    /* fix the start position, if the start-address of this page is 
     * not page-aligned */
    if (freel_get_start_address(fl)%db_get_pagesize(db)) {
        ham_offset_t start=((freel_get_start_address(fl)+db_get_pagesize(db))
                /db_get_pagesize(db))*db_get_pagesize(db);
        i=(ham_size_t)((start-freel_get_start_address(fl)/DB_CHUNKSIZE));
        max-=db_get_pagesize(db)/DB_CHUNKSIZE;
    }

    /* TODO this does not yet check for spaces which span several pages */
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
__freel_alloc_page(ham_db_t *db, ham_offset_t start_address)
{
    ham_page_t *page;
    ham_status_t st;

    page=db_alloc_page(db, PAGE_TYPE_FREELIST, 
            PAGE_IGNORE_FREELIST|PAGE_CLEAR_WITH_ZERO);
    if (!page)
        return (0);

    st=freel_prepare(db, page_get_freelist(page), start_address,
            db_get_usable_pagesize(db));
    if (st) {
        db_set_error(db, st);
        return (0);
    }

    page_set_dirty(page, HAM_TRUE);

    return (page);
}

ham_status_t
freel_create(ham_db_t *db)
{
    (void)db;
    return (0);
}

ham_status_t
freel_shutdown(ham_db_t *db)
{
    (void)db;
    return (0);
}

ham_status_t
freel_prepare(ham_db_t *db, freelist_t *fl, ham_offset_t start_address, 
        ham_size_t size)
{
    (void)db;

    memset(fl, 0, size);

    size-=sizeof(*fl)+1; /* +1 for bitmap[1] */

    freel_set_start_address(fl, start_address);
    freel_set_max_bits(fl, size*8);

    return (0);
}

ham_status_t
freel_mark_free(ham_db_t *db, ham_offset_t address, ham_size_t size)
{
    freelist_t *fl;
    ham_offset_t end;
    ham_page_t *page=0;
    ham_txn_t txn, *old_txn=db_get_txn(db);
    ham_status_t st;

    ham_assert(size%DB_CHUNKSIZE==0, (0));
    ham_assert(address%DB_CHUNKSIZE==0, (0));

    if ((st=ham_txn_begin(&txn, db)))
        return (db_set_error(db, st));

    fl=db_get_freelist(db);
    ham_assert(address>=freel_get_start_address(fl), (0));

    end=freel_get_start_address(fl)+freel_get_max_bits(fl)*DB_CHUNKSIZE;

    while (1) {
        if (address<end) {
            if (address+size<end) {
                freel_set_used_bits(fl, 
                        freel_get_used_bits(fl)+size/DB_CHUNKSIZE);
                __freel_set_bits(fl, 
                        (ham_size_t)(address-freel_get_start_address(fl))/
                            DB_CHUNKSIZE, size/DB_CHUNKSIZE, HAM_TRUE);

                if (page)
                    page_set_dirty(page, HAM_TRUE);
                else
                    db_set_dirty(db, HAM_TRUE);
                break;
            }
            else {
                ham_size_t s=(ham_size_t)(end-address+1);
                freel_set_used_bits(fl, 
                        freel_get_used_bits(fl)+s/DB_CHUNKSIZE);
                __freel_set_bits(fl, 
                        (ham_size_t)(address-freel_get_start_address(fl))/
                            DB_CHUNKSIZE, s/DB_CHUNKSIZE, HAM_TRUE);
                address+=s;
                size-=s;
                if (page)
                    page_set_dirty(page, HAM_TRUE);
                else
                    db_set_dirty(db, HAM_TRUE);
                /* fall through */
            }
        }

        if (!freel_get_overflow(fl)) {
            if (!page)
                db_set_dirty(db, HAM_TRUE);
            page=__freel_alloc_page(db, end);
            if (!page) {
                (void)ham_txn_abort(&txn);
                db_set_txn(db, old_txn);
                return (db_get_error(db));
            }

            freel_set_overflow(fl, page_get_self(page));
            fl=page_get_freelist(page);
            ham_assert(freel_get_overflow(fl)!=page_get_self(page), (""));
        }
        else {
            page=db_fetch_page(db, freel_get_overflow(fl), 0);
            if (!page) {
                (void)ham_txn_abort(&txn);
                db_set_txn(db, old_txn);
                return (db_get_error(db));
            }
            fl=page_get_freelist(page);
        }
        end=freel_get_start_address(fl)+freel_get_max_bits(fl)*DB_CHUNKSIZE;
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
    ham_s32_t s;
    freelist_t *fl;
    ham_page_t *page=0;
    ham_txn_t txn, *old_txn=db_get_txn(db);
    ham_status_t st;

    ham_assert(size%DB_CHUNKSIZE==0, (0));

    if ((st=ham_txn_begin(&txn, db)))
        return (db_set_error(db, st));

    fl=db_get_freelist(db);

    while (1) {
        if (freel_get_used_bits(fl)>=size/DB_CHUNKSIZE) {
            s=__freel_search_bits(fl, size/DB_CHUNKSIZE);
            if (s!=-1) {
                __freel_set_bits(fl, s, size/DB_CHUNKSIZE, HAM_FALSE);
                if (page)
                    page_set_dirty(page, HAM_TRUE);
                else
                    db_set_dirty(db, HAM_TRUE);
                break;
            }
        }

        if (!freel_get_overflow(fl)) {
            (void)ham_txn_abort(&txn);
            db_set_txn(db, old_txn);
            return (0);
        }

        page=db_fetch_page(db, freel_get_overflow(fl), 0);
        if (!page) {
            (void)ham_txn_abort(&txn);
            db_set_txn(db, old_txn);
            return (0);
        }

        fl=page_get_freelist(page);
    }

    freel_set_used_bits(fl, freel_get_used_bits(fl)-size/DB_CHUNKSIZE);

    st=ham_txn_commit(&txn, 
            db_get_rt_flags(db)&HAM_DISABLE_FREELIST_FLUSH 
                ? 0 
                : TXN_FORCE_WRITE);
    db_set_txn(db, old_txn);
    if (st) {
        db_set_error(db, st);
        return (0);
    }

    return (freel_get_start_address(fl)+(s*DB_CHUNKSIZE));
}

ham_offset_t
freel_alloc_page(ham_db_t *db)
{
    ham_s32_t s;
    freelist_t *fl;
    ham_page_t *page=0;
    ham_size_t size=db_get_pagesize(db);
    ham_txn_t txn, *old_txn=db_get_txn(db);
    ham_status_t st;

    if ((st=ham_txn_begin(&txn, db)))
        return (db_set_error(db, st));

    fl=db_get_freelist(db);

    while (1) {
        if (freel_get_used_bits(fl)>=size/DB_CHUNKSIZE) {
            s=__freel_search_aligned_bits(db, fl, size/DB_CHUNKSIZE);
            if (s!=-1) {
                __freel_set_bits(fl, s, size/DB_CHUNKSIZE, HAM_FALSE);
                if (page)
                    page_set_dirty(page, HAM_TRUE);
                else
                    db_set_dirty(db, HAM_TRUE);
                break;
            }
        }

        if (!freel_get_overflow(fl)) {
            (void)ham_txn_abort(&txn);
            db_set_txn(db, old_txn);
            return (0);
        }

        page=db_fetch_page(db, freel_get_overflow(fl), 0);
        if (!page) {
            (void)ham_txn_abort(&txn);
            db_set_txn(db, old_txn);
            return (0);
        }

        fl=page_get_freelist(page);
    }

    freel_set_used_bits(fl, freel_get_used_bits(fl)-size/DB_CHUNKSIZE);

    st=ham_txn_commit(&txn, 
            db_get_rt_flags(db)&HAM_DISABLE_FREELIST_FLUSH 
                ? 0 
                : TXN_FORCE_WRITE);
    db_set_txn(db, old_txn);
    if (st) {
        db_set_error(db, st);
        return (0);
    }

    return (freel_get_start_address(fl)+(s*DB_CHUNKSIZE));
}

