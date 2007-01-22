/**
 * Copyright (C) 2005, 2006 Christoph Rupp (chris@crupp.de)
 * see file LICENSE for licence information
 *
 */

#include <string.h>
#include "txn.h"
#include "db.h"
#include "error.h"
#include "freelist.h"
#include "mem.h"

ham_status_t
txn_add_page(ham_txn_t *txn, ham_page_t *page)
{
#ifndef HAM_RELEASE
    /*
     * check if the page is already in the transaction's pagelist - 
     * that would be a bug
     */
    ham_assert(txn_get_page(txn, page_get_self(page))==0, 
            ("page 0x%llx is already in the txn", page_get_self(page)));
    /* don't check the inuse-flag - with the new cursors, more than one
     * transaction can be open 
     ham_assert(page_get_inuse(page)==0, 
            ("page 0x%llx is already in use", page_get_self(page)));
            */
#endif

    /*
     * not found? add the page
     */
    txn_set_pagelist(txn, page_list_insert(txn_get_pagelist(txn), 
            PAGE_LIST_TXN, page));

    page_inc_inuse(page);

    return (0);
}

ham_status_t
txn_remove_page(ham_txn_t *txn, struct ham_page_t *page)
{
    page_dec_inuse(page);

    txn_set_pagelist(txn, page_list_remove(txn_get_pagelist(txn), 
            PAGE_LIST_TXN, page));

    return (0);
}

ham_page_t *
txn_get_page(ham_txn_t *txn, ham_offset_t address)
{
    ham_page_t *start, *p=txn_get_pagelist(txn);

    start=p;

    while (p) {
        if (page_get_self(p)==address)
            return (p);
        p=page_get_next(p, PAGE_LIST_TXN);
        ham_assert(start!=p, ("circular reference in page-list"));
        if (start==p)
            break;
    }

    return (0);
}

ham_status_t
ham_txn_begin(ham_txn_t *txn, ham_db_t *db)
{
    memset(txn, 0, sizeof(*txn));
    txn_set_db(txn, db);
    db_set_txn(db, txn);
    return (0);
}

ham_status_t
ham_txn_commit(ham_txn_t *txn)
{
    ham_status_t st;
    ham_page_t *head, *next;
    ham_db_t *db=txn_get_db(txn);

    db_set_txn(db, 0);

    /*
     * flush the pages
     */
    head=txn_get_pagelist(txn);
    while (head) {
        /* page is no longer in use */
        page_dec_inuse(head);

        next=page_get_next(head, PAGE_LIST_TXN);
        page_set_next(head, PAGE_LIST_TXN, 0);
        page_set_previous(head, PAGE_LIST_TXN, 0);

        /* 
         * delete the page? 
         *
         * in-memory-databases don't use a freelist and therefore
         * can delete the page without consequences
         */
        if (page_get_npers_flags(head)&PAGE_NPERS_DELETE_PENDING) {
            if (db_get_flags(db)&HAM_IN_MEMORY_DB) { 
                db_free_page_struct(head);
            }
            else {
                /* remove page from cache, add it to garbage list */
                page_set_dirty(head, 0);

                /* add the page to the freelist */
                st=freel_add_area(db, page_get_self(head), 
                    db_get_usable_pagesize(db));
                if (st) {
                    ham_trace(("freel_add_page failed with status 0x%x", st));
                    st=0;
                }

                st=cache_move_to_garbage(db_get_cache(db), head);
                if (st) {
                    ham_trace(("cache_move_to_garbage failed with status 0x%x", 
                            st));
                    st=0;
                }
            }

            goto commit_next;
        }

        /* flush the page */
        st=db_flush_page(db, head, 0);
        if (st) {
            ham_trace(("commit failed with status 0x%x", st));
            txn_set_pagelist(txn, head);
            (void)ham_txn_abort(txn);
            return (st); /* TODO oder return 0? */
        }

commit_next:

        head=next;
    }

    txn_set_pagelist(txn, 0);

    return (0);
}

ham_status_t
ham_txn_abort(ham_txn_t *txn)
{
    ham_page_t *head, *next;

    db_set_txn(txn_get_db(txn), 0);

    /*
     * delete all modified pages
     */
    head=txn_get_pagelist(txn);
    while (head) {
        /* page is no longer in use */
        page_dec_inuse(head);

        next=page_get_next(head, PAGE_LIST_TXN);
        page_set_next(head, PAGE_LIST_TXN, 0);
        page_set_previous(head, PAGE_LIST_TXN, 0);

#if 0
        page_set_dirty(head, 0);

        /* delete the page? */
        if (page_get_npers_flags(head)&PAGE_NPERS_DELETE_PENDING) {
            /* remove the flag */
            page_set_npers_flags(head, 
                    page_get_npers_flags(head)&(~PAGE_NPERS_DELETE_PENDING));
        }

        (void)cache_move_to_garbage(db_get_cache(db), 0, head);
#endif

        head=next;
    }

    txn_set_pagelist(txn, 0);

    return (0);
}

