/**
 * Copyright (C) 2005-2007 Christoph Rupp (chris@crupp.de).
 * All rights reserved. See file LICENSE for licence and copyright
 * information.
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
#endif

    /*
     * not found? add the page
     */
    page_add_ref(page);

    txn_set_pagelist(txn, page_list_insert(txn_get_pagelist(txn), 
            PAGE_LIST_TXN, page));

    return (HAM_SUCCESS);
}

ham_status_t
txn_free_page(ham_txn_t *txn, ham_page_t *page)
{
    ham_assert(!(page_get_npers_flags(page)&PAGE_NPERS_DELETE_PENDING), (0));

    page_set_npers_flags(page,
            page_get_npers_flags(page)|PAGE_NPERS_DELETE_PENDING);

    return (HAM_SUCCESS);
}

ham_status_t
txn_remove_page(ham_txn_t *txn, struct ham_page_t *page)
{
    txn_set_pagelist(txn, page_list_remove(txn_get_pagelist(txn), 
            PAGE_LIST_TXN, page));

    page_release_ref(page);

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
        next=page_get_next(head, PAGE_LIST_TXN);
        page_set_next(head, PAGE_LIST_TXN, 0);
        page_set_previous(head, PAGE_LIST_TXN, 0);

        /* page is no longer in use */
        page_release_ref(head);

        /* 
         * delete the page? 
         *
         * in-memory-databases don't use a freelist and therefore
         * can delete the page without consequences
         */
        if (page_get_npers_flags(head)&PAGE_NPERS_DELETE_PENDING) {
            /* remove page from cache, add it to garbage list */
            page_set_dirty(head, 0);
        
            st=db_free_page(head);
            if (st)
                return (st);

            goto commit_next;
        }

        /* flush the page */
        st=db_flush_page(db, head, 0);
        if (st) {
            ham_trace(("commit failed with status 0x%x", st));
            txn_set_pagelist(txn, head);
            (void)ham_txn_abort(txn);
            /* errors here are fatal... */
            return (st);
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
        next=page_get_next(head, PAGE_LIST_TXN);
        page_set_next(head, PAGE_LIST_TXN, 0);
        page_set_previous(head, PAGE_LIST_TXN, 0);

        /* page is no longer in use */
        page_release_ref(head);

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

