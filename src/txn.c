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
#include "txn.h"
#include "db.h"
#include "error.h"
#include "freelist.h"
#include "mem.h"
#include "log.h"

ham_status_t
txn_add_page(ham_txn_t *txn, ham_page_t *page, ham_bool_t ignore_if_inserted)
{
    /*
     * don't re-insert, if 'ignore_if_inserted' is true
     */
    if (ignore_if_inserted && txn_get_page(txn, page_get_self(page)))
        return (0);

#ifdef HAM_DEBUG
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
    ham_assert(page_get_cursors(page)==0, (0));

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
    ham_page_t *p=txn_get_pagelist(txn);

#ifdef HAM_DEBUG
    ham_page_t *start=p;
#endif

    while (p) {
        ham_offset_t o=page_get_self(p);
        if (o==address)
            return (p);
        p=page_get_next(p, PAGE_LIST_TXN);
        ham_assert(start!=p, ("circular reference in page-list"));
    }

    return (0);
}

ham_status_t
ham_txn_begin(ham_txn_t *txn, ham_db_t *db, ham_u32_t flags)
{
    ham_status_t st=0;

    memset(txn, 0, sizeof(*txn));
    txn_set_db(txn, db);
    db_set_txn(db, txn);
    txn_set_id(txn, db_get_txn_id(db)+1);
    txn_set_flags(txn, flags);
    db_set_txn_id(db, txn_get_id(txn));

    if (db_get_log(db) && !(flags&HAM_TXN_READ_ONLY))
        st=ham_log_append_txn_begin(db_get_log(db), txn);

    return (db_set_error(db, st));
}

ham_status_t
ham_txn_commit(ham_txn_t *txn, ham_u32_t flags)
{
    ham_status_t st;
    ham_page_t *head, *next;
    ham_db_t *db=txn_get_db(txn);

    /*
     * in case of logging: write after-images of all modified pages,
     * then write the transaction boundary
     */
    if (db_get_log(db) && !(txn_get_flags(txn)&HAM_TXN_READ_ONLY)) {
        head=txn_get_pagelist(txn);
        while (head) {
            next=page_get_next(head, PAGE_LIST_TXN);
            if (page_is_dirty(head)) {
                st=ham_log_add_page_after(head);
                if (st) 
                    return (db_set_error(db, st));
            }
            head=next;
        }

        st=ham_log_append_txn_commit(db_get_log(db), txn);
        if (st) 
            return (db_set_error(db, st));
    }

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
         */
        if (page_get_npers_flags(head)&PAGE_NPERS_DELETE_PENDING) {
            /* remove page from cache, add it to garbage list */
            page_set_dirty(head, 0);
        
            st=db_free_page(head, DB_MOVE_TO_FREELIST);
            if (st)
                return (st);

            goto commit_next;
        }

        /* flush the page */
        st=db_flush_page(db, head, 
                flags&TXN_FORCE_WRITE ? HAM_WRITE_THROUGH : 0);
        if (st) {
            page_add_ref(head);
            txn_set_pagelist(txn, head);
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
    ham_status_t st;
    ham_page_t *head, *next;
    ham_db_t *db=txn_get_db(txn);

    if (db_get_log(db) && !(txn_get_flags(txn)&HAM_TXN_READ_ONLY)) {
        st=ham_log_append_txn_abort(db_get_log(db), txn);
        if (st) 
            return (db_set_error(db, st));
    }

    db_set_txn(db, 0);

    /*
     * delete all modified pages
     */
    head=txn_get_pagelist(txn);
    while (head) {
        next=page_get_next(head, PAGE_LIST_TXN);
        page_set_next(head, PAGE_LIST_TXN, 0);
        page_set_previous(head, PAGE_LIST_TXN, 0);

        /* remove the 'delete pending' flag */
        if (page_get_npers_flags(head)&PAGE_NPERS_DELETE_PENDING)
            page_set_npers_flags(head, 
                    page_get_npers_flags(head)&~PAGE_NPERS_DELETE_PENDING);

        /* page is no longer in use */
        page_release_ref(head);

        head=next;
    }

    txn_set_pagelist(txn, 0);

    return (0);
}

