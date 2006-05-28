/**
 * Copyright (C) 2005, 2006 Christoph Rupp (chris@crupp.de)
 * see file COPYING for licence information
 *
 */

#include <string.h>
#include "txn.h"
#include "db.h"
#include "error.h"

ham_status_t
txn_add_page(ham_txn_t *txn, ham_page_t *page)
{
    ham_page_t *p;

    /*
     * check the internal page list
     * TODO we don't need an internal list if the txn is readonly!
     */
#if HAM_DEBUG
    p=txn_get_pagelist(txn);
    while (p) {
        ham_assert(page_get_self(p)!=page_get_self(page), 
                "page 0x%llx is already in the txn", page_get_self(p));
        p=page_get_next(p, PAGE_LIST_TXN);
    }
#endif

    /*
     * not found? add the page
     */
    page_ref_inc(page);
    ham_assert(page_ref_get(page)==1, "refcount of page 0x%llx is %d", 
            page_get_self(page), page_ref_get(page));
    txn_set_pagelist(txn, page_list_insert(txn_get_pagelist(txn), 
            PAGE_LIST_TXN, page));
    return (0);
}

ham_page_t *
txn_get_page(ham_txn_t *txn, ham_offset_t offset)
{
    ham_page_t *page=txn_get_pagelist(txn);
    while (page) {
        if (page_get_self(page)==offset)
            return (page);
        page=page_get_next(page, PAGE_LIST_TXN);
    }
    return (0);
}

ham_status_t
txn_remove_page(ham_txn_t *txn, ham_page_t *page)
{
    ham_page_t *sp;

    page_set_dirty(page, 0);
    page_ref_dec(page);
    ham_assert(page_ref_get(page)==0, "refcount of page 0x%llx is %d", 
            page_get_self(page), page_ref_get(page));

    sp=page_get_shadowpage(page);
    if (sp)
        page_set_shadowpage(sp, 0);
    page_set_shadowpage(page, 0);

    txn_set_pagelist(txn, 
            page_list_remove(txn_get_pagelist(txn), 
            PAGE_LIST_TXN, page));
    return (0);
}

ham_status_t
ham_txn_begin(ham_txn_t *txn, ham_db_t *db, ham_u32_t flags)
{
    memset(txn, 0, sizeof(*txn));
    txn_set_owner(txn, db);
    txn_set_flags(txn, flags);
    return (0);
}

ham_status_t
ham_txn_commit(ham_txn_t *txn, ham_u32_t flags)
{
    ham_status_t st=0;
    ham_page_t *head, *next;
    ham_db_t *db=txn_get_owner(txn);

    (void)flags;

    /*
     * flush the pages
     */
    head=txn_get_pagelist(txn);
    while (head) {
        page_ref_dec(head);
        next=page_get_next(head, PAGE_LIST_TXN);
        if (page_get_npers_flags(head)&PAGE_NPERS_LOCKED) 
            st=db_unlock_page(db, txn, head, 0);
        if (!st)
            st=db_flush_page(db, 0, head, 0);
        if (st) {
            ham_trace("commit failed with status 0x%x", st);
            txn_set_pagelist(txn, head);
            (void)ham_txn_abort(txn, flags);
            return (st);
        }
        page_set_next(head, PAGE_LIST_TXN, 0);
        page_set_previous(head, PAGE_LIST_TXN, 0);
        head=next;
    }

    txn_set_pagelist(txn, 0);

    return (0);
}

ham_status_t
ham_txn_abort(ham_txn_t *txn, ham_u32_t flags)
{
    ham_page_t *head, *next;
    ham_db_t *db=txn_get_owner(txn);

    (void)flags;

    /*
     * flush the pages
     */
    head=txn_get_pagelist(txn);
    while (head) {
        page_ref_dec(head);
        next=page_get_next(head, PAGE_LIST_TXN);
        if (page_get_npers_flags(head)&PAGE_NPERS_LOCKED) 
            (void)db_unlock_page(db, txn, head, 0);
        (void)db_flush_page(db, 0, head, DB_REVERT_CHANGES);
        page_set_next(head, PAGE_LIST_TXN, 0);
        page_set_previous(head, PAGE_LIST_TXN, 0);
        head=next;
    }

    txn_set_pagelist(txn, 0);

    return (0);
}
