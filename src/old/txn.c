/**
 * Copyright (C) 2005, 2006 Christoph Rupp (chris@crupp.de)
 * see file COPYING for licence information
 *
 */

#include <string.h>
#include "txn.h"
#include "db.h"
#include "cachemgr.h"
#include "error.h"

ham_page_t *
txn_fetch_page(ham_txn_t *txn, ham_offset_t address, ham_u32_t flags)
{
    ham_page_t *p;
    ham_db_t *db=txn_get_owner(txn);

    /*
     * check the internal page list
     * TODO we don't need an internal list if the txn is readonly!
     */
    p=txn_get_pagelist(txn);
    while (p) {
        if (page_get_self(p)==address)
            return p;
        p=page_get_next(p, PAGE_LIST_TXN);
    }

    /*
     * not found? fetch the page from the cache
     */
    if ((flags&TXN_READ_ONLY) || (txn_get_flags(txn)&TXN_READ_ONLY))
        p=cm_fetch(db->_npers._cm, address, CM_READ_ONLY);
    else
        p=cm_fetch(db->_npers._cm, address, 0);
    if (p) {
        page_ref_inc(p, 0);
        txn_set_pagelist(txn, page_list_insert(txn_get_pagelist(txn), 
                PAGE_LIST_TXN, p));
    }
    return p;
}

ham_page_t *
txn_alloc_page(ham_txn_t *txn, ham_u32_t flags)
{
    ham_page_t *p;
    ham_db_t *db=txn_get_owner(txn);

    /*
     * TODO assert(txn->flags!=READ_ONLY);
     */
    p=cm_alloc_page(db->_npers._cm, txn, flags);
    if (p) {
        page_ref_inc(p, 0);
        txn_set_pagelist(txn, page_list_insert(txn_get_pagelist(txn), 
                PAGE_LIST_TXN, p));
    }
    return p;
}

void
txn_remove_page(ham_txn_t *txn, ham_page_t *page)
{
    ham_page_t *sp;

    page_set_dirty(page, 0);
    page_ref_dec(page, 0);
    ham_assert(page_ref_get(page)==1, "refcount of page 0x%llx is %d", 
            page_get_self(page), page_ref_get(page));

    sp=page_get_shadowpage(page);
    if (sp)
        page_set_shadowpage(sp, 0);
    page_set_shadowpage(page, 0);

    txn_set_pagelist(txn, 
            page_list_remove(txn_get_pagelist(txn), 
            PAGE_LIST_TXN, page));
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
    ham_status_t st;
    ham_page_t *head, *next;
    ham_db_t *db=txn_get_owner(txn);

    (void)flags;

    /*
     * flush the pages
     */
    head=txn_get_pagelist(txn);
    while (head) {
        page_ref_dec(head, 0);
        next=page_get_next(head, PAGE_LIST_TXN);
        st=cm_flush(db->_npers._cm, head, 0);
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
        page_ref_dec(head, 0);
        next=page_get_next(head, PAGE_LIST_TXN);
        (void)cm_flush(db->_npers._cm, head, HAM_CM_REVERT_CHANGES);
        page_set_next(head, PAGE_LIST_TXN, 0);
        page_set_previous(head, PAGE_LIST_TXN, 0);
        head=next;
    }

    txn_set_pagelist(txn, 0);

    return (0);
}
