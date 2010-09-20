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

#include "db.h"
#include "env.h"
#include "error.h"
#include "freelist.h"
#include "log.h"
#include "mem.h"
#include "page.h"
#include "statistics.h"
#include "txn.h"

/* stuff for rb.h */
typedef signed ssize_t;
typedef int bool;
#define true 1
#define false (!true)

static int
__cmpfoo(void *vlhs, void *vrhs)
{
    ham_compare_func_t foo;
    txn_node_t *ln, *rn;
    txn_optree_node_t *lhs=(txn_optree_node_t *)vlhs;
    txn_optree_node_t *rhs=(txn_optree_node_t *)vrhs;
#if 0
    ln=txn_optree_node_get_node(lhs);
    rn=txn_optree_node_get_node(rhs);

    ham_assert(txn_op_get_db(oplhs)==txn_op_get_db(oprhs));
    foo=db_get_compfunc(txn_op_get_db(oplhs));

    return (foo(0, /* TODO first parameter - gdb might already
                        be freed!! */
                txn_op_get_key(oplhs)->data,
                txn_op_get_key(oplhs)->size,
                txn_op_get_key(oprhs)->data,
                txn_op_get_key(oprhs)->size));
#endif
return (0);
}

rb_wrap(static, rbt_, txn_optree_t, txn_optree_node_t, node, __cmpfoo)

ham_status_t
txn_add_page(ham_txn_t *txn, ham_page_t *page, ham_bool_t ignore_if_inserted)
{
    /*
     * don't re-insert, if 'ignore_if_inserted' is true
     */
    if (ignore_if_inserted && txn_get_page(txn, page_get_self(page)))
        return (0);

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
    ham_assert(page_is_in_list(txn_get_pagelist(txn), page, PAGE_LIST_TXN), (0));
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
txn_begin(ham_txn_t **ptxn, ham_env_t *env, ham_u32_t flags)
{
    ham_status_t st=0;
    ham_txn_t *txn;

    txn=(ham_txn_t *)allocator_alloc(env_get_allocator(env), sizeof(ham_txn_t));
    if (!txn)
        return (HAM_OUT_OF_MEMORY);

    memset(txn, 0, sizeof(*txn));
    txn_set_id(txn, env_get_txn_id(env)+1);
    txn_set_flags(txn, flags);
    env_set_txn_id(env, txn_get_id(txn));

    if (env_get_log(env) && !(flags&HAM_TXN_READ_ONLY))
        st=ham_log_append_txn_begin(env_get_log(env), txn);

    /* link this txn with the Environment */
    env_append_txn(env, txn);

    *ptxn=txn;

    return (st);
}

ham_status_t
txn_commit(ham_txn_t *txn, ham_u32_t flags)
{
    ham_env_t *env=txn_get_env(txn);

    /*
     * are cursors attached to this txn? if yes, fail
     */
    if (txn_get_cursor_refcount(txn)) {
        ham_trace(("Transaction cannot be committed till all attached "
                    "Cursors are closed"));
        return (HAM_CURSOR_STILL_OPEN);
    }

    /*
     * this transaction is now committed!
     */
    txn_set_flags(txn, txn_get_flags(txn)|TXN_STATE_COMMITTED);

#if 0
    /* TODO write log! */

    /* decrease the reference counter of the modified databases */
    __decrease_db_refcount(txn);
#endif

    /* now flush all committed Transactions to disk */
    return (env_flush_committed_txns(env));
}

ham_status_t
txn_abort(ham_txn_t *txn, ham_u32_t flags)
{
    /*
     * are cursors attached to this txn? if yes, fail
     */
    if (txn_get_cursor_refcount(txn)) {
        ham_trace(("Transaction cannot be aborted till all attached "
                    "Cursors are closed"));
        return (HAM_CURSOR_STILL_OPEN);
    }

    /*
     * this transaction is now aborted!
     */
    txn_set_flags(txn, txn_get_flags(txn)|TXN_STATE_ABORTED);

#if 0
    TODO
    if (env_get_log(env) && !(txn_get_flags(txn)&HAM_TXN_READ_ONLY)) {
        st=ham_log_append_txn_abort(env_get_log(env), txn);
        if (st) 
            return st;
    }
#endif

#if 0
    /* decrease the reference counter of the modified databases */
    __decrease_db_refcount(txn);

    /* immediately release memory of the cached operations */
    txn_free_ops(txn);
#endif

    return (0);
}

void
txn_free(ham_txn_t *txn)
{
    ham_env_t *env=txn_get_env(txn);

#if DEBUG
    memset(txn, 0, sizeof(*txn));
#endif

    allocator_free(env_get_allocator(env), txn);
}

