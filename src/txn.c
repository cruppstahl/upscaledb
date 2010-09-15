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

    ham_assert(!page_is_in_list(txn_get_pagelist(txn), page, PAGE_LIST_TXN), (0));
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

/*
BIG FAT WARNING:

This routine should NEVER be used like this:

  ham_txn_t txn;
  txn_begin(&txn, env, 0);
  ...
  txn_commit/abort(&txn);

in any (C/C++) environment where the code in the '...' may trigger out of band jumps, such as longjmp()
to an outer layer or a C++ exception, as the transaction 'txn' will be bound to the 'db' structure
internally and cause a CORE DUMP once the 'db' structure is closed (and cleaned up) as then, in the
outer layer exception handler, the 'txn' stack space will have been NUKED.

This shortcutting style of coding was used throughout the unittests and it was waiting for the axe to fall...

It is also used within the hamsterdb C code itself, which is perfectly fine as this library does not
call any exception throwing code... UNLESS OF COURSE such sort of code is to be found in ANY of the
registered hooks/callbacks!

Hence any callbacks which get registered with hamsterDB should NEVER allow any C longjmp() or C++ exception
to pass /through/ the hamsterdb layer itself, or a core dump at ham_close/ham_env_close invocation
will be your share.
*/

ham_status_t
txn_begin(ham_txn_t *txn, ham_env_t *env, ham_u32_t flags)
{
    ham_status_t st=0;

    memset(txn, 0, sizeof(*txn));
    txn_set_id(txn, env_get_txn_id(env)+1);
    txn_set_flags(txn, flags);
    env_set_txn_id(env, txn_get_id(txn));

    if (env_get_log(env) && !(flags&HAM_TXN_READ_ONLY))
        st=ham_log_append_txn_begin(env_get_log(env), txn);

    return st;
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

    allocator_free(env_get_allocator(env), txn);
}

