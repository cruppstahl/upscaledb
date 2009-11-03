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
  txn_begin(&txn, db, 0);
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
txn_begin(ham_txn_t *txn, ham_db_t *db, ham_u32_t flags)
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
txn_commit(ham_txn_t *txn, ham_u32_t flags)
{
    ham_status_t st;
    ham_db_t *db=txn_get_db(txn);

    /*
     * are cursors attached to this txn? if yes, fail
     */
    if (txn_get_cursor_refcount(txn)) {
        ham_trace(("transaction cannot be committed till all attached "
                    "cursors are closed"));
        return (db_set_error(db, HAM_CURSOR_STILL_OPEN));
    }

    /*
     * in case of logging: write after-images of all modified pages,
     * if they were modified by this transaction;
     * then write the transaction boundary
     */
    if (db_get_log(db) && !(txn_get_flags(txn)&HAM_TXN_READ_ONLY)) 
	{
        ham_page_t *head=txn_get_pagelist(txn);
        while (head) {
			ham_page_t *next;

            next=page_get_next(head, PAGE_LIST_TXN);
            if (page_get_dirty_txn(head)==txn_get_id(txn) 
                    || page_get_dirty_txn(head)==PAGE_DUMMY_TXN_ID) {
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
     *
     * shouldn't use local var for the list head, as
     * txn_get_pagelist(txn) should be kept up to date and correctly
     * formatted while we call db_free_page() et al.
     */
	while (txn_get_pagelist(txn))
	{
		ham_page_t *head = txn_get_pagelist(txn);
		
		txn_get_pagelist(txn) = page_list_remove(head, PAGE_LIST_TXN, head);

        /* page is no longer in use */
        page_release_ref(head);

        /* 
         * delete the page? 
         */
        if (page_get_npers_flags(head)&PAGE_NPERS_DELETE_PENDING) {
            /* remove page from cache, add it to garbage list */
            page_set_undirty(head);
        
            st=db_free_page(head, DB_MOVE_TO_FREELIST);
            if (st)
                return (st);
        }
		else
		{
			/* flush the page */
			st=db_flush_page(db, head, 
					flags & HAM_TXN_FORCE_WRITE ? HAM_WRITE_THROUGH : 0);
			if (st) {
				page_add_ref(head);
				/* failure: re-insert into transaction list! */
				txn_get_pagelist(txn) = page_list_insert(txn_get_pagelist(txn),
                            PAGE_LIST_TXN, head);
				return (st);
			}
		}
    }

    txn_set_pagelist(txn, 0);

    return (0);
}

ham_status_t
txn_abort(ham_txn_t *txn, ham_u32_t flags)
{
    ham_status_t st;
    ham_db_t *db=txn_get_db(txn);

    /*
     * are cursors attached to this txn? if yes, fail
     */
    if (txn_get_cursor_refcount(txn)) {
        ham_trace(("transaction cannot be aborted till all attached "
                    "cursors are closed"));
        return (db_set_error(db, HAM_CURSOR_STILL_OPEN));
    }

    if (db_get_log(db) && !(txn_get_flags(txn)&HAM_TXN_READ_ONLY)) {
        st=ham_log_append_txn_abort(db_get_log(db), txn);
        if (st) 
            return (db_set_error(db, st));
    }

    db_set_txn(db, 0);

    /*
     * delete all modified pages
	 *
	 * keep txn_get_pagelist(txn) intact during every round, so no 
     * local var for this one.
     */
    while (txn_get_pagelist(txn)) 
	{
	    ham_page_t *head = txn_get_pagelist(txn);

		if (!(flags & DO_NOT_NUKE_PAGE_STATS))
		{
			/* 
			 * nuke critical statistics, such as tracked outer bounds; imagine,
             * for example, a failing erase transaction which, through erasing 
             * the top-most key, lowers the actual upper bound, after which 
             * the transaction fails at some later point in life. Now if we 
             * wouldn't 'rewind' our bounds-statistics, we would have a 
             * situation where a subsequent out-of-bounds insert (~ append) 
             * would possibly FAIL due to the hinter using incorrect bounds 
             * information then!
             *
             * Hence we 'reverse' our statistics here and the easiest route 
             * is to just nuke the critical bits; subsequent find/insert/erase 
             * operations will ensure that the stats will get updated again, 
             * anyhow. All we loose then is a few subsequent operations, which 
             * might have been hinted if we had played a smarter game of 
             * statistics 'reversal'. Soit.
			 */
			stats_page_is_nuked(db, head, HAM_FALSE); 
		}

		ham_assert(page_is_in_list(txn_get_pagelist(txn), head, PAGE_LIST_TXN),
                             (0));
		txn_get_pagelist(txn) = page_list_remove(head, PAGE_LIST_TXN, head);

        /* if this page was allocated by this transaction, then we can
         * move the whole page to the freelist */
        if (page_get_alloc_txn_id(head)==txn_get_id(txn)) 
		{
            (void)freel_mark_free(db, page_get_self(head), 
                    db_get_cooked_pagesize(db), HAM_TRUE);
        }
		else
		{
			/* remove the 'delete pending' flag */
			page_set_npers_flags(head, 
					page_get_npers_flags(head)&~PAGE_NPERS_DELETE_PENDING);

            /* if the page is dirty, and RECOVERY is enabled: recreate
             * the original, unmodified page from the log */
			if (db_get_log(db) && page_is_dirty(head)) 
			{
				st=ham_log_recreate(db_get_log(db), head);
				if (st)
					return (st);
				/*page_set_undirty(head); */
			}
		}

        /* page is no longer in use */
        page_release_ref(head);
    }

    ham_assert(txn_get_pagelist(txn)==0, (""));

    return (0);
}

