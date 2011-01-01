/*
 * Copyright (C) 2005-2010 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

#include "internal_fwd_decl.h"
#include "txn_cursor.h"
#include "db.h"
#include "env.h"
#include "mem.h"

ham_bool_t
txn_cursor_is_nil(txn_cursor_t *cursor)
{
    if (txn_cursor_get_flags(cursor)&TXN_CURSOR_FLAG_COUPLED)
        return (HAM_FALSE);
    if (txn_cursor_get_flags(cursor)&TXN_CURSOR_FLAG_UNCOUPLED)
        return (HAM_FALSE);
    return (HAM_TRUE);
}

void
txn_cursor_set_to_nil(txn_cursor_t *cursor)
{
    ham_env_t *env=db_get_env(txn_cursor_get_db(cursor));

    /* uncoupled cursor? free the cached pointer */
    if (txn_cursor_get_flags(cursor)&TXN_CURSOR_FLAG_UNCOUPLED) {
        ham_key_t *key=txn_cursor_get_uncoupled_key(cursor);
        if (key) {
            if (key->data)
                allocator_free(env_get_allocator(env), key->data);
            allocator_free(env_get_allocator(env), key);
        }
        txn_cursor_set_uncoupled_key(cursor, 0);
        txn_cursor_set_flags(cursor, 
                txn_cursor_get_flags(cursor)&(~TXN_CURSOR_FLAG_UNCOUPLED));
    }
    /* uncoupled cursor? remove from the txn_op structure */
    else if (txn_cursor_get_flags(cursor)&TXN_CURSOR_FLAG_COUPLED) {
        /* TODO */
        txn_cursor_set_flags(cursor, 
                txn_cursor_get_flags(cursor)&(~TXN_CURSOR_FLAG_COUPLED));
    }

    /* otherwise cursor is already nil */
}

txn_cursor_t *
txn_cursor_clone(txn_cursor_t *cursor)
{
    return (0);
}

void
txn_cursor_close(txn_cursor_t *cursor)
{
}

void
txn_cursor_overwrite(txn_cursor_t *cursor, ham_record_t *record)
{
}

ham_status_t
txn_cursor_move(txn_cursor_t *cursor, ham_u32_t flags)
{
    return (0);
}

ham_status_t
txn_cursor_find(txn_cursor_t *cursor, ham_key_t *key)
{
    return (0);
}

ham_status_t
txn_cursor_insert(txn_cursor_t *cursor, ham_key_t *key, ham_record_t *record,
                ham_u32_t flags)
{
    return (0);
}

ham_status_t
txn_cursor_get_key(txn_cursor_t *cursor, ham_key_t *key)
{
    return (0);
}

ham_status_t
txn_cursor_get_record(txn_cursor_t *cursor, ham_record_t *record)
{
    return (0);
}

ham_status_t
txn_cursor_erase(txn_cursor_t *cursor, ham_key_t *key)
{
    return (0);
}

ham_status_t
txn_cursor_get_duplicate_count(txn_cursor_t *cursor, ham_u32_t *count)
{
    return (0);
}
