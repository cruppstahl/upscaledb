/**
 * Copyright (C) 2005-2007 Christoph Rupp (chris@crupp.de).
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
#include <ham/hamsterdb.h>
#include "db.h"
#include "keys.h"
#include "btree.h"
#include "error.h"
#include "blob.h"

int
key_compare_int_to_pub(ham_page_t *page, ham_u16_t lhs, ham_key_t *rhs)
{
    int_key_t *l;
    btree_node_t *node=ham_page_get_btree_node(page);

    l=btree_node_get_key(page_get_owner(page), node, lhs);

    return (db_compare_keys(page_get_owner(page), page, 
                lhs, key_get_flags(l), key_get_key(l), 
                key_get_size(l), 0, rhs->_flags, rhs->data, rhs->size));
}

int
key_compare_pub_to_int(ham_page_t *page, ham_key_t *lhs, ham_u16_t rhs)
{
    int_key_t *r;
    btree_node_t *node=ham_page_get_btree_node(page);

    r=btree_node_get_key(page_get_owner(page), node, rhs);

    return (db_compare_keys(page_get_owner(page), page, 
                0, lhs->_flags, lhs->data, lhs->size, 
                rhs, key_get_flags(r), key_get_key(r), key_get_size(r)));
}

int
key_compare_int_to_int(ham_page_t *page, 
        ham_u16_t lhs, ham_u16_t rhs)
{
    int_key_t *l, *r;
    btree_node_t *node=ham_page_get_btree_node(page);

    l=btree_node_get_key(page_get_owner(page), node, lhs);
    r=btree_node_get_key(page_get_owner(page), node, rhs);

    return (db_compare_keys(page_get_owner(page), page, 
                lhs, key_get_flags(l), key_get_key(l), 
                key_get_size(l), rhs, key_get_flags(r), key_get_key(r), 
                key_get_size(r)));
}

ham_offset_t
key_insert_extended(ham_db_t *db, ham_page_t *page, 
        ham_key_t *key)
{
    ham_offset_t blobid;
    ham_u8_t *data_ptr=(ham_u8_t *)key->data;
    ham_status_t st;

    ham_assert(key->size>db_get_keysize(db), ("invalid keysize"));
    
    if ((st=blob_allocate(db, 
                data_ptr +(db_get_keysize(db)-sizeof(ham_offset_t)), 
                key->size-(db_get_keysize(db)-sizeof(ham_offset_t)), 
                0, &blobid))) {
        db_set_error(db, st);
        return (0);
    }

    if (db_get_extkey_cache(db)) 
        (void)extkey_cache_insert(db_get_extkey_cache(db), blobid, 
                key->size, key->data);

    return (blobid);
}

ham_status_t
key_set_record(ham_db_t *db, int_key_t *key, ham_record_t *record, 
                ham_size_t dupe_id, ham_u32_t flags)
{
    ham_status_t st;
    ham_offset_t rid=0;
    ham_u32_t oldflags=key_get_flags(key);

    key_set_flags(key, 
            oldflags&~(KEY_BLOB_SIZE_SMALL
                |KEY_BLOB_SIZE_TINY
                |KEY_BLOB_SIZE_EMPTY));

    /*
     * no existing key, just create a new key (but not a duplicate)?
     */
    if (!key_get_ptr(key)
            || ((oldflags&KEY_BLOB_SIZE_SMALL)
                && (oldflags&KEY_BLOB_SIZE_TINY)
                && (oldflags&KEY_BLOB_SIZE_EMPTY)
                && !(flags&HAM_DUPLICATE)
                && !(flags&HAM_DUPLICATE_INSERT_BEFORE)
                && !(flags&HAM_DUPLICATE_INSERT_AFTER)
                && !(flags&HAM_DUPLICATE_INSERT_FIRST)
                && !(flags&HAM_DUPLICATE_INSERT_LAST))) {
        if (record->size>0 && record->size<=sizeof(ham_offset_t)) {
            if (record->data)
                memcpy(&rid, record->data, record->size);
            if (record->size==0)
                key_set_flags(key, key_get_flags(key)|KEY_BLOB_SIZE_EMPTY);
            else if (record->size<sizeof(ham_offset_t)) {
                char *p=(char *)&rid;
                p[sizeof(ham_offset_t)-1]=record->size;
                key_set_flags(key, key_get_flags(key)|KEY_BLOB_SIZE_TINY);
            }
            else 
                key_set_flags(key, key_get_flags(key)|KEY_BLOB_SIZE_SMALL);
            key_set_ptr(key, rid);
        }
        else {
            st=blob_allocate(db, record->data, record->size, 0, &rid);
            if (st)
                return (db_set_error(db, st));
            key_set_ptr(key, rid);
        }
    }
    /*
     * an existing key, which is overwritten with a big record?
     */
    else if (!(oldflags&KEY_HAS_DUPLICATES)
            && record->size>sizeof(ham_offset_t) 
            && !(flags&HAM_DUPLICATE) 
            && !(flags&HAM_DUPLICATE_INSERT_BEFORE)
            && !(flags&HAM_DUPLICATE_INSERT_AFTER)
            && !(flags&HAM_DUPLICATE_INSERT_FIRST)
            && !(flags&HAM_DUPLICATE_INSERT_LAST)) {
        if ((oldflags&KEY_BLOB_SIZE_SMALL)
                || (oldflags&KEY_BLOB_SIZE_TINY)
                || (oldflags&KEY_BLOB_SIZE_EMPTY)) {
            st=blob_allocate(db, record->data, record->size, 0, &rid);
            if (st)
                return (db_set_error(db, st));
            key_set_ptr(key, rid);
        }
        else {
            st=blob_overwrite(db, key_get_ptr(key), record->data, 
                    record->size, 0, &rid);
            if (st)
                return (db_set_error(db, st));
            key_set_ptr(key, rid);
        }
    }
    /*
     * an existing key which is overwritten with a small record?
     */
    else if (!(oldflags&KEY_HAS_DUPLICATES)
            && record->size<=sizeof(ham_offset_t) 
            && !(flags&HAM_DUPLICATE) 
            && !(flags&HAM_DUPLICATE_INSERT_BEFORE)
            && !(flags&HAM_DUPLICATE_INSERT_AFTER)
            && !(flags&HAM_DUPLICATE_INSERT_FIRST)
            && !(flags&HAM_DUPLICATE_INSERT_LAST)) {
        if (!((oldflags&KEY_BLOB_SIZE_SMALL)
                || (oldflags&KEY_BLOB_SIZE_TINY)
                || (oldflags&KEY_BLOB_SIZE_EMPTY))) {
            st=blob_free(db, key_get_ptr(key), BLOB_FREE_ALL_DUPES);
            if (st)
                return (db_set_error(db, st));
        }
        if (record->data)
            memcpy(&rid, record->data, record->size);
        if (record->size==0)
            key_set_flags(key, key_get_flags(key)|KEY_BLOB_SIZE_EMPTY);
        else if (record->size<sizeof(ham_offset_t)) {
            char *p=(char *)&rid;
            p[sizeof(ham_offset_t)-1]=record->size;
            key_set_flags(key, key_get_flags(key)|KEY_BLOB_SIZE_TINY);
        }
        else 
            key_set_flags(key, key_get_flags(key)|KEY_BLOB_SIZE_SMALL);
        key_set_ptr(key, rid);
    }
    /*
     * a duplicate of an existing key? - always insert it at the end of
     * the duplicate list
     *
     * (or create a duplicate list, if it does not yet exist)
     */
    else {
        ham_assert((flags&HAM_DUPLICATE) 
                || (flags&HAM_DUPLICATE_INSERT_BEFORE)
                || (flags&HAM_DUPLICATE_INSERT_AFTER)
                || (flags&HAM_DUPLICATE_INSERT_FIRST)
                || (flags&HAM_DUPLICATE_INSERT_LAST)
                || (flags&HAM_OVERWRITE), (""));
        dupe_entry_t entries[2];
        int i=0;
        memset(entries, 0, sizeof(entries));
        if (!(oldflags&KEY_HAS_DUPLICATES)) {
            dupe_entry_set_flags(&entries[i], 
                        oldflags&(KEY_BLOB_SIZE_SMALL
                                |KEY_BLOB_SIZE_TINY
                                |KEY_BLOB_SIZE_EMPTY));
            dupe_entry_set_rid(&entries[i], key_get_ptr(key));
            i++;
        }
        if (record->size<=sizeof(ham_offset_t)) {
            if (record->data)
                memcpy(&rid, record->data, record->size);
            if (record->size==0)
                dupe_entry_set_flags(&entries[i], KEY_BLOB_SIZE_EMPTY);
            else if (record->size<sizeof(ham_offset_t)) {
                char *p=(char *)&rid;
                p[sizeof(ham_offset_t)-1]=record->size;
                dupe_entry_set_flags(&entries[i], KEY_BLOB_SIZE_TINY);
            }
            else 
                dupe_entry_set_flags(&entries[i], KEY_BLOB_SIZE_SMALL);
            dupe_entry_set_rid(&entries[i], rid);
        }
        else {
            st=blob_allocate(db, record->data, record->size, 0, &rid);
            if (st)
                return (db_set_error(db, st));
            dupe_entry_set_flags(&entries[i], 0);
            dupe_entry_set_rid(&entries[i], rid);
        }
        i++;

        st=blob_duplicate_insert(db, i==2 ? 0 : key_get_ptr(key), 0, flags,
                        &entries[0], i, &rid);
        if (st)
            return (db_set_error(db, st));

        key_set_flags(key, key_get_flags(key)|KEY_HAS_DUPLICATES);
        key_set_ptr(key, rid);
    }

    return (0);
}

