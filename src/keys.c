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

#include "blob.h"
#include "btree.h"
#include "db.h"
#include "env.h"
#include "error.h"
#include "extkeys.h"
#include "keys.h"
#include "mem.h"
#include "page.h"


int
key_compare_pub_to_int(ham_db_t *db, ham_page_t *page, 
        ham_key_t *lhs, ham_u16_t rhs_int)
{
    int_key_t *r;
    btree_node_t *node=ham_page_get_btree_node(page);
    ham_key_t rhs;
    int cmp;
    ham_status_t st;

	ham_assert(db == page_get_owner(page), (0));

    r=btree_node_get_key(db, node, rhs_int);

    st=db_prepare_ham_key_for_compare(db, r, &rhs);
    if (st) {
        ham_assert(st<-1, (""));
        return st;
    }

    cmp=db_compare_keys(db, lhs, &rhs);

	db_release_ham_key_after_compare(db, &rhs);
	/* ensures key is always released; errors will be detected by caller */

    return (cmp);
}

ham_status_t
key_insert_extended(ham_offset_t *rid_ref, ham_db_t *db, ham_page_t *page, 
        ham_key_t *key)
{
    ham_offset_t blobid;
    ham_u8_t *data_ptr=(ham_u8_t *)key->data;
    ham_status_t st;

    ham_assert(key->size>db_get_keysize(db), ("invalid keysize"));
    
	*rid_ref = 0;

    if ((st=blob_allocate(db_get_env(db), db,
                data_ptr +(db_get_keysize(db)-sizeof(ham_offset_t)), 
                key->size-(db_get_keysize(db)-sizeof(ham_offset_t)), 
                0, &blobid))) {
        return st;
    }

    if (db_get_extkey_cache(db)) 
    {
        st = extkey_cache_insert(db_get_extkey_cache(db), blobid, 
                key->size, key->data);
        if (st)
            return st;
    }

    *rid_ref = blobid;
	return HAM_SUCCESS;
}

ham_status_t
key_set_record(ham_db_t *db, int_key_t *key, ham_record_t *record, 
                ham_size_t position, ham_u32_t flags, 
                ham_size_t *new_position)
{
    ham_status_t st;
	ham_env_t *env = db_get_env(db);
    ham_offset_t rid = 0;
    ham_offset_t ptr = key_get_ptr(key);
    ham_u8_t oldflags = key_get_flags(key);

    key_set_flags(key, 
            oldflags&~(KEY_BLOB_SIZE_SMALL
                |KEY_BLOB_SIZE_TINY
                |KEY_BLOB_SIZE_EMPTY));

    /*
     * no existing key, just create a new key (but not a duplicate)?
     */
    if (!ptr
            && !(oldflags&(KEY_BLOB_SIZE_SMALL
                          |KEY_BLOB_SIZE_TINY
                          |KEY_BLOB_SIZE_EMPTY))) 
    {
        if (record->size<=sizeof(ham_offset_t)) {
            if (record->data)
                memcpy(&rid, record->data, record->size);
            if (record->size==0)
                key_set_flags(key, key_get_flags(key)|KEY_BLOB_SIZE_EMPTY);
            else if (record->size<sizeof(ham_offset_t)) {
                char *p=(char *)&rid;
                p[sizeof(ham_offset_t)-1]=(char)record->size;
                key_set_flags(key, key_get_flags(key)|KEY_BLOB_SIZE_TINY);
            }
            else 
                key_set_flags(key, key_get_flags(key)|KEY_BLOB_SIZE_SMALL);
            key_set_ptr(key, rid);
        }
        else {
            st=blob_allocate(env, db, record->data, record->size, 0, &rid);
            if (st)
                return (st);
            key_set_ptr(key, rid);
        }
    }
    else if (!(oldflags&KEY_HAS_DUPLICATES)
            && record->size>sizeof(ham_offset_t) 
            && !(flags&(HAM_DUPLICATE
                        |HAM_DUPLICATE_INSERT_BEFORE
                        |HAM_DUPLICATE_INSERT_AFTER
                        |HAM_DUPLICATE_INSERT_FIRST
                        |HAM_DUPLICATE_INSERT_LAST))) 
    {
        /*
         * an existing key, which is overwritten with a big record
         Note that the case where old record is EMPTY (!ptr) or
         SMALL (size = 8, but content = 00000000 --> !ptr) are caught here
         and in the next branch, as they should.
         */
        if (oldflags&(KEY_BLOB_SIZE_SMALL
                     |KEY_BLOB_SIZE_TINY
                     |KEY_BLOB_SIZE_EMPTY))
        {
            rid=0;
            st=blob_allocate(env, db, record->data, record->size, 0, &rid);
            if (st)
                return (st);
            if (rid)
                key_set_ptr(key, rid);
        }
        else {
            st=blob_overwrite(env, db, ptr, record->data, 
                    record->size, 0, &rid);
            if (st)
                return (st);
            key_set_ptr(key, rid);
        }
    }
    else if (!(oldflags&KEY_HAS_DUPLICATES)
            && record->size<=sizeof(ham_offset_t) 
            && !(flags&(HAM_DUPLICATE
                        |HAM_DUPLICATE_INSERT_BEFORE
                        |HAM_DUPLICATE_INSERT_AFTER
                        |HAM_DUPLICATE_INSERT_FIRST
                        |HAM_DUPLICATE_INSERT_LAST))) {
        /*
         * an existing key which is overwritten with a small record
         */
        if (!(oldflags&(KEY_BLOB_SIZE_SMALL
                       |KEY_BLOB_SIZE_TINY
                       |KEY_BLOB_SIZE_EMPTY)))
        {
            st=blob_free(env, db, ptr, 0);
            if (st)
                return st;
        }
        if (record->data)
            memcpy(&rid, record->data, record->size);
        if (record->size==0)
            key_set_flags(key, key_get_flags(key)|KEY_BLOB_SIZE_EMPTY);
        else if (record->size<sizeof(ham_offset_t)) {
            char *p=(char *)&rid;
            p[sizeof(ham_offset_t)-1]=(char)record->size;
            key_set_flags(key, key_get_flags(key)|KEY_BLOB_SIZE_TINY);
        }
        else 
            key_set_flags(key, key_get_flags(key)|KEY_BLOB_SIZE_SMALL);
        key_set_ptr(key, rid);
    }
    else 
    {
        /*
         * a duplicate of an existing key - always insert it at the end of
         * the duplicate list (unless the DUPLICATE flags say otherwise OR
         * when we have a duplicate-record comparison function for
         * ordered insertion of duplicate records)
         *
         * create a duplicate list, if it does not yet exist
         */
        dupe_entry_t entries[2];
        int i=0;
        ham_assert((flags&(HAM_DUPLICATE
                          |HAM_DUPLICATE_INSERT_BEFORE
                          |HAM_DUPLICATE_INSERT_AFTER
                          |HAM_DUPLICATE_INSERT_FIRST
                          |HAM_DUPLICATE_INSERT_LAST
                          |HAM_OVERWRITE)), (""));
        memset(entries, 0, sizeof(entries));
        if (!(oldflags&KEY_HAS_DUPLICATES)) 
        {
            ham_assert((flags&(HAM_DUPLICATE
                              |HAM_DUPLICATE_INSERT_BEFORE
                              |HAM_DUPLICATE_INSERT_AFTER
                              |HAM_DUPLICATE_INSERT_FIRST
                              |HAM_DUPLICATE_INSERT_LAST)), (""));
            dupe_entry_set_flags(&entries[i], 
                        oldflags&(KEY_BLOB_SIZE_SMALL
                                |KEY_BLOB_SIZE_TINY
                                |KEY_BLOB_SIZE_EMPTY));
            dupe_entry_set_rid(&entries[i], ptr);
            i++;
        }
        if (record->size<=sizeof(ham_offset_t)) 
        {
            if (record->data)
                memcpy(&rid, record->data, record->size);
            if (record->size==0)
                dupe_entry_set_flags(&entries[i], KEY_BLOB_SIZE_EMPTY);
            else if (record->size<sizeof(ham_offset_t)) {
                char *p=(char *)&rid;
                p[sizeof(ham_offset_t)-1]=(char)record->size;
                dupe_entry_set_flags(&entries[i], KEY_BLOB_SIZE_TINY);
            }
            else 
                dupe_entry_set_flags(&entries[i], KEY_BLOB_SIZE_SMALL);
            dupe_entry_set_rid(&entries[i], rid);
        }
        else 
        {
            st=blob_allocate(env, db, record->data, record->size, 0, &rid);
            if (st)
                return (st);
            dupe_entry_set_flags(&entries[i], 0);
            dupe_entry_set_rid(&entries[i], rid);
        }
        i++;

        rid=0;
        st=blob_duplicate_insert(db, 
                (i==2 ? 0 : ptr), record, position,
                flags, &entries[0], i, &rid, new_position);
        if (st) {
            /* don't leak memory through the blob allocation above */
            ham_assert((!(dupe_entry_get_flags(&entries[i-1]) 
                            & (KEY_BLOB_SIZE_SMALL
                               | KEY_BLOB_SIZE_TINY
                               | KEY_BLOB_SIZE_EMPTY)))
                        == (record->size>sizeof(ham_offset_t)), (0));

            if (record->size > sizeof(ham_offset_t)) 
            {
                (void)blob_free(env, db, dupe_entry_get_rid(&entries[i-1]), 0);
            }
            return st;
        }

        key_set_flags(key, key_get_flags(key)|KEY_HAS_DUPLICATES);
        if (rid)
            key_set_ptr(key, rid);
    }

    return (0);
}

ham_status_t
key_erase_record(ham_db_t *db, int_key_t *key, 
                ham_size_t dupe_id, ham_u32_t flags)
{
    ham_status_t st;
    ham_offset_t rid;

    if (!(key_get_flags(key)&(KEY_BLOB_SIZE_SMALL
                        |KEY_BLOB_SIZE_TINY
                        |KEY_BLOB_SIZE_EMPTY))) {
        if (key_get_flags(key)&KEY_HAS_DUPLICATES) {
            /* delete one (or all) duplicates */
            st=blob_duplicate_erase(db, key_get_ptr(key), dupe_id, flags,
                    &rid);
            if (st)
                return (st);
            if (flags&BLOB_FREE_ALL_DUPES) {
                key_set_flags(key, key_get_flags(key)&~(KEY_HAS_DUPLICATES));
                key_set_ptr(key, 0);
            }
            else {
                key_set_ptr(key, rid);
                if (!rid) /* rid == 0: the last duplicate was deleted */
                    key_set_flags(key, 0);
            }
        }
        else {
            /* delete the blob */
            st=blob_free(db_get_env(db), db, key_get_ptr(key), 0);
            if (st)
                return (st);
            key_set_ptr(key, 0);
        }
    }
    else {
        key_set_flags(key, key_get_flags(key)&~(KEY_BLOB_SIZE_SMALL
                    | KEY_BLOB_SIZE_TINY
                    | KEY_BLOB_SIZE_EMPTY
                    | KEY_HAS_DUPLICATES));
        key_set_ptr(key, 0);
    }

    return (0);
}

ham_offset_t
key_get_extended_rid(ham_db_t *db, int_key_t *key)
{
    ham_offset_t rid;
    memcpy(&rid, key_get_key(key)+(db_get_keysize(db)-sizeof(ham_offset_t)),
            sizeof(rid));
    return (ham_db2h_offset(rid));
}

void
key_set_extended_rid(ham_db_t *db, int_key_t *key, ham_offset_t rid)
{
    rid=ham_h2db_offset(rid);
    memcpy(key_get_key(key)+(db_get_keysize(db)-sizeof(ham_offset_t)),
            &rid, sizeof(rid));
}

