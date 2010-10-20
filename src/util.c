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
#include <stdio.h>
#include <stdarg.h>

#include "blob.h"
#include "db.h"
#include "env.h"
#include "error.h"
#include "keys.h"
#include "mem.h"
#include "util.h"


int
util_vsnprintf(char *str, size_t size, const char *format, va_list ap)
{
#if defined(HAM_OS_POSIX)
    return vsnprintf(str, size, format, ap);
#elif defined(HAM_OS_WIN32)
    return _vsnprintf(str, size, format, ap);
#else
    (void)size;
    return vsprintf(str, format, ap);
#endif
}


ham_status_t
util_copy_key(ham_db_t *db, const ham_key_t *source, ham_key_t *dest)
{
    //memset(dest, 0, sizeof(*dest));

    /*
     * extended key: copy the whole key
     */
    if (source->_flags&KEY_IS_EXTENDED) {
        ham_status_t st=db_get_extended_key(db, source->data,
                    source->size, source->_flags, dest);
        if (st) {
            return st;
        }
        ham_assert(dest->data!=0, ("invalid extended key"));
        /* dest->size is set by db_get_extended_key() */
        ham_assert(dest->size == source->size, (0)); 
        /* the extended flag is set later, when this key is inserted */
        dest->_flags = source->_flags & ~KEY_IS_EXTENDED;
    }
    else if (source->size) 
	{
        if (!(dest->flags & HAM_KEY_USER_ALLOC)) 
		{
			if (!dest->data || dest->size < source->size)
			{
				if (dest->data)
					allocator_free(env_get_allocator(db_get_env(db)), dest->data);
				dest->data = (ham_u8_t *)allocator_alloc(env_get_allocator(db_get_env(db)), source->size);
				if (!dest->data) 
				{
					return HAM_OUT_OF_MEMORY;
				}
			}
		}
        memcpy(dest->data, source->data, source->size);
        dest->size=source->size;
        dest->_flags=source->_flags;
    }
    else 
	{ 
        /* key.size is 0 */
        if (!(dest->flags & HAM_KEY_USER_ALLOC)) {
            if (dest->data)
                allocator_free(env_get_allocator(db_get_env(db)), dest->data);
            dest->data=0;
        }
        dest->size=0;
        dest->_flags=source->_flags;
    }

    return HAM_SUCCESS;
}

ham_status_t
util_copy_key_int2pub(ham_db_t *db, const int_key_t *source, ham_key_t *dest)
{
    mem_allocator_t *alloc=env_get_allocator(db_get_env(db));

    /*
     * extended key: copy the whole key
     */
    if (key_get_flags(source)&KEY_IS_EXTENDED) {
        ham_status_t st=db_get_extended_key(db, 
                    (ham_u8_t *)key_get_key(source),
                    key_get_size(source), key_get_flags(source),
                    dest);
        if (st) {
            return st;
        }
        ham_assert(dest->data!=0, ("invalid extended key"));
        /* dest->size is set by db_get_extended_key() */
        ham_assert(dest->size == key_get_size(source), (0)); 
    }
    else if (key_get_size(source)) {
        if (!(dest->flags & HAM_KEY_USER_ALLOC)) {
			if (!dest->data || dest->size < key_get_size(source)) {
				if (dest->data)
					allocator_free(alloc, dest->data);
				dest->data = (ham_u8_t *)allocator_alloc(alloc, 
                            key_get_size(source));
				if (!dest->data) 
					return HAM_OUT_OF_MEMORY;
			}
		}

        memcpy(dest->data, key_get_key(source), key_get_size(source));
        dest->size=key_get_size(source);
    }
    else {
        /* key.size is 0 */
        if (!(dest->flags & HAM_KEY_USER_ALLOC)) {
            if (dest->data)
                allocator_free(alloc, dest->data);
            dest->data=0;
        }
        dest->size=0;
        dest->data=0;
    }

    dest->flags=0;

    return HAM_SUCCESS;
}

ham_status_t
util_read_record(ham_db_t *db, ham_record_t *record, ham_u64_t *ridptr,
                ham_u32_t flags)
{
    ham_status_t st;
    ham_bool_t noblob=HAM_FALSE;
    ham_size_t blobsize;

    /*
     * if this key has duplicates: fetch the duplicate entry
     */
    if (record->_intflags&KEY_HAS_DUPLICATES) {
        dupe_entry_t entry;
        ham_status_t st=blob_duplicate_get(db_get_env(db), 
                            record->_rid, 0, &entry);
        if (st)
            return st;
        record->_intflags=dupe_entry_get_flags(&entry);
        record->_rid     =dupe_entry_get_rid(&entry);
        ridptr           =dupe_entry_get_ridptr(&entry);
    }

    /*
     * sometimes (if the record size is small enough), there's
     * no blob available, but the data is stored in the record's
     * offset.
     */
    if (record->_intflags&KEY_BLOB_SIZE_TINY) {
        /* the highest byte of the record id is the size of the blob */
        char *p=(char *)ridptr;
        blobsize = p[sizeof(ham_offset_t)-1];
        noblob=HAM_TRUE;
    }
    else if (record->_intflags&KEY_BLOB_SIZE_SMALL) {
        /* record size is sizeof(ham_offset_t) */
        blobsize = sizeof(ham_offset_t);
        noblob=HAM_TRUE;
    }
    else if (record->_intflags&KEY_BLOB_SIZE_EMPTY) {
        /* record size is 0 */
        blobsize = 0;
        noblob=HAM_TRUE;
    }
    else {
        /* set to a dummy value, so the third if-branch is executed */
        blobsize = 0xffffffff;
    }

    if (noblob && blobsize == 0) {
        record->size = 0;
        record->data = 0;
    }
    else if (noblob && blobsize > 0) {
        if (!(record->flags & HAM_RECORD_USER_ALLOC)
                && (flags&HAM_DIRECT_ACCESS)) {
            record->data=ridptr;
            record->size=blobsize;
        }
        else {
            if (!(record->flags & HAM_RECORD_USER_ALLOC)) {
                st=db_resize_record_allocdata(db, blobsize);
                if (st)
                    return (st);
                record->data=db_get_record_allocdata(db);
            }
            memcpy(record->data, ridptr, blobsize);
            record->size = blobsize;
        }
    }
    else if (!noblob && blobsize != 0) {
        return (blob_read(db, record->_rid, record, flags));
    }

    return HAM_SUCCESS;
}

ham_status_t
util_read_key(ham_db_t *db, int_key_t *source, ham_key_t *dest)
{
    mem_allocator_t *alloc=env_get_allocator(db_get_env(db));

    /*
     * extended key: copy the whole key, not just the
     * overflow region!
     */
    if (key_get_flags(source)&KEY_IS_EXTENDED) {
        ham_u16_t keysize = key_get_size(source);
        ham_status_t st=db_get_extended_key(db, key_get_key(source),
                    keysize, key_get_flags(source),
                    dest);
        if (st) {
            if (!(dest->flags & HAM_KEY_USER_ALLOC)) {
                /*
                 * key data can be allocated in db_get_extended_key():
                 * prevent that heap memory leak
                 */
                if (dest->data && (db_get_key_allocdata(db)!=dest->data))
                    allocator_free(alloc, dest->data);
                dest->data=0;
            }
            return st;
        }
        ham_assert(dest->data != 0, ("invalid extended key"));

        ham_assert(sizeof(key_get_size(source)) == sizeof(keysize), (0));
        ham_assert(sizeof(dest->size) == sizeof(keysize), (0));
        ham_assert(keysize == dest->size, (0));
        ham_assert(keysize ? dest->data != 0 : 1, (0));

        if (dest->flags & HAM_KEY_USER_ALLOC) {
            ham_assert(dest->size == keysize, (0));
        }
        else {
            if (keysize) {
                (void)db_resize_key_allocdata(db, 0);
                db_set_key_allocdata(db, dest->data);
                db_set_key_allocsize(db, keysize);
            }
            else {
                dest->data=0;
            }
        }
    }
    else {
        /*
         * otherwise (non-extended key)...
         */
        ham_u16_t keysize = key_get_size(source);

        ham_assert(sizeof(key_get_size(source)) == sizeof(keysize), (0));
        if (keysize) {
            if (dest->flags & HAM_KEY_USER_ALLOC) {
                memcpy(dest->data, key_get_key(source), keysize);
            }
            else {
                if (keysize > db_get_key_allocsize(db)) {
                    ham_status_t st=db_resize_key_allocdata(db, keysize);
                    if (st)
                        return (st);
                    else
                        db_set_key_allocsize(db, keysize);
                }
                dest->data = db_get_key_allocdata(db);
                memcpy(dest->data, key_get_key(source), keysize);
            }
        }
        else {
            if (!(dest->flags & HAM_KEY_USER_ALLOC)) {
                dest->data=0;
            }
        }

        ham_assert(sizeof(dest->size) == sizeof(keysize), (0));
        dest->size = keysize;
    }

    /*
     * recno databases: recno is stored in db-endian!
     */
    if (db_get_rt_flags(db)&HAM_RECORD_NUMBER) 
    {
        ham_u64_t recno;
        ham_assert(dest->data!=0, ("this should never happen."));
        ham_assert(dest->size==sizeof(ham_u64_t), (0));
        if (dest->data==0 || dest->size!=sizeof(ham_u64_t))
            return (HAM_INTERNAL_ERROR);
        recno=*(ham_u64_t *)dest->data;
        recno=ham_db2h64(recno);
        memcpy(dest->data, &recno, sizeof(ham_u64_t));
    }

    return HAM_SUCCESS;
}

