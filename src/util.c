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
#include <ham/hamsterdb.h>

#include "util.h"
#include "mem.h"
#include "db.h"
#include "keys.h"
#include "error.h"
#include "blob.h"

#if HAM_OS_POSIX
extern int vsnprintf(char *str, size_t size, const char *format, va_list ap);
#endif

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

ham_key_t *
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
            db_set_error(db, st);
            return (0);
        }
        ham_assert(dest->data!=0, ("invalid extended key"));
        /* dest->size is set by db_get_extended_key() */
        ham_assert(dest->size == source->size, (0)); 
        /* the extended flag is set later, when this key is inserted */
        dest->_flags = source->_flags & ~KEY_IS_EXTENDED;
    }
    else if (source->size) {
        if (!(dest->flags & HAM_KEY_USER_ALLOC)) {
            if (dest->data)
                ham_mem_free(db, dest->data);
            dest->data = (ham_u8_t *)ham_mem_alloc(db, source->size);
            if (!dest->data) {
                db_set_error(db, HAM_OUT_OF_MEMORY);
                return 0;
            }
        }
        memcpy(dest->data, source->data, source->size);
        dest->size=source->size;
        dest->_flags=source->_flags;
    }
    else { 
        /* key.size is 0 */
        if (!(dest->flags & HAM_KEY_USER_ALLOC)) {
            if (dest->data)
                ham_mem_free(db, dest->data);
            dest->data=0;
        }
        dest->size=0;
        dest->_flags=source->_flags;
    }

    return (dest);
}

ham_key_t *
util_copy_key_int2pub(ham_db_t *db, const int_key_t *source, ham_key_t *dest)
{
    /*
     * extended key: copy the whole key
     */
    if (key_get_flags(source)&KEY_IS_EXTENDED) {
        ham_status_t st=db_get_extended_key(db, 
                    (ham_u8_t *)key_get_key(source),
                    key_get_size(source), key_get_flags(source),
                    dest);
        if (st) {
            db_set_error(db, st);
            return (0);
        }
        ham_assert(dest->data!=0, ("invalid extended key"));
        /* dest->size is set by db_get_extended_key() */
        ham_assert(dest->size == key_get_size(source), (0)); 
    }
    else if (key_get_size(source)) {
        if (!(dest->flags & HAM_KEY_USER_ALLOC)) {
            if (dest->data)
                ham_mem_free(db, dest->data);
            dest->data = (ham_u8_t *)ham_mem_alloc(db, key_get_size(source));
            if (!dest->data) {
                db_set_error(db, HAM_OUT_OF_MEMORY);
                return 0;
            }
        }

        memcpy(dest->data, key_get_key(source), key_get_size(source));
        dest->size=key_get_size(source);
    }
    else {
        /* key.size is 0 */
        if (!(dest->flags & HAM_KEY_USER_ALLOC)) {
            if (dest->data)
                ham_mem_free(db, dest->data);
            dest->data=0;
        }
        dest->size=0;
        dest->data=0;
    }

    dest->flags=0;

    return (dest);
}

ham_status_t
util_read_record(ham_db_t *db, ham_record_t *record, ham_u32_t flags)
{
    ham_status_t st;
    ham_bool_t noblob=HAM_FALSE;
    ham_size_t blobsize;

    /*
     * if this key has duplicates: fetch the duplicate entry
     */
    if (record->_intflags&KEY_HAS_DUPLICATES) {
        dupe_entry_t entry;
        ham_status_t st=blob_duplicate_get(db, record->_rid, 0, &entry);
        if (st)
            return (db_set_error(db, st));
        record->_intflags=dupe_entry_get_flags(&entry);
        record->_rid     =dupe_entry_get_rid(&entry);
    }

    /*
     * sometimes (if the record size is small enough), there's
     * no blob available, but the data is stored in the record's
     * offset.
     */
    if (record->_intflags&KEY_BLOB_SIZE_TINY) {
        /* the highest byte of the record id is the size of the blob */
        char *p=(char *)&record->_rid;
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
        /* set to a dummy value, so the second if-branch is executed */
        blobsize = 0xffffffff;
    }

    if (noblob && blobsize > 0) {
        if (!(record->flags & HAM_RECORD_USER_ALLOC)) {
            st=db_resize_allocdata(db, blobsize);
            if (st)
                return (st);
            record->data = db_get_record_allocdata(db);
        }

        memcpy(record->data, &record->_rid, blobsize);
        record->size = blobsize;
    }
    else if (!noblob && blobsize != 0) {
        return (blob_read(db, record->_rid, record, flags));
    }

    return (0);
}

ham_status_t
util_read_key(ham_db_t *db, int_key_t *source, ham_key_t *dest)
{
    /*
     * extended key: copy the whole key
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
                if (dest->data
                    	&& db_get_key_allocdata(db) != dest->data) {
                    ham_mem_free(db, dest->data);
                }
                dest->data=0;
            }
            return db_set_error(db, st);
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
                if (db_get_key_allocdata(db))
                    ham_mem_free(db, db_get_key_allocdata(db));
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
                    if (db_get_key_allocdata(db))
                        ham_mem_free(db, db_get_key_allocdata(db));
                    db_set_key_allocdata(db, ham_mem_alloc(db, keysize));
                    if (!db_get_key_allocdata(db)) {
                        db_set_key_allocsize(db, 0);
                        dest->data = NULL;
                        return (db_set_error(db, HAM_OUT_OF_MEMORY));
                    }
                    else {
                        db_set_key_allocsize(db, keysize);
                    }
                }
                dest->data = db_get_key_allocdata(db);
                memcpy(dest->data, key_get_key(source), keysize);
                //db_set_key_allocdata(db, dest->data);
                //db_set_key_allocsize(db, keysize);
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
        ham_assert(dest->size==sizeof(ham_u64_t), (""));
        if (dest->data==0 || dest->size!=sizeof(ham_u64_t))
            return (HAM_INTERNAL_ERROR);
        recno=*(ham_u64_t *)dest->data;
        recno=ham_db2h64(recno);
        memcpy(dest->data, &recno, sizeof(ham_u64_t));
    }

    return (0);
}
