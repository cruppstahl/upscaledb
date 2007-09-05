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

#include "util.h"
#include "mem.h"
#include "db.h"
#include "keys.h"
#include "error.h"
#include "blob.h"

ham_key_t *
util_copy_key(ham_db_t *db, const ham_key_t *source, ham_key_t *dest)
{
    memset(dest, 0, sizeof(*dest));

    /*
     * extended key: copy the whole key
     */
    if (source->_flags&KEY_IS_EXTENDED) {
        ham_status_t st=db_get_extended_key(db, source->data,
                    source->size, source->_flags, (ham_u8_t **)&dest->data);
        if (st) {
            db_set_error(db, st);
            return (0);
        }
        ham_assert(dest->data!=0, ("invalid extended key"));
        dest->size=source->size;
        /* the extended flag is set later, when this key is inserted */
        dest->_flags=source->_flags&(~KEY_IS_EXTENDED);
    }
    else {
        dest->data=(ham_u8_t *)ham_mem_alloc(db, source->size);
        if (!dest->data) {
            db_set_error(db, HAM_OUT_OF_MEMORY);
            return (0);
        }

        memcpy(dest->data, source->data, source->size);
        dest->size=source->size;
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
                    (ham_u8_t **)&dest->data);
        if (st) {
            db_set_error(db, st);
            return (0);
        }
        ham_assert(dest->data!=0, ("invalid extended key"));
        dest->size=key_get_size(source);
    }
    else if (key_get_size(source)) {
        dest->data=(ham_u8_t *)ham_mem_alloc(db, key_get_size(source));
        if (!dest->data) {
            db_set_error(db, HAM_OUT_OF_MEMORY);
            return (0);
        }

        memcpy(dest->data, key_get_key(source), key_get_size(source));
        dest->size=key_get_size(source);
    }
    else { /* key.size is 0 */
        dest->size=0;
        dest->data=0;
    }

    dest->flags=0;

    return (dest);
}

ham_status_t
util_read_record(ham_db_t *db, ham_record_t *record, ham_u32_t flags)
{
    ham_bool_t noblob=HAM_FALSE;

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
        /* the highest nibble of the record id is the size of the blob */
        char *p=(char *)&record->_rid;
        record->size=p[sizeof(ham_offset_t)-1];
        noblob=HAM_TRUE;
    }
    else if (record->_intflags&KEY_BLOB_SIZE_SMALL) {
        /* record size is sizeof(ham_offset_t) */
        record->size=sizeof(ham_offset_t);
        noblob=HAM_TRUE;
    }
    else if (record->_intflags&KEY_BLOB_SIZE_EMPTY) {
        /* record size is 0 */
        record->size=0;
        noblob=HAM_TRUE;
    }
    else
        /* set to a dummy value, so the second if-branch is 
         * executed */
        record->size=0xffffffff;

    if (noblob && record->size>0) {
        if (!(record->flags & HAM_RECORD_USER_ALLOC)) {
            if (record->size>db_get_record_allocsize(db)) {
                if (db_get_record_allocdata(db))
                    ham_mem_free(db, db_get_record_allocdata(db));
                db_set_record_allocdata(db, ham_mem_alloc(db, record->size));
                if (!db_get_record_allocdata(db)) {
                    db_set_record_allocsize(db, 0);
                    return (db_set_error(db, HAM_OUT_OF_MEMORY));;
                }
                else {
                    db_set_record_allocsize(db, record->size);
                }
            }
            record->data=db_get_record_allocdata(db);
        }

        memcpy(record->data, &record->_rid, record->size);
    }
    else if (!noblob && record->size!=0) {
        return (blob_read(db, record->_rid, record, flags));
    }

    return (0);
}

ham_status_t
util_read_key(ham_db_t *db, int_key_t *source, ham_key_t *dest, ham_u32_t flags)
{
    ham_u8_t *data;

    /*
     * extended key: copy the whole key
     */
    if (key_get_flags(source)&KEY_IS_EXTENDED) {
        ham_status_t st=db_get_extended_key(db, key_get_key(source),
                    key_get_size(source), key_get_flags(source),
                    (ham_u8_t **)&data);
        if (st) {
            db_set_error(db, st);
            return (0);
        }
        ham_assert(data!=0, ("invalid extended key"));
        dest->size=key_get_size(source);

        if (key_get_size(source)) {
            if (dest->flags&HAM_KEY_USER_ALLOC) {
                memcpy(dest->data, data, key_get_size(source));
                ham_mem_free(db, data);
            }
            else {
                if (db_get_key_allocdata(db))
                    ham_mem_free(db, db_get_key_allocdata(db));
                db_set_key_allocdata(db, data);
                db_set_key_allocsize(db, dest->size);
                dest->data=data;
            }
        }
        else {
            dest->data=0;
        }
    }

    /*
     * otherwise (non-extended key)...
     */
    else {
        dest->size=key_get_size(source);

        if (key_get_size(source)) {
            if (dest->flags&HAM_KEY_USER_ALLOC) {
                memcpy(dest->data, key_get_key(source), key_get_size(source));
            }
            else {
                if (dest->size>db_get_key_allocsize(db)) {
                    if (db_get_key_allocdata(db))
                        ham_mem_free(db, db_get_key_allocdata(db));
                    db_set_key_allocdata(db, ham_mem_alloc(db, dest->size));
                    if (!db_get_key_allocdata(db)) {
                        db_set_key_allocsize(db, 0);
                        return (db_set_error(db, HAM_OUT_OF_MEMORY));
                    }
                    else {
                        db_set_key_allocsize(db, dest->size);
                    }
                }
                dest->data=db_get_key_allocdata(db);

                memcpy(dest->data, key_get_key(source), key_get_size(source));
                db_set_key_allocdata(db, dest->data);
                db_set_key_allocsize(db, dest->size);
            }
        }
        else {
            dest->data=0;
        }
    }

    /*
     * recno databases: recno is stored in db-endian!
     */
    if (db_get_rt_flags(db)&HAM_RECORD_NUMBER) {
        ham_u64_t recno=*(ham_u64_t *)dest->data;
        ham_assert(dest->size==sizeof(ham_u64_t), (""));
        recno=ham_db2h64(recno);
        memcpy(dest->data, &recno, sizeof(ham_u64_t));
    }

    return (0);
}
