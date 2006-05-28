/**
 * Copyright 2005, 2006 Christoph Rupp (chris@crupp.de)
 * see file LICENSE for license and copyright information
 *
 *
 */

#include <string.h>
#include <ham/hamsterdb.h>
#include <ham/config.h>
#include "error.h"
#include "mem.h"
#include "db.h"
#include "version.h"
#include "txn.h"
#include "keys.h"
#include "cache.h"
#include "blob.h"

/*
 * default callback function which dumps a key
 */
static void
my_dump_cb(const ham_u8_t *key, ham_size_t keysize)
{
    ham_size_t i, limit;

    if (keysize>16)
        limit=16;
    else
        limit=keysize;

    for (i=0; i<limit; i++)
        printf("%02x ", key[i]);

    if (keysize>limit)
        printf("... (%d more bytes)\n", keysize-limit);
    else
        printf("\n");
}

const char *
ham_strerror(ham_status_t result)
{
    switch (result) {
        case HAM_SUCCESS: 
            return ("Success");
        case HAM_SHORT_READ: 
            return ("Short read");
        case HAM_SHORT_WRITE: 
            return ("Short write");
        case HAM_INV_KEYSIZE: 
            return ("Invalid key size");
        case HAM_INV_PAGESIZE: 
            return ("Invalid page size");
        case HAM_DB_ALREADY_OPEN: 
            return ("Db already open");
        case HAM_OUT_OF_MEMORY: 
            return ("Out of memory");
        case HAM_INV_BACKEND:
            return ("Invalid backend");
        case HAM_INV_PARAMETER:
            return ("Invalid parameter");
        case HAM_INV_FILE_HEADER:
            return ("Invalid database file header");
        case HAM_INV_FILE_VERSION:
            return ("Invalid database file version");
        case HAM_KEY_NOT_FOUND:
            return ("Key not found");
        case HAM_DUPLICATE_KEY:
            return ("Duplicate key");
        case HAM_INTEGRITY_VIOLATED:
            return ("Internal integrity violated");
        case HAM_INTERNAL_ERROR:
            return ("Internal error");
        case HAM_DB_READ_ONLY:
            return ("Database opened read only");
        case HAM_BLOB_NOT_FOUND:
            return ("Data blob not found");
        case HAM_PREFIX_REQUEST_FULLKEY:
            return ("Comparator needs more data");

        /* fall back to strerror() */
        default: 
            return (strerror(result));
    }
}

ham_status_t
ham_new(ham_db_t **db)
{
    ham_size_t size;

    size=SIZEOF_PERS_HEADER+sizeof((*db)->_npers);
    /* allocate memory for the ham_db_t-structure */
    *db=(ham_db_t *)ham_mem_alloc(size);
    if (!(*db)) 
        return (HAM_OUT_OF_MEMORY);

    /* reset the whole structure */
    memset(*db, 0, size);
    db_set_fd((*db), -1);
    return (0);
}

ham_status_t
ham_delete(ham_db_t *db)
{
    /* free cached data pointers */
    if (db_get_record_allocdata(db))
        ham_mem_free(db_get_record_allocdata(db));

    /* close the backend */
    if (db_get_backend(db)) {
        ham_backend_t *be=db_get_backend(db);
        be->_fun_delete(be);
        ham_mem_free(be);
    }

    /* get rid of the cache */
    if (db_get_cache(db))
        cache_delete(db_get_cache(db));

    /* free all remaining memory */
    ham_mem_free(db);
    return (0);
}

ham_status_t
ham_open(ham_db_t *db, const char *filename, ham_u32_t flags)
{
    ham_fd_t fd;
    ham_status_t st;
    ham_cache_t *cache;
    ham_backend_t *backend;

    /* cannot open an in-memory-db */
    if (flags&HAM_IN_MEMORY_DB)
        return (HAM_INV_PARAMETER);

    /* 
     * TODO mmap ist eingeschaltet
    if (!(flags&HAM_IN_MEMORY_DB))
        flags|=DB_USE_MMAP;
     */

    /* open the file */
    st=os_open(filename, flags, &fd);
    if (st) {
        ham_log("os_open of %s failed with status %d (%s)", filename, 
                st, ham_strerror(st));
        db_set_error(db, st);
        return (st);
    }

    /* read the database header */
    st=os_read(fd, db->_u._persistent, SIZEOF_PERS_HEADER);
    if (st) {
        ham_log("os_read of %s failed with status %d (%s)", filename, 
                st, ham_strerror(st));
        db_set_error(db, st);
        return (st);
    }

    /* check the file magic */
    if (db_get_magic(db, 0)!='H' || 
        db_get_magic(db, 1)!='A' || 
        db_get_magic(db, 2)!='M' || 
        db_get_magic(db, 3)!='\0') {
        ham_log("invalid file type - %s is not a hamster-db", filename);
        db_set_error(db, HAM_INV_FILE_HEADER);
        return (HAM_INV_FILE_HEADER);
    }

    /* check the database version */
    if (db_get_version(db, 0)!=HAM_VERSION_MAJ || 
        db_get_version(db, 1)!=HAM_VERSION_MIN) {
        ham_log("invalid file version", 0);
        db_set_error(db, HAM_INV_FILE_VERSION);
        return (HAM_INV_FILE_VERSION);
    }

    /* initialize the database handle */
    db_set_fd(db, fd);
    db_set_error(db, 0);

    /* create the backend */
    backend=db_create_backend(db, flags);
    if (!backend) {
        ham_log("unable to create backend with flags 0x%x", flags);
        db_set_error(db, HAM_INV_BACKEND);
        return (HAM_INV_BACKEND);
    }
    db_set_backend(db, backend);

    /* initialize the backend */
    st=backend->_fun_open(backend, flags);
    if (st) {
        ham_log("backend create() failed with status %d (%s)", 
                st, ham_strerror(st));
        db_set_error(db, st);
        return (st);
    }

    /* initialize the cache */
    cache=cache_new(db, 0, HAM_DEFAULT_CACHESIZE);
    if (!cache)
        return (db_get_error(db));
    db_set_cache(db, cache);

    /* set the key compare function */
    ham_set_compare_func(db, db_default_compare);
    ham_set_prefix_compare_func(db, 0); /* TODO */

    return (HAM_SUCCESS);
}

ham_status_t
ham_create(ham_db_t *db, const char *filename, ham_u32_t flags, ham_u32_t mode)
{
    return (ham_create_ex(db, filename, flags, mode, 0, 0, 
                HAM_DEFAULT_CACHESIZE));
}

ham_status_t
ham_create_ex(ham_db_t *db, const char *filename, 
        ham_u32_t flags, ham_u32_t mode, ham_u16_t pagesize, 
        ham_u16_t keysize, ham_size_t cachesize)
{
    ham_fd_t fd;
    ham_status_t st;
    ham_cache_t *cache;
    ham_backend_t *backend;

    /* 
     * TODO mmap ist eingeschaltet
    if (!(flags&HAM_IN_MEMORY_DB))
        flags|=DB_USE_MMAP;
     */

    if (keysize==0)
        keysize=32-sizeof(key_t)-1;

    /*
     * make sure that the pagesize is aligned to 512k and that 
     * a page is big enough for at least 4 keys
     */
    if (pagesize==0) 
        pagesize=HAM_DEFAULT_PAGESIZE;
    else if (pagesize%512)
        return (HAM_INV_PAGESIZE);
    if (pagesize/keysize<4)
        return (HAM_INV_KEYSIZE);
    
    db_set_magic(db, 'H', 'A', 'M', '\0');
    db_set_version(db, HAM_VERSION_MAJ, HAM_VERSION_MIN, HAM_VERSION_REV, 0);
    db_set_serialno(db, HAM_SERIALNO);
    db_set_fd(db, fd);
    db_set_flags(db, flags);
    db_set_error(db, HAM_SUCCESS);
    db_set_dirty(db, HAM_TRUE);
    db_set_pagesize(db, pagesize);
    db_set_keysize(db, keysize);

    /* initialize the cache */
    cache=cache_new(db, 0, HAM_DEFAULT_CACHESIZE);
    if (!cache)
        return (db_get_error(db));
    db_set_cache(db, cache);

    if (!(flags&HAM_IN_MEMORY_DB)) {
        /* create the file */
        st=os_create(filename, flags, mode, &fd);
        if (st) {
            ham_log("os_open of %s failed with status %d (%s)", filename, 
                    st, ham_strerror(st));
            db_set_error(db, st);
            return (st);
        }

        /* write the database header */
        st=os_write(fd, db->_u._persistent, SIZEOF_PERS_HEADER);
        if (st) {
            ham_log("os_write of %s failed with status %d (%s)", filename, 
                    st, ham_strerror(st));
            db_set_error(db, st);
            return (st);
        }
        db_set_fd(db, fd);
    }

    /* create the backend */
    backend=db_create_backend(db, flags);
    if (!backend) {
        ham_log("unable to create backend with flags 0x%x", flags);
        db_set_error(db, HAM_INV_BACKEND);
        return (HAM_INV_BACKEND);
    }

    /* initialize the backend */
    st=backend->_fun_create(backend, flags);
    if (st) {
        db_set_error(db, st);
        return (st);
    }

    /* store the backend in the database */
    db_set_backend(db, backend);

    /* set the default key compare functions */
    ham_set_compare_func(db, db_default_compare);
    ham_set_prefix_compare_func(db, 0); /* TODO */

    return (HAM_SUCCESS);
}

ham_status_t
ham_get_error(ham_db_t *db)
{
    return (db_get_error(db));
}

ham_status_t
ham_set_prefix_compare_func(ham_db_t *db, ham_prefix_compare_func_t foo)
{
    db->_npers._prefixcompfoo=foo;
    return (HAM_SUCCESS);
}

ham_status_t
ham_set_compare_func(ham_db_t *db, ham_compare_func_t foo)
{
    db->_npers._compfoo=foo;
    return (HAM_SUCCESS);
}

ham_status_t
ham_find(ham_db_t *db, ham_txn_t *txn, ham_key_t *key, 
        ham_record_t *record, ham_u32_t flags)
{
    ham_txn_t dtxn;
    ham_status_t st;
    ham_backend_t *be=db_get_backend(db);

    if (!be)
        return (HAM_INV_BACKEND);
    if (!record)
        return (HAM_INV_PARAMETER);
    if (!txn) {
        txn=&dtxn;
        if ((st=ham_txn_begin(txn, db, TXN_READ_ONLY)))
            return (st);
    }

    /*
     * first look up the blob id, then fetch the blob
     */
    st=be->_fun_find(be, txn, key, record, flags);
    if (st==HAM_SUCCESS) {
        ham_bool_t noblob=HAM_FALSE;

        /*
         * success!
         *
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

        if (noblob && record->size>0) {
            if (!(record->flags & HAM_RECORD_USER_ALLOC)) {
                if (record->size>db_get_record_allocsize(db)) {
                    if (db_get_record_allocdata(db))
                        ham_mem_free(db_get_record_allocdata(db));
                    db_set_record_allocdata(db, ham_mem_alloc(record->size));
                    if (!db_get_record_allocdata(db)) {
                        st=HAM_OUT_OF_MEMORY;
                        db_set_record_allocsize(db, 0);
                    }
                    else {
                        db_set_record_allocsize(db, record->size);
                    }
                }
                record->data=db_get_record_allocdata(db);
            }

            if (!st) 
                memcpy(record->data, &record->_rid, record->size);
        }
        else 
            st=blob_read(db, txn, record->_rid, record, flags);
    }

    if (txn==&dtxn) {
        if (st) {
            (void)ham_txn_abort(txn, 0);
            return (st);
        }
        return (ham_txn_commit(txn, 0));
    }

    return (st);
}

ham_status_t
ham_insert(ham_db_t *db, ham_txn_t *txn, ham_key_t *key, 
        ham_record_t *record, ham_u32_t flags)
{
    ham_txn_t dtxn;
    ham_status_t st;
    ham_backend_t *be=db_get_backend(db);

    if (!be)
        return (HAM_INV_BACKEND);
    if (db_get_flags(db)&HAM_READ_ONLY)
        return (HAM_DB_READ_ONLY);
    if ((db_get_flags(db)&HAM_DISABLE_VAR_KEYLEN) && 
        key->size>db_get_keysize(db))
        return (HAM_INV_KEYSIZE);
    if ((db_get_keysize(db)<=sizeof(ham_offset_t)) && 
        key->size>db_get_keysize(db))
        return (HAM_INV_KEYSIZE);
    if (!txn) {
        txn=&dtxn;
        if ((st=ham_txn_begin(txn, db, 0)))
            return (st);
    }

    if (*(unsigned *)key->data==6332)
        printf("hit\n");

    /*
     * store the index entry; the backend will store the blob
     */
    st=be->_fun_insert(be, txn, key, record, flags);

    if (txn==&dtxn) {
        if (st) {
            (void)ham_txn_abort(txn, 0);
            return (st);
        }
        return (ham_txn_commit(txn, 0));
    }

    return (st);
}

ham_status_t
ham_erase(ham_db_t *db, ham_txn_t *txn, ham_key_t *key, ham_u32_t flags)
{
    ham_txn_t dtxn;
    ham_status_t st;
    ham_u32_t intflags=0;
    ham_offset_t blobid=0;
    ham_backend_t *be=db_get_backend(db);

    if (!be)
        return (HAM_INV_BACKEND);
    if (db_get_flags(db)&HAM_READ_ONLY)
        return (HAM_DB_READ_ONLY);
    if (!txn) {
        txn=&dtxn;
        if ((st=ham_txn_begin(txn, db, 0)))
            return (st);
    }

    /*
     * get rid of the index entry, then free the blob
     */
    st=be->_fun_erase(be, txn, key, &blobid, &intflags, flags);
    if (st==HAM_SUCCESS) {
        if (!((intflags&KEY_BLOB_SIZE_TINY) || 
              (intflags&KEY_BLOB_SIZE_SMALL) ||
              (intflags&KEY_BLOB_SIZE_EMPTY)))
            st=blob_free(db, txn, blobid, flags); 
    }

    if (txn==&dtxn) {
        if (st) {
            (void)ham_txn_abort(txn, 0);
            return (st);
        }
        return (ham_txn_commit(txn, 0));
    }

    return (st);
}
    
ham_status_t
ham_dump(ham_db_t *db, ham_txn_t *txn, ham_dump_cb_t cb)
{
    ham_txn_t dtxn;
    ham_status_t st;
    ham_backend_t *be=db_get_backend(db);

    if (!be)
        return (HAM_INV_BACKEND);
    if (!cb)
        cb=my_dump_cb;
    if (!txn) {
        txn=&dtxn;
        if ((st=ham_txn_begin(txn, db, TXN_READ_ONLY)))
            return (st);
    }

    /*
     * call the backend function
     */
    st=be->_fun_dump(be, txn, cb);

    if (txn==&dtxn) {
        if (st) {
            (void)ham_txn_abort(txn, 0);
            return (st);
        }
        return (ham_txn_commit(txn, 0));
    }

    return (st);
}

ham_status_t
ham_check_integrity(ham_db_t *db, ham_txn_t *txn)
{
    ham_txn_t dtxn;
    ham_status_t st;
    ham_backend_t *be=db_get_backend(db);

    if (!be)
        return (HAM_INV_BACKEND);
    if (!txn) {
        txn=&dtxn;
        if ((st=ham_txn_begin(txn, db, TXN_READ_ONLY)))
            return (st);
    }

    /*
     * call the backend function
     */
    st=be->_fun_check_integrity(be, txn);

    if (txn==&dtxn) {
        if (st) {
            (void)ham_txn_abort(txn, 0);
            return (st);
        }
        return (ham_txn_commit(txn, 0));
    }

    return (st);
}

ham_status_t
ham_close(ham_db_t *db)
{
    ham_status_t st=0;

    /*
     * flush all pages
     */
    st=db_flush_all(db, 0);
    if (st) {
        ham_log("db_flush_all() failed with status %d (%s)", 
                st, ham_strerror(st));
        /* fall through */
    }

    /* 
     * if we're not in read-only mode, and not an in-memory-database, 
     * and the dirty-flag is true: flush the page-header to disk 
     */
    if (!(db_get_flags(db)&HAM_IN_MEMORY_DB) && 
        db_is_open(db) && 
        (!(db_get_flags(db)&HAM_READ_ONLY)) && 
        db_is_dirty(db)) {

        /* move to the beginning of the file */
        st=os_seek(db_get_fd(db), 0, HAM_OS_SEEK_SET);
        if (st) {
            ham_log("os_seek failed with status %d (%s)", 
                    st, ham_strerror(st));
            /* fall through */
        }
        else {
            /* write the database header */
            st=os_write(db_get_fd(db), db->_u._persistent, SIZEOF_PERS_HEADER);
            if (st) {
                ham_log("os_write failed with status %d (%s)", 
                        st, ham_strerror(st));
                /* fall through */
            }
        }
    }

    /* close the backend */
    if (db_get_backend(db)) {
        st=db_get_backend(db)->_fun_close(db_get_backend(db));
        if (st) {
            ham_log("backend close() failed with status %d (%s)", 
                    st, ham_strerror(st));
        }
    }

    /* close the file */
    if (!(db_get_flags(db)&HAM_IN_MEMORY_DB) && 
        db_is_open(db)) {
        (void)os_close(db_get_fd(db));
        /* set an invalid database handle */
        db_set_fd(db, -1);
    }

    return (0);
}
