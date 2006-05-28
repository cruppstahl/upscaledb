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
#include "os.h"
#include "btree.h"
#include "version.h"
#include "cachemgr.h"
#include "txn.h"
#include "blob.h"

static int 
my_default_compare(const ham_u8_t *lhs, ham_size_t lhs_length, 
                   const ham_u8_t *rhs, ham_size_t rhs_length)
{
    int m;

    /* 
     * the default compare uses memcmp
     */
    if (lhs_length<rhs_length) {
        m=memcmp(lhs, rhs, lhs_length);
        if (m==0)
            return (-1);
        else
            return (0);
    }

    else if (rhs_length<lhs_length) {
        m=memcmp(lhs, rhs, rhs_length);
        if (m==0)
            return (1);
        else
            return (0);
    }

    return memcmp(lhs, rhs, lhs_length);
}

static ham_u8_t *
my_load_key(ham_db_t *db, ham_ext_key_t *extkey, const ham_u8_t *prefix, 
        ham_size_t prefix_length, ham_size_t real_length)
{
    ham_txn_t txn;
    ham_u8_t *ptr;
    ham_offset_t blobid;
    ham_record_t record;

    ham_assert(prefix_length>=sizeof(ham_offset_t), "invalid prefix length", 0);
    memset(&record, 0, sizeof(record));

    ptr=ham_mem_alloc(real_length);
    if (!ptr) {
        db_set_error(db, HAM_OUT_OF_MEMORY);
        return (0);
    }
    memcpy(ptr, prefix, prefix_length-sizeof(ham_offset_t));
    record.data =ptr+prefix_length-sizeof(ham_offset_t);
    record.flags=HAM_RECORD_USER_ALLOC;
    blobid=*(ham_offset_t *)&prefix[prefix_length-sizeof(ham_offset_t)];
    blobid=ham_db2h_offset(blobid);

    ham_txn_begin(&txn, db, TXN_READ_ONLY);
    if (blob_read(db, &txn, blobid, &record, 0)) {
        ham_txn_abort(&txn, 0);
        return (0);
    }
    ham_txn_commit(&txn, 0);

    ham_assert(real_length==record.size+(prefix_length-sizeof(ham_offset_t)), 
            "key length mismatch", 0);

    extkey->data=ptr;
    extkey->size=real_length;
    return (ptr);
}

static ham_backend_t *
my_create_backend(ham_db_t *db, ham_u32_t flags)
{
    ham_status_t st;
    ham_backend_t *be;

    if (flags &  HAM_BE_HASH) {
        /*
         * TODO
         */
        return (0);
    }

    /* 
     * the default backend is the BTREE
     */
    if (1) { /* flags & HAM_BE_BTREE */
        /*
         * note that we create a ham_backend_t with the size of a 
         * ham_btree_t!
         */
        be=(ham_backend_t *)ham_mem_alloc(sizeof(ham_btree_t));
        if (!be) {
            ham_log("out of memory", 0);
            return (0);
        }

        /* initialize the backend */
        st=btree_create((ham_btree_t *)be, db, flags);
        if (st) {
            ham_log("failed to initialize backend: 0x%s", st);
            return (0);
        }

        return (be);
    }

    return (0);
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

    /* dispose the cache manager */
    if (db->_npers._cm)
        cm_delete(db->_npers._cm);

    /* free all remaining memory */
    ham_mem_free(db);
    return (0);
}

ham_status_t
ham_open(ham_db_t *db, const char *filename, ham_u32_t flags)
{
    ham_fd_t fd;
    ham_status_t st;
    ham_backend_t *backend;
    ham_cachemgr_t *cm;

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
    db_set_error(db, HAM_SUCCESS);

    /* create the backend */
    backend=my_create_backend(db, flags);
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

    /* create a new cache manager */
    cm=cm_new(db, 0, HAM_DEFAULT_CACHESIZE);
    if (!cm)
        return (db_get_error(db));
    db_set_cm(db, cm);

    /* set the key compare function */
    ham_set_compare_func(db, my_default_compare);

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
    ham_backend_t *backend;
    ham_cachemgr_t *cm;

    /*
     * calculate a pagesize
     */
    if (pagesize==0) {
        pagesize=16*keysize;
        if (pagesize<1024)
            pagesize=HAM_DEFAULT_PAGESIZE;
        else
            pagesize-=(pagesize%1024);
    }

    ham_set_pagesize(db, pagesize);
    ham_set_keysize(db, keysize);

    /* create the file */
    st=os_create(filename, flags, mode, &fd);
    if (st) {
        ham_log("os_open of %s failed with status %d (%s)", filename, 
                st, ham_strerror(st));
        db_set_error(db, st);
        return (st);
    }
    
    db_set_magic(db, 'H', 'A', 'M', '\0');
    db_set_version(db, HAM_VERSION_MAJ, HAM_VERSION_MIN, HAM_VERSION_REV, 0);
    db_set_serialno(db, HAM_SERIALNO);
    db_set_fd(db, fd);
    db_set_flags(db, flags);
    db_set_error(db, HAM_SUCCESS);
    db_set_dirty(db, HAM_TRUE);

    /* create a new cache manager */
    cm=cm_new(db, 0, cachesize);
    if (!cm)
        return (db_get_error(db));
    db_set_cm(db, cm);

    /* write the database header */
    st=os_write(fd, db->_u._persistent, SIZEOF_PERS_HEADER);
    if (st) {
        ham_log("os_write of %s failed with status %d (%s)", filename, 
                st, ham_strerror(st));
        db_set_error(db, st);
        return (st);
    }

    /* create the backend */
    backend=my_create_backend(db, flags);
    if (!backend) {
        ham_log("unable to create backend with flags 0x%x", flags);
        db_set_error(db, HAM_INV_BACKEND);
        return (HAM_INV_BACKEND);
    }

    /* initialize the backend */
    st=backend->_fun_create(backend, flags);
    if (st) {
        ham_log("backend create() failed with status %d (%s)", 
                st, ham_strerror(st));
        db_set_error(db, st);
        return (st);
    }

    /* store the backend in the database */
    db_set_backend(db, backend);

    /* set the key compare function */
    ham_set_compare_func(db, my_default_compare);

    return (HAM_SUCCESS);
}

ham_status_t
ham_close(ham_db_t *db)
{
    ham_status_t st=0;

    /* 
     * if we're not in read-only mode, and the dirty-flag is true:
     * flush the page-header to disk 
     */
    if (ham_is_open(db) && (!(ham_get_flags(db)&HAM_READ_ONLY)) && 
            db_is_dirty(db)) {

        /* move to the beginning of the file */
        st=os_seek(db_get_fd(db), 0, HAM_OS_SEEK_SET);
        if (st) {
            ham_log("os_seek failed with status %d (%s)", 
                    st, ham_strerror(st));
        }
        else {
            /* write the database header */
            st=os_write(db_get_fd(db), db->_u._persistent, SIZEOF_PERS_HEADER);
            if (st) {
                ham_log("os_write failed with status %d (%s)", 
                        st, ham_strerror(st));
            }
        }
    }

    /* flush the whole cache */
    if (db_get_cm(db)) {
        st=db_flush_all(db, 0);
        if (st) {
            ham_log("flushing the caches failed with status %d (%s)", 
                    st, ham_strerror(st));
            db_set_error(db, st);
            return (st);
        }
    }

    /* close the backend */
    if (db_get_backend(db)) {
        st=db_get_backend(db)->_fun_close(db_get_backend(db));
        if (st) {
            ham_log("backend close() failed with status %d (%s)", 
                    st, ham_strerror(st));
            db_set_error(db, st);
            return (st);
        }
    }

    /* close the file */
    if (ham_is_open(db)) {
        (void)os_close(db_get_fd(db));
        /* set an invalid database handle */
        db_set_fd(db, -1);
    }

    return (0);
}

ham_u16_t 
ham_get_keysize(ham_db_t *db)
{
    return (db_get_keysize(db));
}

ham_status_t
ham_set_keysize(ham_db_t *db, ham_u16_t size)
{
    if (ham_is_open(db)) {
        db_set_error(db, HAM_DB_ALREADY_OPEN);
        return (HAM_DB_ALREADY_OPEN);
    }
    db->_u._pers._keysize=ham_h2db16(size);
    return (0);
}

ham_u16_t
ham_get_pagesize(ham_db_t *db)
{
    return (db_get_pagesize(db));
}

ham_u16_t
ham_set_pagesize(ham_db_t *db, ham_u16_t size)
{
    if (ham_is_open(db)) {
        db_set_error(db, HAM_DB_ALREADY_OPEN);
        return (HAM_DB_ALREADY_OPEN);
    }
    db->_u._pers._pagesize=ham_h2db16(size);
    return (0);
}

ham_u32_t
ham_get_flags(ham_db_t *db)
{
    return (ham_db2h32(db->_u._pers._flags));
}

ham_status_t
ham_get_error(ham_db_t *db)
{
    return (db_get_error(db));
}

ham_compare_func_t
ham_get_compare_func(ham_db_t *db)
{
    return (db->_npers._compfoo);
}

ham_status_t
ham_set_compare_func(ham_db_t *db, ham_compare_func_t foo)
{
    db->_npers._compfoo=foo;
    return (0);
}

ham_status_t
ham_set_prefix_compare_func(ham_db_t *db, ham_prefix_compare_func_t foo)
{
    db->_npers._prefixcompfoo=foo;
    return (0);
}

ham_bool_t
ham_is_open(ham_db_t *db)
{
    return (db_get_backend(db)!=0 && db_get_fd(db)!=-1);
}

ham_offset_t
db_ext_key_insert(ham_db_t *db, ham_txn_t *txn, ham_page_t *page, 
        ham_key_t *key)
{
    ham_status_t st;
    ham_offset_t blobid;
    ham_u8_t *data=(ham_u8_t *)key->data;
    ham_size_t size=key->size;
    ham_ext_key_t *ext;

    if (!page_get_extkeys(page)) {
        ext=(ham_ext_key_t *)ham_mem_alloc(db_get_maxkeys(db)*
                sizeof(ham_ext_key_t));
        if (!ext) {
            db_set_error(db, HAM_OUT_OF_MEMORY);
            return (0);
        }
        memset(ext, 0, db_get_maxkeys(db)*sizeof(ham_ext_key_t));
        page_set_extkeys(page, ext);
    }

    data+=db_get_keysize(db)-sizeof(ham_offset_t);
    size-=db_get_keysize(db)-sizeof(ham_offset_t);

    st=blob_allocate(db, txn, data, size, 0, &blobid);
    if (st) {
        db_set_error(db, st);
        return (0);
    }
    return (blobid);
}

ham_status_t
db_ext_key_erase(ham_db_t *db, ham_txn_t *txn, ham_offset_t blobid)
{
    ham_status_t st;
    st=blob_free(db, txn, blobid, 0);
    if (st)
        db_set_error(db, st);
    return (st);
}

int
db_compare_keys(ham_db_t *db, ham_page_t *page,
                long lhs_idx, ham_u32_t lhs_flags, const ham_u8_t *lhs, 
                ham_size_t lhs_length, ham_size_t lhs_real_length, 
                long rhs_idx, ham_u32_t rhs_flags, const ham_u8_t *rhs, 
                ham_size_t rhs_length, ham_size_t rhs_real_length)
{
    int cmp=0;
    ham_ext_key_t *ext;
    ham_size_t lhsprefixlen, rhsprefixlen;
    ham_compare_func_t foo=db_get_compare_func(db);
    ham_prefix_compare_func_t prefoo=db_get_prefix_compare_func(db);
    db_set_error(db, 0);

    /*
     * need prefix compare? 
     */
    if (lhs_length==lhs_real_length && rhs_length==rhs_real_length) {
        /*
         * no!
         */
        return (foo(lhs, lhs_length, rhs, rhs_length));
    }

    /*
     * yes! - run prefix comparison, but only if we have a prefix
     * comparison function
     */
    if (prefoo) {
        lhsprefixlen=lhs_length==lhs_real_length 
                ? lhs_length 
                : lhs_length-sizeof(ham_offset_t);
        rhsprefixlen=rhs_length==rhs_real_length 
                ? rhs_length 
                : rhs_length-sizeof(ham_offset_t);
        cmp=prefoo(lhs, lhsprefixlen, lhs_real_length, 
            rhs, rhsprefixlen, rhs_real_length);
        if (db_get_error(db))
            return (0);
    }
    if (!prefoo || cmp==HAM_PREFIX_REQUEST_FULLKEY) {
        /*
         * load the full key
         * 1. check if an extkeys-array is loaded (if not, allocate memory)
         */
        if (!page_get_extkeys(page)) {
            ext=(ham_ext_key_t *)ham_mem_alloc(db_get_maxkeys(db)*
                    sizeof(ham_ext_key_t));
            if (!ext) {
                db_set_error(db, HAM_OUT_OF_MEMORY);
                return (0);
            }
            memset(ext, 0, db_get_maxkeys(db)*sizeof(ham_ext_key_t));
            page_set_extkeys(page, ext);
        }

        /*
         * 2. make sure that both keys are loaded; if not, load them from 
         * disk
         */
        ext=page_get_extkeys(page);
        if (lhs_length!=lhs_real_length && ext[lhs_idx].data==0) {
            ham_assert(lhs_idx!=-1, "invalid rhs_index -1", 0);
            if (!(lhs=my_load_key(db, &ext[lhs_idx], lhs, lhs_length,
                            lhs_real_length)))
                return (0);
            lhs_length=lhs_real_length;
        }
        if (rhs_length!=rhs_real_length && ext[rhs_idx].data==0) {
            ham_assert(rhs_idx!=-1, "invalid rhs_index -1", 0);
            if (!(rhs=my_load_key(db, &ext[rhs_idx], rhs, rhs_length, 
                            rhs_real_length)))
                return (0);
            rhs_length=rhs_real_length;
        }

        /*
         * 3. run the comparison function
         */
        return (foo(lhs, lhs_length, rhs, rhs_length));
    }
    return (cmp);
}

ham_status_t 
db_flush_all(ham_db_t *db, ham_u32_t flags)
{
    return (cm_flush_all(db->_npers._cm, flags));
}

ham_status_t
ham_find(ham_db_t *db, ham_key_t *key, ham_record_t *record, ham_u32_t flags)
{
    ham_txn_t txn;
    ham_status_t st;
    ham_backend_t *be=db_get_backend(db);
    ham_offset_t blobid;

    if (!be)
        return (HAM_INV_BACKEND);
    if ((st=ham_txn_begin(&txn, db, TXN_READ_ONLY)))
        return (st);

    /*
     * first look up the blob id
     */
    st=be->_fun_find(be, &txn, key, &blobid, flags);
    if (st) {
        (void)ham_txn_abort(&txn, 0);
        return (st);
    }

    /*
     * then fetch the blob
     */
    st=blob_read(db, &txn, blobid, record, flags);
    if (st) {
        (void)ham_txn_abort(&txn, 0);
        return (st);
    }

    return (ham_txn_commit(&txn, 0));
}

ham_status_t
ham_insert(ham_db_t *db, ham_key_t *key, ham_record_t *record, ham_u32_t flags)
{
    ham_txn_t txn;
    ham_status_t st;
    ham_backend_t *be=db_get_backend(db);

    if (!be)
        return (HAM_INV_BACKEND);
    if (ham_get_flags(db)&HAM_READ_ONLY)
        return (HAM_DB_READ_ONLY);
    if ((ham_get_flags(db)&HAM_DISABLE_VAR_KEYLEN) && 
        key->size>db_get_keysize(db))
        return (HAM_INV_KEYSIZE);
    if ((db_get_keysize(db)<=sizeof(ham_offset_t)) && 
        key->size>db_get_keysize(db))
        return (HAM_INV_KEYSIZE);

    if ((st=ham_txn_begin(&txn, db, 0)))
        return (st);

    /*
     * store the index entry; the backend will store the blob
     */
    st=be->_fun_insert(be, &txn, key, record, flags);
    if (st) {
        (void)ham_txn_abort(&txn, 0);
        return (st);
    }

    return (ham_txn_commit(&txn, 0));
}

ham_status_t
ham_erase(ham_db_t *db, ham_key_t *key, ham_u32_t flags)
{
    ham_txn_t txn;
    ham_status_t st;
    ham_backend_t *be=db_get_backend(db);
    ham_offset_t blobid;

    if (!be)
        return (HAM_INV_BACKEND);
    if (ham_get_flags(db)&HAM_READ_ONLY)
        return (HAM_DB_READ_ONLY);
    if ((st=ham_txn_begin(&txn, db, 0)))
        return (st);

    /*
     * get rid of the index entry
     */
    st=be->_fun_erase(be, &txn, key, &blobid, flags);
    if (st) {
        (void)ham_txn_abort(&txn, 0);
        return (st);
    }

    /*
     * now free the blob
     */
    st=blob_free(db, &txn, blobid, flags);
    if (st) {
        (void)ham_txn_abort(&txn, 0);
        return (st);
    }

    return (ham_txn_commit(&txn, 0));
}

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
    
ham_status_t
ham_dump(ham_db_t *db, ham_dump_cb_t cb)
{
    ham_txn_t txn;
    ham_status_t st;
    ham_backend_t *be=db_get_backend(db);

    if (!be)
        return (HAM_INV_BACKEND);
    if (!cb)
        cb=my_dump_cb;

    if ((st=ham_txn_begin(&txn, db, TXN_READ_ONLY)))
        return (st);

    st=be->_fun_dump(be, &txn, cb);
    if (st) {
        (void)ham_txn_abort(&txn, 0);
        return (st);
    }

    return (ham_txn_commit(&txn, 0));
}

ham_status_t
ham_check_integrity(ham_db_t *db)
{
    ham_txn_t txn;
    ham_status_t st;

    ham_backend_t *be=db_get_backend(db);
    if (!be)
        return (HAM_INV_BACKEND);
    if ((st=ham_txn_begin(&txn, db, TXN_READ_ONLY)))
        return (st);

    st=be->_fun_check_integrity(be, &txn);
    if (st) {
        (void)ham_txn_abort(&txn, 0);
        return (st);
    }

    return (ham_txn_commit(&txn, 0));
}
