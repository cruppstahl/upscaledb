/**
 * Copyright (C) 2005-2007 Christoph Rupp (chris@crupp.de).
 * All rights reserved. See file LICENSE for licence and copyright
 * information.
 *
 */

#ifdef HAVE_MALLOC_H
#  include <malloc.h>
#else
#  include <stdlib.h>
#endif
#include <string.h>
#include <ham/hamsterdb.h>
#include "config.h"
#include "error.h"
#include "mem.h"
#include "db.h"
#include "version.h"
#include "txn.h"
#include "cache.h"
#include "blob.h"
#include "freelist.h"
#include "extkeys.h"
#include "btree_cursor.h"
#include "cursor.h"
#include "util.h"
#include "keys.h"

typedef struct free_cb_context_t
{
    ham_db_t *db;

    ham_bool_t is_leaf;

} free_cb_context_t;

/*
 * callback function for ham_dump
 */
#ifdef HAM_ENABLE_INTERNAL
static void
my_dump_cb(int event, void *param1, void *param2, void *context)
{
    ham_size_t i, limit, keysize;
    ham_page_t *page;
    int_key_t *key;
    ham_u8_t *data;
    ham_dump_cb_t cb=(ham_dump_cb_t)context;

    switch (event) {
    case ENUM_EVENT_DESCEND:
        break;

    case ENUM_EVENT_PAGE_START:
        page=(ham_page_t *)param1;
        printf("\n------ page 0x%llx ---------------------------------------\n",
				(ham_u64_t)page_get_self(page));
        break;

    case ENUM_EVENT_ITEM:
        key=(int_key_t *)param1;
        data=key_get_key(key);
        keysize=key_get_size(key);

        if (cb) {
            cb(data, keysize);
        }
        else {
            printf(" %02d: ", *(int *)param2);
            printf(" key (%2d byte): ", key_get_size(key));

            if (keysize>16)
                limit=16;
            else
                limit=keysize;

            for (i=0; i<limit; i++)
                printf("%02x ", data[i]);

            if (keysize>limit)
                printf("... (%d more bytes)\n", keysize-limit);
            else
                printf("\n");

            printf("      ptr: 0x%llx\n",
                    (ham_u64_t)key_get_ptr(key));
        }
        break;

    default:
        ham_assert(!"unknown callback event", (""));
        break;
    }
}
#endif /* HAM_ENABLE_INTERNAL */

/*
 * callback function for freeing blobs of an in-memory-database
 */
static void
my_free_cb(int event, void *param1, void *param2, void *context)
{
    int_key_t *key;
    free_cb_context_t *c;

    c=(free_cb_context_t *)context;

    switch (event) {
    case ENUM_EVENT_DESCEND:
        break;

    case ENUM_EVENT_PAGE_START:
        c->is_leaf=*(ham_bool_t *)param2;
        break;

    case ENUM_EVENT_ITEM:
        key=(int_key_t *)param1;

        if (key_get_flags(key)&KEY_BLOB_SIZE_TINY ||
            key_get_flags(key)&KEY_BLOB_SIZE_SMALL ||
            key_get_flags(key)&KEY_BLOB_SIZE_EMPTY)
            break;

        if (c->is_leaf)
            ham_mem_free(c->db, (void *)key_get_ptr(key));
        break;

    default:
        ham_assert(!"unknown callback event", (""));
        break;
    }
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
        case HAM_INV_INDEX:
            return ("Invalid index structure");
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
            return ("Database opened in read-only mode");
        case HAM_BLOB_NOT_FOUND:
            return ("Data blob not found");
        case HAM_PREFIX_REQUEST_FULLKEY:
            return ("Comparator function needs more data");
        case HAM_IO_ERROR:
            return ("System I/O error");
        case HAM_CACHE_FULL:
            return ("Database cache is full");
        case HAM_FILE_NOT_FOUND:
            return ("File not found");
        case HAM_CURSOR_IS_NIL:
            return ("Cursor points to NIL");
        default:
            return ("Unknown error");
    }
}

void
ham_get_version(ham_u32_t *major, ham_u32_t *minor,
                ham_u32_t *revision)
{
    if (major)
        *major=HAM_VERSION_MAJ;
    if (minor)
        *minor=HAM_VERSION_MIN;
    if (revision)
        *revision=HAM_VERSION_REV;
}

ham_status_t
ham_new(ham_db_t **db)
{
    /* allocate memory for the ham_db_t-structure;
     * we can't use our allocator because it's not yet created! */
    *db=(ham_db_t *)malloc(sizeof(ham_db_t));
    if (!(*db))
        return (HAM_OUT_OF_MEMORY);

    /* reset the whole structure */
    memset(*db, 0, sizeof(ham_db_t));

    return (0);
}

ham_status_t
ham_delete(ham_db_t *db)
{
    /* free cached data pointers */
    if (db_get_record_allocdata(db))
        ham_mem_free(db, db_get_record_allocdata(db));
    if (db_get_key_allocdata(db))
        ham_mem_free(db, db_get_key_allocdata(db));

    /* close the allocator */
    db_get_allocator(db)->close(db_get_allocator(db));
    memset(db_get_allocator(db), 0, sizeof(mem_allocator_t));

    /* "free" all remaining memory */
    free(db);

    return (0);
}

ham_status_t
ham_open(ham_db_t *db, const char *filename, ham_u32_t flags)
{
    return (ham_open_ex(db, filename, flags, HAM_DEFAULT_CACHESIZE));
}

ham_status_t
ham_open_ex(ham_db_t *db, const char *filename,
        ham_u32_t flags, ham_size_t cachesize)
{
    ham_status_t st;
    ham_cache_t *cache;
    ham_backend_t *backend;
    ham_u8_t hdrbuf[512];
    db_header_t *dbhdr;
    ham_page_t *page;
    ham_device_t *device;

    if (!db)
        return (HAM_INV_PARAMETER);

    db_set_error(db, 0);

    /* cannot open an in-memory-db */
    if (flags&HAM_IN_MEMORY_DB)
        return (db_set_error(db, HAM_INV_PARAMETER));

    /* if we do not yet have an allocator: create a new one */
    if (!db_get_allocator(db)->alloc) {
        db_set_allocator(db, ham_default_allocator_new());
        if (!db_get_allocator(db)->alloc)
            return (db_set_error(db, HAM_OUT_OF_MEMORY));
    }

    /* initialize the device */
    device=ham_device_new(db, flags&HAM_IN_MEMORY_DB);
    if (!device)
        return (db_get_error(db));
    /* open the file */
    st=device->open(device, filename, flags);
    if (st)
        return (db_set_error(db, st));
    db_set_device(db, device);

    /*
     * read the database header
     *
     * !!!
     * now this is an ugly problem - the database header is one page, but
     * how large is one page? chances are good that it's the default
     * page-size, but we really can't be sure.
     *
     * read 512 byte (the minimum page size) and
     * extract the "real" page size, then read the real page.
     * (but i really don't like this)
     */
    st=device->read(device, 0, hdrbuf, sizeof(hdrbuf));
    if (st) {
        ham_log(("os_pread of %s failed with status %d (%s)", filename,
                st, ham_strerror(st)));
        return (db_set_error(db, st));
    }
    dbhdr=(db_header_t *)&hdrbuf[12];
    db_set_pagesize(db, ham_db2h32(dbhdr->_pagesize));

    /*
     * can we use mmap?
     */
#if HAVE_MMAP
    if (!(flags&HAM_DISABLE_MMAP)) {
        if (db_get_pagesize(db)%os_get_pagesize()==0)
            flags|=DB_USE_MMAP;
        else
            device->set_flags(device, DEVICE_NO_MMAP);
    }
    flags&=~HAM_DISABLE_MMAP; /* don't store this flag */
#else
    device->set_flags(device, DEVICE_NO_MMAP);
#endif

    db_set_error(db, HAM_SUCCESS);

    /*
     * now allocate and read the header page
     */
    page=page_new(db);
    if (!page)
        return (db_get_error(db));
    st=page_fetch(page);
    if (st)
        return (st);
    page_set_type(page, PAGE_TYPE_HEADER);
    db_set_header_page(db, page);

    /*
     * copy the persistent header to the database object
     */
    memcpy(&db_get_header(db), page_get_payload(page),
            sizeof(db_header_t)-sizeof(freel_payload_t));

    /* set the database flags */
    db_set_rt_flags(db, flags|db_get_pers_flags(db));
    ham_assert(!(db_get_pers_flags(db)&HAM_DISABLE_VAR_KEYLEN), 
            ("invalid persistent database flags 0x%x", db_get_pers_flags(db)));
    ham_assert(!(db_get_pers_flags(db)&HAM_CACHE_STRICT), 
            ("invalid persistent database flags 0x%x", db_get_pers_flags(db)));
    ham_assert(!(db_get_pers_flags(db)&HAM_DISABLE_MMAP), 
            ("invalid persistent database flags 0x%x", db_get_pers_flags(db)));
    ham_assert(!(db_get_pers_flags(db)&HAM_WRITE_THROUGH), 
            ("invalid persistent database flags 0x%x", db_get_pers_flags(db)));
    ham_assert(!(db_get_pers_flags(db)&HAM_READ_ONLY), 
            ("invalid persistent database flags 0x%x", db_get_pers_flags(db)));
    ham_assert(!(db_get_pers_flags(db)&HAM_OPTIMIZE_SIZE), 
            ("invalid persistent database flags 0x%x", db_get_pers_flags(db)));
    ham_assert(!(db_get_pers_flags(db)&HAM_DISABLE_FREELIST_FLUSH), 
            ("invalid persistent database flags 0x%x", db_get_pers_flags(db)));
    ham_assert(!(db_get_pers_flags(db)&DB_USE_MMAP), 
            ("invalid persistent database flags 0x%x", db_get_pers_flags(db)));

    /* check the file magic */
    if (db_get_magic(db, 0)!='H' ||
        db_get_magic(db, 1)!='A' ||
        db_get_magic(db, 2)!='M' ||
        db_get_magic(db, 3)!='\0') {
        ham_log(("invalid file type - %s is not a hamster-db", filename));
        db_set_error(db, HAM_INV_FILE_HEADER);
        return (HAM_INV_FILE_HEADER);
    }

    /* check the database version */
    if (db_get_version(db, 0)!=HAM_VERSION_MAJ ||
        db_get_version(db, 1)!=HAM_VERSION_MIN) {
        ham_log(("invalid file version"));
        db_set_error(db, HAM_INV_FILE_VERSION);
        return (HAM_INV_FILE_VERSION);
    }

    /* create the backend */
    backend=db_create_backend(db, flags);
    if (!backend) {
        ham_log(("unable to create backend with flags 0x%x", flags));
        db_set_error(db, HAM_INV_INDEX);
        return (HAM_INV_INDEX);
    }
    db_set_backend(db, backend);

    /* initialize the backend */
    st=backend->_fun_open(backend, flags);
    if (st) {
        ham_log(("backend create() failed with status %d (%s)",
                st, ham_strerror(st)));
        db_set_error(db, st);
        return (st);
    }

    /* initialize the cache */
    if (cachesize==0)
        cachesize=HAM_DEFAULT_CACHESIZE; 
    cache=cache_new(db, cachesize/db_get_pagesize(db));
    if (!cache)
        return (db_get_error(db));
    db_set_cache(db, cache);

    /* create the freelist - not needed for in-memory-databases */
    if (!(flags&HAM_IN_MEMORY_DB)) {
        st=freel_create(db);
        if (st) {
            ham_log(("unable to create freelist"));
            return (st);
        }
    }

    /* set the key compare function */
    ham_set_compare_func(db, db_default_compare);
    ham_set_prefix_compare_func(db, db_default_prefix_compare);

    return (HAM_SUCCESS);
}

ham_status_t
ham_create(ham_db_t *db, const char *filename, ham_u32_t flags, ham_u32_t mode)
{
    return (ham_create_ex(db, filename, flags, mode, 0, 0, 0));
}

ham_status_t
ham_create_ex(ham_db_t *db, const char *filename,
        ham_u32_t flags, ham_u32_t mode, ham_u32_t pagesize,
        ham_u16_t keysize, ham_size_t cachesize)
{
    ham_status_t st;
    ham_cache_t *cache;
    ham_backend_t *backend;
    db_header_t *h;
    ham_page_t *page;
    ham_u32_t pflags;
    ham_device_t *device;

    if (!db)
        return (HAM_INV_PARAMETER);

    db_set_error(db, 0);

    /*
     * make sure that the pagesize is aligned to 512k
     */
    if (pagesize) {
        if (pagesize%512)
            return (db_set_error(db, HAM_INV_PAGESIZE));
    }

    /*
     * in-memory-db? don't allow cache limits!
     */
    if (flags&HAM_IN_MEMORY_DB) {
        if ((flags&HAM_CACHE_STRICT) || cachesize!=0)
            return (db_set_error(db, HAM_INV_PARAMETER));
    }

    /*
     * in-memory-db? use the default pagesize of the system
     */
    if (flags&HAM_IN_MEMORY_DB) {
        if (!pagesize) {
            pagesize=os_get_pagesize();
            if (!pagesize) {
                pagesize=1024*4;
                flags|=HAM_DISABLE_MMAP;
            }
        }
    }

    /* initialize the device */
    device=ham_device_new(db, flags&HAM_IN_MEMORY_DB);
    if (!device)
        return (db_get_error(db));

    /*
     * can we use mmap?
     */
#if HAVE_MMAP
    else if (!(flags&HAM_DISABLE_MMAP)) {
        if (pagesize) {
            if (pagesize%os_get_pagesize()==0)
                flags|=DB_USE_MMAP;
            else
                device->set_flags(device, DEVICE_NO_MMAP);
        }
        else {
            pagesize=os_get_pagesize();
            if (!pagesize) {
                pagesize=1024*4;
                flags|=HAM_DISABLE_MMAP;
                device->set_flags(device, DEVICE_NO_MMAP);
            }
            else
                flags|=DB_USE_MMAP;
        }
        flags&=~HAM_DISABLE_MMAP; /* don't store this flag */
    }
#else
    device->set_flags(device, DEVICE_NO_MMAP);
#endif

    /*
     * if we still don't have a pagesize, try to get a good default value
     */
    if (!pagesize) {
        pagesize=os_get_pagesize();
        if (!pagesize) {
            pagesize=1024*4;
            flags&=~DB_USE_MMAP;
        }
    }

    /*
     * make sure that the pagesize is big enough for at least 5 keys
     */
    if (keysize)
        if (pagesize/keysize<5)
            return (db_set_error(db, HAM_INV_KEYSIZE));

    /*
     * initialize the database with a good default value;
     * 32byte is the size of a first level cache line for most modern
     * processors; adjust the keysize, so the keys are aligned to
     * 32byte
     */
    if (keysize==0)
        keysize=32-(sizeof(int_key_t)-1);

    /* if we do not yet have an allocator: create a new one */
    if (!db_get_allocator(db)->alloc) {
        db_set_allocator(db, ham_default_allocator_new());
        if (!db_get_allocator(db)->alloc)
            return (db_set_error(db, HAM_OUT_OF_MEMORY));
    }

    /*
     * initialize the header
     */
    db_set_magic(db, 'H', 'A', 'M', '\0');
    db_set_version(db, HAM_VERSION_MAJ, HAM_VERSION_MIN, HAM_VERSION_REV, 0);
    db_set_serialno(db, HAM_SERIALNO);
    db_set_error(db, HAM_SUCCESS);
    db_set_pagesize(db, pagesize);
    db_set_keysize(db, keysize);

    /*
     * set the flags
     */
    pflags=flags;
    pflags&=~HAM_DISABLE_VAR_KEYLEN;
    pflags&=~HAM_CACHE_STRICT;
    pflags&=~HAM_DISABLE_MMAP;
    pflags&=~HAM_WRITE_THROUGH;
    pflags&=~HAM_READ_ONLY;
    pflags&=~HAM_OPTIMIZE_SIZE;
    pflags&=~HAM_DISABLE_FREELIST_FLUSH;
    pflags&=~DB_USE_MMAP;
    db_set_pers_flags(db, pflags);
    db_set_rt_flags(db, flags);

    /* initialize the cache */
    if (cachesize==0)
        cachesize=HAM_DEFAULT_CACHESIZE;
    cache=cache_new(db, cachesize/db_get_pagesize(db));
    if (!cache)
        return (db_get_error(db));
    db_set_cache(db, cache);

    /* create the file */
    st=device->create(device, filename, flags, mode);
    if (st)
        return (db_set_error(db, st));
    db_set_device(db, device);

    /* allocate a database header page */
    page=page_new(db);
    if (!page) {
        ham_log(("unable to allocate the header page"));
        return (db_get_error(db));
    }
    st=page_alloc(page);
    if (st)
        return (st);
    memset(page_get_pers(page), 0, pagesize);
    page_set_type(page, PAGE_TYPE_HEADER);
    db_set_header_page(db, page);
    page_set_dirty(page, 1);

    /* initialize the freelist structure in the header page */
    h=(db_header_t *)page_get_payload(page);
    freel_payload_set_maxsize(&h->_freelist,
            (db_get_usable_pagesize(db)-sizeof(db_header_t))/
            sizeof(freel_entry_t));

    /* create the backend */
    backend=db_create_backend(db, flags);
    if (!backend) {
        ham_log(("unable to create backend with flags 0x%x", flags));
        db_set_error(db, HAM_INV_INDEX);
        return (HAM_INV_INDEX);
    }

    /* initialize the backend */
    st=backend->_fun_create(backend, flags);
    if (st) {
        ham_log(("unable to create the backend"));
        db_set_error(db, st);
        return (st);
    }

    /* store the backend in the database */
    db_set_backend(db, backend);

    /* create the freelist - not needed for in-memory-databases */
    if (!(flags&HAM_IN_MEMORY_DB)) {
        st=freel_create(db);
        if (st) {
            ham_log(("unable to create freelist"));
            return (st);
        }
    }

    /* set the default key compare functions */
    ham_set_compare_func(db, db_default_compare);
    ham_set_prefix_compare_func(db, db_default_prefix_compare);
    db_set_dirty(db, HAM_TRUE);

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
    if (!db)
        return (HAM_INV_PARAMETER);

    db_set_error(db, 0);
    db_set_prefix_compare_func(db, foo);

    return (HAM_SUCCESS);
}

ham_status_t
ham_set_compare_func(ham_db_t *db, ham_compare_func_t foo)
{
    if (!db)
        return (HAM_INV_PARAMETER);

    db_set_error(db, 0);
    db_set_compare_func(db, foo);

    return (HAM_SUCCESS);
}

ham_status_t
ham_find(ham_db_t *db, void *reserved, ham_key_t *key,
        ham_record_t *record, ham_u32_t flags)
{
    ham_txn_t txn;
    ham_status_t st;
    ham_backend_t *be;

    if (!db || !key || !record)
        return (HAM_INV_PARAMETER);

    db_set_error(db, 0);

    be=db_get_backend(db);
    if (!be)
        return (db_set_error(db, HAM_INV_INDEX));

    if ((st=ham_txn_begin(&txn, db)))
        return (st);

    /*
     * first look up the blob id, then fetch the blob
     */
    st=be->_fun_find(be, key, record, flags);
    if (st==HAM_SUCCESS)
        st=util_read_record(db, record, flags);

    if (st) {
        (void)ham_txn_abort(&txn);
        return (st);
    }

    return (ham_txn_commit(&txn));
}

ham_status_t
ham_insert(ham_db_t *db, void *reserved, ham_key_t *key,
        ham_record_t *record, ham_u32_t flags)
{
    ham_txn_t txn;
    ham_status_t st;
    ham_backend_t *be;

    if (!db || !key || !record)
        return (HAM_INV_PARAMETER);

    db_set_error(db, 0);

    be=db_get_backend(db);
    if (!be)
        return (db_set_error(db, HAM_INV_INDEX));
    if (db_get_rt_flags(db)&HAM_READ_ONLY)
        return (db_set_error(db, HAM_DB_READ_ONLY));
    if ((db_get_rt_flags(db)&HAM_DISABLE_VAR_KEYLEN) &&
        (key->size>db_get_keysize(db)))
        return (db_set_error(db, HAM_INV_KEYSIZE));
    if ((db_get_keysize(db)<sizeof(ham_offset_t)) &&
        (key->size>db_get_keysize(db)))
        return (db_set_error(db, HAM_INV_KEYSIZE));
    if ((st=ham_txn_begin(&txn, db)))
        return (st);

    /*
     * store the index entry; the backend will store the blob
     */
    st=be->_fun_insert(be, key, record, flags);

    if (st) {
#if 0
        (void)ham_txn_abort(&txn);
#endif
        (void)ham_txn_commit(&txn);
        return (st);
    }

    return (ham_txn_commit(&txn));
}

ham_status_t
ham_erase(ham_db_t *db, void *reserved, ham_key_t *key, ham_u32_t flags)
{
    ham_txn_t txn;
    ham_status_t st;
    ham_u32_t intflags=0;
    ham_offset_t blobid=0;
    ham_backend_t *be;

    if (!db || !key)
        return (HAM_INV_PARAMETER);

    db_set_error(db, 0);

    be=db_get_backend(db);
    if (!be)
        return (db_set_error(db, HAM_INV_INDEX));
    if (db_get_rt_flags(db)&HAM_READ_ONLY)
        return (db_set_error(db, HAM_DB_READ_ONLY));
    if ((st=ham_txn_begin(&txn, db)))
        return (st);

    /*
     * get rid of the index entry, then free the blob
     */
    st=be->_fun_erase(be, key, &blobid, &intflags, flags);
    if (st==HAM_SUCCESS) {
        if (!((intflags&KEY_BLOB_SIZE_TINY) ||
              (intflags&KEY_BLOB_SIZE_SMALL) ||
              (intflags&KEY_BLOB_SIZE_EMPTY)))
            st=blob_free(db, blobid, flags);
    }

    if (st) {
#if 0
        (void)ham_txn_abort(&txn);
#endif
        (void)ham_txn_commit(&txn);
        return (st);
    }

    return (ham_txn_commit(&txn));
}

ham_status_t
ham_dump(ham_db_t *db, void *reserved, ham_dump_cb_t cb)
{
#ifdef HAM_ENABLE_INTERNAL
    ham_txn_t txn;
    ham_status_t st;
    ham_backend_t *be;

    if (!db || !cb)
        return (HAM_INV_PARAMETER);

    db_set_error(db, 0);

    be=db_get_backend(db);
    if (!be)
        return (db_set_error(db, HAM_INV_INDEX));
    if ((st=ham_txn_begin(&txn, db)))
        return (st);

    /*
     * call the backend function
     */
    st=be->_fun_enumerate(be, my_dump_cb, cb);

    if (st) {
        (void)ham_txn_abort(&txn);
        return (st);
    }

    return (ham_txn_commit(&txn));
#else /* !HAM_ENABLE_INTERNAL */
    return (HAM_NOT_IMPLEMENTED);
#endif
}

ham_status_t
ham_check_integrity(ham_db_t *db, void *reserved)
{
#ifdef HAM_ENABLE_INTERNAL
    ham_txn_t txn;
    ham_status_t st;
    ham_backend_t *be;

    if (!db)
        return (HAM_INV_PARAMETER);

    db_set_error(db, 0);

    /*
     * check the cache integrity
     */
    st=cache_check_integrity(db_get_cache(db));
    if (st)
        return (st);

    be=db_get_backend(db);
    if (!be)
        return (db_set_error(db, HAM_INV_INDEX));
    if ((st=ham_txn_begin(&txn, db)))
        return (st);

    /*
     * call the backend function
     */
    st=be->_fun_check_integrity(be);

    if (st) {
        (void)ham_txn_abort(&txn);
        return (st);
    }

    return (ham_txn_commit(&txn));
#else /* !HAM_ENABLE_INTERNAL */
    return (HAM_NOT_IMPLEMENTED);
#endif
}

ham_status_t
ham_flush(ham_db_t *db, ham_u32_t flags)
{
    ham_status_t st;

    (void)flags;

    if (!db)
        return (HAM_INV_PARAMETER);

    db_set_error(db, 0);

    /*
     * never flush an in-memory-database
     */
    if (db_get_rt_flags(db)&HAM_IN_MEMORY_DB)
        return (0);

    /*
     * update the header page, if necessary
     */
    if (db_is_dirty(db)) {
        ham_page_t *page=db_get_header_page(db);

        memcpy(page_get_payload(page), &db_get_header(db),
                sizeof(db_header_t)-sizeof(freel_payload_t));
        page_set_dirty(page, 1);
        st=page_flush(page);
        if (st)
            return (db_set_error(db, st));
    }

    st=db_flush_all(db, DB_FLUSH_NODELETE);
    if (st)
        return (db_set_error(db, st));

    st=db_get_device(db)->flush(db_get_device(db));
    if (st)
        return (db_set_error(db, st));

    return (HAM_SUCCESS);
}

ham_status_t
ham_close(ham_db_t *db)
{
    ham_status_t st=0;
    ham_backend_t *be;

    if (!db)
        return (HAM_INV_PARAMETER);

    db_set_error(db, 0);

    be=db_get_backend(db);

    /*
     * in-memory-database: free all allocated blobs
     */
    if (db_get_rt_flags(db)&HAM_IN_MEMORY_DB) {
        ham_txn_t txn;
        free_cb_context_t context;
        context.db=db;
        if (!ham_txn_begin(&txn, db)) {
            (void)be->_fun_enumerate(be, my_free_cb, &context);
            (void)ham_txn_commit(&txn);
        }
    }

    /*
     * free cached memory
     */
    if (db_get_record_allocdata(db)) {
        ham_mem_free(db, db_get_record_allocdata(db));
        db_set_record_allocdata(db, 0);
        db_set_record_allocsize(db, 0);
    }
    if (db_get_key_allocdata(db)) {
        ham_mem_free(db, db_get_key_allocdata(db));
        db_set_key_allocdata(db, 0);
        db_set_key_allocsize(db, 0);
    }

    /*
     * flush the freelist
     */
    st=freel_shutdown(db);
    if (st) {
        ham_log(("freel_shutdown() failed with status %d (%s)",
                st, ham_strerror(st)));
        return (st);
    }

    /*
     * flush all pages
     */
    st=db_flush_all(db, 0);
    if (st) {
        ham_log(("db_flush_all() failed with status %d (%s)",
                st, ham_strerror(st)));
        return (st);
    }

    /*
     * free the cache for extended keys
     */
    if (db_get_extkey_cache(db)) {
        extkey_cache_destroy(db_get_extkey_cache(db));
        db_set_extkey_cache(db, 0);
    }

    /* close the backend */
    if (be) {
        st=be->_fun_close(db_get_backend(db));
        if (st) {
            ham_log(("backend close() failed with status %d (%s)",
                    st, ham_strerror(st)));
            return (st);
        }
        ham_mem_free(db, be);
        db_set_backend(db, 0);
    }

    /*
     * if we're not in read-only mode, and not an in-memory-database,
     * and the dirty-flag is true: flush the page-header to disk
     */
    if (!(db_get_rt_flags(db)&HAM_IN_MEMORY_DB) &&
        db_get_device(db)->is_open(db_get_device(db)) &&
        (!(db_get_rt_flags(db)&HAM_READ_ONLY)) &&
        db_is_dirty(db)) {
        /* copy the persistent header to the database object */
        memcpy(page_get_payload(db_get_header_page(db)), &db_get_header(db),
            sizeof(db_header_t)-sizeof(freel_payload_t));
        page_set_dirty(db_get_header_page(db), 1);
        st=page_flush(db_get_header_page(db));
        if (st) {
            ham_log(("page_flush() failed with status %d (%s)",
                    st, ham_strerror(st)));
            return (db_set_error(db, st));
        }
        st=page_free(db_get_header_page(db));
        if (st) {
            ham_log(("page_free() failed with status %d (%s)",
                    st, ham_strerror(st)));
            return (db_set_error(db, st));
        }
        page_delete(db_get_header_page(db));
        db_set_header_page(db, 0);
    }

    /* get rid of the cache */
    if (db_get_cache(db)) {
        cache_delete(db, db_get_cache(db));
        db_set_cache(db, 0);
    }

    /* close the device */
    if (db_get_device(db)->is_open(db_get_device(db))) {
        (void)db_get_device(db)->flush(db_get_device(db));
        (void)db_get_device(db)->close(db_get_device(db));
        db_set_device(db, 0);
    }

    return (0);
}

ham_status_t
ham_cursor_create(ham_db_t *db, void *reserved, ham_u32_t flags,
        ham_cursor_t **cursor)
{
    if (!db || !cursor)
        return (HAM_INV_PARAMETER);

    db_set_error(db, 0);

    return (bt_cursor_create(db, 0, flags, (ham_bt_cursor_t **)cursor));
}

ham_status_t
ham_cursor_clone(ham_cursor_t *src, ham_cursor_t **dest)
{
    if (!src || !dest)
        return (HAM_INV_PARAMETER);

    db_set_error(cursor_get_db(src), 0);

    return (bt_cursor_clone((ham_bt_cursor_t *)src, (ham_bt_cursor_t **)dest));
}

ham_status_t
ham_cursor_replace(ham_cursor_t *cursor, ham_record_t *record,
            ham_u32_t flags)
{
    if (!cursor || !record)
        return (HAM_INV_PARAMETER);

    if (db_get_rt_flags(cursor_get_db(cursor))&HAM_READ_ONLY)
        return (db_set_error(cursor_get_db(cursor), HAM_DB_READ_ONLY));

    db_set_error(cursor_get_db(cursor), 0);

    return (bt_cursor_replace((ham_bt_cursor_t *)cursor, record, flags));
}

ham_status_t
ham_cursor_move(ham_cursor_t *cursor, ham_key_t *key,
            ham_record_t *record, ham_u32_t flags)
{
    if (!cursor)
        return (HAM_INV_PARAMETER);

    db_set_error(cursor_get_db(cursor), 0);

    return (bt_cursor_move((ham_bt_cursor_t *)cursor, key, record, flags));
}

ham_status_t
ham_cursor_find(ham_cursor_t *cursor, ham_key_t *key, ham_u32_t flags)
{
    if (!cursor || !key)
        return (HAM_INV_PARAMETER);

    db_set_error(cursor_get_db(cursor), 0);

    return (bt_cursor_find((ham_bt_cursor_t *)cursor, key, flags));
}

ham_status_t
ham_cursor_insert(ham_cursor_t *cursor, ham_key_t *key,
            ham_record_t *record, ham_u32_t flags)
{
    ham_db_t *db;

    if (!cursor || !key || !record)
        return (HAM_INV_PARAMETER);

    db=cursor_get_db(cursor);
    db_set_error(db, 0);

    if (db_get_rt_flags(db)&HAM_READ_ONLY)
        return (db_set_error(db, HAM_DB_READ_ONLY));
    if ((db_get_rt_flags(db)&HAM_DISABLE_VAR_KEYLEN) &&
        (key->size>db_get_keysize(db)))
        return (db_set_error(db, HAM_INV_KEYSIZE));
    if ((db_get_keysize(db)<sizeof(ham_offset_t)) &&
        (key->size>db_get_keysize(db)))
        return (db_set_error(db, HAM_INV_KEYSIZE));

    return (bt_cursor_insert((ham_bt_cursor_t *)cursor, key, record, flags));
}

ham_status_t
ham_cursor_erase(ham_cursor_t *cursor, ham_u32_t flags)
{
    ham_status_t st;
    ham_offset_t rid;
    ham_u32_t intflags;
    ham_txn_t txn;
    ham_db_t *db;

    if (!cursor)
        return (HAM_INV_PARAMETER);

    db=cursor_get_db(cursor);
    db_set_error(db, 0);

    if (db_get_rt_flags(db)&HAM_READ_ONLY)
        return (db_set_error(db, HAM_DB_READ_ONLY));

    if ((st=ham_txn_begin(&txn, db)))
        return (st);

    st=bt_cursor_erase((ham_bt_cursor_t *)cursor, &rid, &intflags, flags);
    if (st==HAM_SUCCESS) {
        if (!((intflags&KEY_BLOB_SIZE_TINY) ||
              (intflags&KEY_BLOB_SIZE_SMALL) ||
              (intflags&KEY_BLOB_SIZE_EMPTY)))
            st=blob_free(cursor_get_db(cursor), rid, flags);
    }

    if (st) {
        (void)ham_txn_abort(&txn);
        return (st);
    }

    return (ham_txn_commit(&txn));
}

ham_status_t
ham_cursor_close(ham_cursor_t *cursor)
{
    if (!cursor)
        return (HAM_INV_PARAMETER);

    db_set_error(cursor_get_db(cursor), 0);

    return (bt_cursor_close((ham_bt_cursor_t *)cursor));
}
