/*
 * Copyright (C) 2005-2012 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

/**
 * @brief The Environment structure definitions, getters/setters and functions
 *
 */

#ifndef HAM_ENV_H__
#define HAM_ENV_H__

#include "internal_fwd_decl.h"
#include <string>

#include <ham/hamsterdb_stats.h>
#include <ham/hamsterdb.h>

#include "endianswap.h"
#include "error.h"
#include "page.h"
#include "changeset.h"

#define OFFSETOF(type, member) ((size_t) &((type *)0)->member)

/**
 * This is the minimum chunk size; all chunks (pages and blobs) are aligned
 * to this size. 
 *
 * WARNING: pages (and 'aligned' huge blobs) are aligned to 
 * a DB_PAGESIZE_MIN_REQD_ALIGNMENT boundary, that is, any 'aligned=true' 
 * freelist allocations will produce blocks which are aligned to a 
 * 8*32 == 256 bytes boundary.
 */
#define DB_CHUNKSIZE        32

/**
 * The minimum required database page alignment: since the freelist scanner 
 * works on a byte-boundary basis for aligned storage, all aligned storage 
 * must/align to an 8-bits times 1 DB_CHUNKSIZE-per-bit boundary. Which for a 
 * 32 bytes chunksize means your pagesize minimum required alignment/size 
 * is 8*32 = 256 bytes.
 */
#define DB_PAGESIZE_MIN_REQD_ALIGNMENT		(8 * DB_CHUNKSIZE)

#include "packstart.h"

/**
 * the persistent file header
 */
typedef HAM_PACK_0 struct HAM_PACK_1 
{
	/** magic cookie - always "ham\0" */
	ham_u8_t  _magic[4];

	/** version information - major, minor, rev, reserved */
	ham_u8_t  _version[4];

	/** serial number */
	ham_u32_t _serialno;

	/** size of the page */
	ham_u32_t _pagesize;

	/**
	 * maximum number of databases for this environment
     *
	 * NOTE: formerly, the _max_databases was 32 bits, but since
	 * nobody would use more than 64K tables/indexes, we have the
	 * MSW free for repurposing; as we store data in Little Endian
	 * order, that would be the second WORD.
	 *
	 * For reasons of backwards compatibility, the default value
	 * there would be zero (0).
	 */
	ham_u16_t _max_databases;

	/** reserved */
	ham_u16_t _reserved1;

	/* 
	 * following here: 
	 *
	 * 1. the private data of the index backend(s) 
	 *      -> see env_get_indexdata()
	 *
	 * 2. the freelist data
	 *      -> see env_get_freelist()
	 */

} HAM_PACK_2 env_header_t;

#include "packstop.h"


/**
 * A helper structure; ham_env_t is declared in ham/hamsterdb.h as an
 * opaque C structure, but internally we use a C++ class. The ham_env_t
 * struct satisfies the C compiler, and internally we just cast the pointers.
 */
struct ham_env_t {
    int dummy;
};

/**
 * the Environment structure
 */
class Environment
{
  public:
    /** default constructor initializes all members */
    Environment();

    /** the current transaction ID */
    ham_u64_t _txn_id;

    /** the filename of the environment file */
    std::string _filename;

    /** the 'mode' parameter of ham_env_create_ex */
    ham_u32_t _file_mode;

	/** the user-provided context data */
	void *_context;

    /** the device (either a file or an in-memory-db) */
    ham_device_t *_device;

    /** the cache */
    Cache *_cache;

    /** the memory allocator */
    mem_allocator_t *_alloc;

    /** the file header page */
    ham_page_t *_hdrpage;

    /** the txn that is currently flushed; needed as long as we have a 
     * combination of logical and phyiscal journal/log; can be removed as 
     * soon as we moved to a 100% logical log */
    ham_txn_t *_flushed_txn;

    /* the head of the transaction list (the oldest transaction) */
    ham_txn_t *_oldest_txn;

    /* the tail of the transaction list (the youngest/newest transaction) */
    ham_txn_t *_newest_txn;

    /** the physical log */
    Log *_log;

    /** the logical journal */
    Journal *_journal;

    /** the Environment flags - a combination of the persistent flags
     * and runtime flags */
    ham_u32_t _rt_flags;

    /** a linked list of all open databases */
    Database *_next;

    /** the changeset - a list of all pages that were modified during
     * one database operation */
    Changeset _changeset;

    /** the pagesize which was specified when the env was created */
    ham_size_t _pagesize;

    /** the cachesize which was specified when the env was created/opened */
    ham_u64_t _cachesize;

#if HAM_ENABLE_REMOTE
    /** libcurl remote handle */
    void *_curl;
#endif

    /** the max. number of databases which was specified when the env 
     * was created */
	ham_u16_t _max_databases;

	/** 
     * true after this item has been opened/created.
	 * Indicates whether this environment is 'active', i.e. between 
	 * a create/open and matching close API call. 
     */
	bool _is_active;

    /**
     * true if this Environment is pre-1.1.0 format
     */
	bool _is_legacy;

    /* linked list of all file-level filters */
    ham_file_filter_t *_file_filters;

	/**
	 * some freelist algorithm specific run-time data
	 *
	 * This is done as a union as it will reduce code complexity
	 * significantly in the common freelist processing areas.
	 */
	ham_runtime_statistics_globdata_t _perf_data;

    /*
     * following here: function pointers which implement access to 
     * local or remote databases. they are initialized in ham_env_create_ex
     * and ham_env_open_ex after the Environment handle was initialized and
     * an allocator was created.
     *
     * @see env_initialize_local
     * @see env_initialize_remote
     */

    /**
     * initialize and create a new Environment
     */
    ham_status_t (*_fun_create)(Environment *env, const char *filename,
            ham_u32_t flags, ham_u32_t mode,
            const ham_parameter_t *param);

    /**
     * initialize and open a new Environment
     */
    ham_status_t (*_fun_open)(Environment *env, const char *filename,
            ham_u32_t flags, const ham_parameter_t *param);

    /**
     * rename a database in the Environment
     */
    ham_status_t (*_fun_rename_db)(Environment *env, ham_u16_t oldname, 
            ham_u16_t newname, ham_u32_t flags);

    /**
     * erase a database from the Environment
     */
    ham_status_t (*_fun_erase_db)(Environment *env, ham_u16_t name, 
            ham_u32_t flags);

    /**
     * get all database names
     */
    ham_status_t (*_fun_get_database_names)(Environment *env, 
            ham_u16_t *names, ham_size_t *count);

    /**
     * get environment parameters
     */
    ham_status_t (*_fun_get_parameters)(Environment *env, ham_parameter_t *param);

    /**
     * flush the environment
     */
    ham_status_t (*_fun_flush)(Environment *env, ham_u32_t flags);

    /**
     * create a database in the environment
     */
    ham_status_t (*_fun_create_db)(Environment *env, Database *db, 
                ham_u16_t dbname, ham_u32_t flags, 
                const ham_parameter_t *param);

    /**
     * open a database in the environment
     */
    ham_status_t (*_fun_open_db)(Environment *env, Database *db, 
                ham_u16_t dbname, ham_u32_t flags, 
                const ham_parameter_t *param);

    /**
     * create a transaction in this environment
     */
    ham_status_t (*_fun_txn_begin)(Environment *env, ham_txn_t **txn, 
                const char *name, ham_u32_t flags);

    /**
     * aborts a transaction
     */
    ham_status_t (*_fun_txn_abort)(Environment *env, ham_txn_t *txn, 
                ham_u32_t flags);

    /**
     * commits a transaction
     */
    ham_status_t (*_fun_txn_commit)(Environment *env, ham_txn_t *txn, 
                ham_u32_t flags);

    /**
     * close the Environment
     */
    ham_status_t (*_fun_close)(Environment *env, ham_u32_t flags);

	/**
	 * destroy the environment object, free all memory
	 */
	ham_status_t (*destroy)(Environment *self);
};


/** get the current transaction ID */
#define env_get_txn_id(env)              (env)->_txn_id

/** set the current transaction ID */
#define env_set_txn_id(env, id)          (env)->_txn_id=(id)

/** get the filename */
#define env_get_filename(env)            (env)->_filename

/** set the filename */
#define env_set_filename(env, f)         (env)->_filename=(f)

/** get the unix file mode */
#define env_get_file_mode(env)           (env)->_file_mode

/** set the unix file mode */
#define env_set_file_mode(env, m)        (env)->_file_mode=(m)

/** get the user-provided context pointer */
#define env_get_context_data(env)        (env)->_context

/** set the user-provided context pointer */
#define env_set_context_data(env, ctxt)  (env)->_context=(ctxt)

/** get the device */
#define env_get_device(env)              (env)->_device

/** set the device */
#define env_set_device(env, d)           (env)->_device=(d)

/** get the allocator */
#define env_get_allocator(env)           (env)->_alloc

/** set the allocator */
#define env_set_allocator(env, a)        (env)->_alloc=(a)

/** get the cache pointer */
#define env_get_cache(env)               (env)->_cache

/** set the cache pointer */
#define env_set_cache(env, c)            (env)->_cache=(c)

/** get the curl handle */
#define env_get_curl(env)                (env)->_curl

/** set the curl handle */
#define env_set_curl(env, c)             (env)->_curl=(c)

/** get the header page */
#define env_get_header_page(env)         (env)->_hdrpage

/**
 * get a pointer to the header data
 *
 * implemented as a function - a macro would break strict aliasing rules
 */
extern env_header_t *
env_get_header(Environment *env);

/** set the header page */
#define env_set_header_page(env, h)      (env)->_hdrpage=(h)

/** set the dirty-flag - this is the same as db_set_dirty() */
#define env_set_dirty(env)              page_set_dirty(env_get_header_page(env))

/** get the dirty-flag */
#define env_is_dirty(env)               page_is_dirty(env_get_header_page(env))

/**
 * Get a reference to the array of database-specific private data; 
 * interpretation of the data is up to the backends.
 *
 * @return a pointer to the persisted @ref db_indexdata_t data array. 
 *
 * @note Use @ref env_get_indexdata_ptr instead when you want a reference to the
 *       @ref db_indexdata_t-based private data for a particular database in 
 *       the environment.
 */
#define env_get_indexdata_arrptr(env)                                         \
    ((db_indexdata_t *)((ham_u8_t *)page_get_payload(                         \
        env_get_header_page(env)) + sizeof(env_header_t)))

/**
 * Get the private data of the specified database stored at index @a i; 
 * interpretation of the data is up to the backend.
 *
 * @return a pointer to the persisted @ref db_indexdata_t data for the 
 * given database.
 *
 * @note Use @ref db_get_indexdata_offset to retrieve the @a i index value 
 * for your database.
 */
#define env_get_indexdata_ptr(env, i)      (env_get_indexdata_arrptr(env) + (i))

/** get the transaction that is currently flushed */
#define env_get_flushed_txn(env)         (env)->_flushed_txn

/** set the transaction that is currently flushed */
#define env_set_flushed_txn(env, t)      (env)->_flushed_txn=t

/** get the newest transaction */
#define env_get_newest_txn(env)          (env)->_newest_txn

/** set the newest transaction */
#define env_set_newest_txn(env, txn)     (env)->_newest_txn=(txn)

/** get the oldest transaction */
#define env_get_oldest_txn(env)          (env)->_oldest_txn

/** set the oldest transaction */
#define env_set_oldest_txn(env, txn)     (env)->_oldest_txn=(txn)

/** get the log object */
#define env_get_log(env)                 (env)->_log

/** set the log object */
#define env_set_log(env, log)            (env)->_log=(log)

/** get the journal */
#define env_get_journal(env)             (env)->_journal

/** set the journal */
#define env_set_journal(env, j)          (env)->_journal=(j)

/** get the runtime-flags */
#define env_get_rt_flags(env)            (env)->_rt_flags

/** set the runtime-flags */
#define env_set_rt_flags(env, f)         (env)->_rt_flags=(f)

/** get the linked list of all open databases */
#define env_get_list(env)                (env)->_next

/** set the linked list of all open databases */
#define env_set_list(env, db)            (env)->_next=(db)

/** get the current changeset */
#define env_get_changeset(env)           (env)->_changeset

/** get the pagesize as specified in ham_env_create_ex */
#define env_get_pagesize(env)            (env)->_pagesize

/** set the pagesize as specified in ham_env_create_ex */
#define env_set_pagesize(env, ps)        (env)->_pagesize=(ps)

/** get the cachesize as specified in ham_env_create_ex/ham_env_open_ex */
#define env_get_cachesize(env)           (env)->_cachesize

/** set the cachesize as specified in ham_env_create_ex/ham_env_open_ex */
#define env_set_cachesize(env, cs)       (env)->_cachesize=(cs)

/** get the keysize */
#define env_get_keysize(env)             (env)->_keysize

/** set the keysize */
#define env_set_keysize(env, ks)         (env)->_keysize=(ks)

/*
 * get the maximum number of databases for this file
 *
 * implemented as a function - a macro would break gcc aliasing rules
 */
extern ham_u16_t
env_get_max_databases(Environment *env);

/**
 * set the maximum number of databases for this file (cached, not written
 * to header file)
 */
#define env_set_max_databases_cached(env, md)  (env)->_max_databases=(md)

/**
 * get the maximum number of databases for this file (cached, not read
 * from header file)
 */
#define env_get_max_databases_cached(env)       (env)->_max_databases

/** set the maximum number of databases for this file */
#define env_set_max_databases(env, md)                                      \
    (env_get_header(env)->_max_databases=(md))

/** get the page size */
#define env_get_persistent_pagesize(env)									\
	(ham_db2h32(env_get_header(env)->_pagesize))

/** set the page size */
#define env_set_persistent_pagesize(env, ps)								\
	env_get_header(env)->_pagesize=ham_h2db32(ps)

/** set the 'magic' field of a file header */
#define env_set_magic(env, a,b,c,d)											\
	{ env_get_header(env)->_magic[0]=a;										\
      env_get_header(env)->_magic[1]=b;										\
      env_get_header(env)->_magic[2]=c;										\
      env_get_header(env)->_magic[3]=d; }

/** get byte @a i of the 'magic'-header */
#define env_get_magic(hdr, i)        ((hdr)->_magic[i])

/** set the version of a file header */
#define env_set_version(env,a,b,c,d)										\
	{ env_get_header(env)->_version[0]=a;									\
      env_get_header(env)->_version[1]=b;									\
      env_get_header(env)->_version[2]=c;									\
      env_get_header(env)->_version[3]=d; }

/* get byte @a i of the 'version'-header */
#define envheader_get_version(hdr, i)      ((hdr)->_version[i])

/**
 * get byte @a i of the 'version'-header
 *
 * implemented as a function - a macro would break gcc aliasing rules
 */
extern ham_u8_t
env_get_version(Environment *env, ham_size_t idx);

/**
 * get the serial number
 *
 * implemented as a function - a macro would break gcc aliasing rules
 */
extern ham_u32_t
env_get_serialno(Environment *env);

/*
 * set the serial number
 *
 * implemented as a function - a macro would break gcc aliasing rules
 */
extern void
env_set_serialno(Environment *env, ham_u32_t n);

#define SIZEOF_FULL_HEADER(env)												\
	(sizeof(env_header_t)+													\
	 env_get_max_databases(env)*sizeof(db_indexdata_t))

/**
 * set the 'active' flag of the environment: a non-zero value 
 * for @a s sets the @a env to 'active', zero(0) sets the @a env 
 * to 'inactive' (closed)
 */
#define env_set_active(env,s)       (env)->_is_active=s

/** check whether this environment has been opened/created.  */
#define env_is_active(env)          (env)->_is_active

/** set the 'legacy' flag of the environment */
#define env_set_legacy(env,l)       (env)->_is_legacy=l

/** check whether this environment is a legacy file (pre 1.1.0) */
#define env_is_legacy(env)          (env)->_is_legacy

/** get the linked list of all file-level filters */
#define env_get_file_filter(env)    (env)->_file_filters

/** set the linked list of all file-level filters */
#define env_set_file_filter(env, f) (env)->_file_filters=(f)

/** get the freelist cache */
#define env_get_freelist_cache(env)      (env)->_freelist_cache

/** set the freelist cache */
#define env_set_freelist_cache(env, c)   (env)->_freelist_cache=(c)

/** get the freelist object of the database */
#define env_get_freelist(env)												\
	((freelist_payload_t *)													\
     (page_get_payload(env_get_header_page(env))+							\
	  SIZEOF_FULL_HEADER(env)))

/** get the size of the usable persistent payload of a page */
#define env_get_usable_pagesize(env)										\
	(env_get_pagesize(env) - page_get_persistent_header_size())

/** get a reference to the DB FILE (global) statistics */
#define env_get_global_perf_data(env)    &(env)->_perf_data

/**
 * fetch a page.
 *
 * This is like db_fetch_page, but only for those cases when there's
 * no Database handle
 */
extern ham_status_t
env_fetch_page(ham_page_t **page_ref, Environment *env, 
        ham_offset_t address, ham_u32_t flags);

/**
 * allocate a page.
 *
 * This is like db_alloc_page, but only for those cases when there's
 * no Database handle
 */
extern ham_status_t
env_alloc_page(ham_page_t **page_ref, Environment *env,
                ham_u32_t type, ham_u32_t flags);

/*
 * create a env_backend_t structure for accessing local files
 */
extern ham_status_t
env_initialize_local(Environment *env);

/*
 * create a env_backend_t structure for accessing remote server
 */
extern ham_status_t
env_initialize_remote(Environment *env);

/**
 * Ensure that the environment occupies a minimum number of pages.
 * 
 * This is useful with various storage devices to prevent / reduce
 * fragmentation.
 * 
 * @param env the environment reference.
 * @param minimum_page_count The desired minimum number of storage pages 
        * available to the environment/database.
 * 
 * process: 
 * 
 * <ol>
 * <li> detect how many pages we already have in the environment
 * <li> calculate how many pages we should have
 * <li> when this is more than what we've got so far, tell
 *      the device driver to allocate the remainder and mark
 *      them all as 'free'.
 * </ol>
 * 
 * @remark Caveat:
 *    The required size may be so large that it does not
 *    fit in the current freelist, so one or more of
 *    the allocated 'free' pages will be used for the
 *    extended freelist.
 */
extern ham_status_t
env_reserve_space(Environment *env, ham_offset_t minimum_page_count);

/**
 * add a new transaction to this Environment
 */
extern void
env_append_txn(Environment *env, ham_txn_t *txn);

/**
 * remove a transaction to this Environment
 */
extern void
env_remove_txn(Environment *env, ham_txn_t *txn);

/*
 * flush all committed Transactions to disk
 */
extern ham_status_t
env_flush_committed_txns(Environment *env);

/*
 * increments the lsn and returns the incremended value; if the lsn
 * overflows, HAM_LIMITS_REACHED is returned
 *
 * only works if a journal is created! Otherwise assert(0)
 */
extern ham_status_t
env_get_incremented_lsn(Environment *env, ham_u64_t *lsn);

/*
 * purge the cache if the limits are exceeded
 */
extern ham_status_t
env_purge_cache(Environment *env);


#endif /* HAM_ENV_H__ */
