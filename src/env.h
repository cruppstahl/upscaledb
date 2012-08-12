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
#include "blob.h"
#include "duplicates.h"

/**
 * A helper structure; ham_env_t is declared in ham/hamsterdb.h as an
 * opaque C structure, but internally we use a C++ class. The ham_env_t
 * struct satisfies the C compiler, and internally we just cast the pointers.
 */
struct ham_env_t {
    int dummy;
};

namespace ham {

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

#define envheader_get_version(hdr, i)  ((hdr))->_version[i]


struct db_indexdata_t;
typedef struct db_indexdata_t db_indexdata_t;

#define SIZEOF_FULL_HEADER(env)												\
	(sizeof(env_header_t)+													\
	 (env)->get_max_databases()*sizeof(db_indexdata_t))

class Worker;

/**
 * the Environment structure
 */
class Environment
{
  public:
    /** default constructor initializes all members */
    Environment();

    /** destructor */
    ~Environment();

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
    ham_status_t (*_fun_txn_begin)(Environment *env, Transaction **txn, 
                const char *name, ham_u32_t flags);

    /**
     * aborts a transaction
     */
    ham_status_t (*_fun_txn_abort)(Environment *env, Transaction *txn, 
                ham_u32_t flags);

    /**
     * commits a transaction
     */
    ham_status_t (*_fun_txn_commit)(Environment *env, Transaction *txn, 
                ham_u32_t flags);

    /**
     * close the Environment
     */
    ham_status_t (*_fun_close)(Environment *env, ham_u32_t flags);

	/**
	 * destroy the environment object, free all memory
	 */
	ham_status_t (*destroy)(Environment *self);

    /** get the filename */
    const std::string &get_filename() {
        return (m_filename);
    }

    /** set the filename */
    void set_filename(std::string filename) {
        m_filename=filename;
    }

    /** get the unix file mode */
    ham_u32_t get_file_mode() {
        return (m_file_mode);
    }

    /** set the unix file mode */
    void set_file_mode(ham_u32_t mode) {
        m_file_mode=mode;
    }

    /** get the user-provided context pointer */
    void *get_context_data() {
        return (m_context);
    }

    /** set the user-provided context pointer */
    void set_context_data(void *ctxt) {
        m_context=ctxt;
    }

    /** get the current transaction ID */
    ham_u64_t get_txn_id() {
        return (m_txn_id);
    }

    /** set the current transaction ID */
    void set_txn_id(ham_u64_t id) {
        m_txn_id=id;
    }

    /** get the device */
    Device *get_device() {
        return (m_device);
    }

    /** set the device */
    void set_device(Device *device) {
        m_device=device;
    }

    /** get the cache pointer */
    Cache *get_cache() {
        return (m_cache);
    }

    /** set the cache pointer */
    void set_cache(Cache *cache) {
        m_cache=cache;
    }

    /** get the allocator */
    Allocator *get_allocator() {
        return (m_alloc);
    }

    /** set the allocator */
    void set_allocator(Allocator *alloc) {
        m_alloc=alloc;
    }

    /** get the header page */
    Page *get_header_page() {
        return (m_hdrpage);
    }

    /** set the header page */
    void set_header_page(Page *page) {
        m_hdrpage=page;
    }

    /** get a pointer to the header data */
    env_header_t *get_header() {
        return ((env_header_t *)(get_header_page()->get_payload()));
    }

    /** get the oldest transaction */
    Transaction *get_oldest_txn() {
        return (m_oldest_txn);
    }

    /** set the oldest transaction */
    void set_oldest_txn(Transaction *txn) {
        m_oldest_txn=txn;
    }

    /** get the newest transaction */
    Transaction *get_newest_txn() {
        return (m_newest_txn);
    }

    /** set the newest transaction */
    void set_newest_txn(Transaction *txn) {
        m_newest_txn=txn;
    }

    /** get the log object */
    Log *get_log() {
        return (m_log);
    }

    /** set the log object */
    void set_log(Log *log) {
        m_log=log;
    }

    /** get the journal */
    Journal *get_journal() {
        return (m_journal);
    }

    /** set the journal */
    void set_journal(Journal *j) {
        m_journal=j;
    }

    /** get the freelist */
    Freelist *get_freelist() {
        return (m_freelist);
    }

    /** set the freelist */
    void set_freelist(Freelist *f) {
        m_freelist=f;
    }

    /** get the flags */
    ham_u32_t get_flags() {
        return (m_flags);
    }

    /** set the flags */
    void set_flags(ham_u32_t flags) {
        m_flags=flags;
    }

    /** get the linked list of all open databases */
    Database *get_databases() {
        return (m_databases);
    }

    /** set the linked list of all open databases */
    void set_databases(Database *db) {
        m_databases=db;
    }

    /** get the current changeset */
    Changeset &get_changeset() {
        return (m_changeset);
    }

    /** get the pagesize as specified in ham_env_create_ex */
    ham_size_t get_pagesize() {
        return (m_pagesize);
    }

    /** get the size of the usable persistent payload of a page */
    ham_size_t get_usable_pagesize() {
	    return (get_pagesize()-Page::sizeof_persistent_header);
    }

    /** set the pagesize as specified in ham_env_create_ex */
    void set_pagesize(ham_size_t ps) {
        m_pagesize=ps;
    }

    /** get the cachesize as specified in ham_env_create_ex/ham_env_open_ex */
    ham_u64_t get_cachesize() {
        return (m_cachesize);
    }

    /** set the cachesize as specified in ham_env_create_ex/ham_env_open_ex */
    void set_cachesize(ham_u64_t cs) {
        m_cachesize=cs;
    }

#if HAM_ENABLE_REMOTE
    /** get the curl handle */
    void *get_curl() {
        return (m_curl);
    }

    /** set the curl handle */
    void set_curl(void *curl) {
        m_curl=curl;
    }
#endif

    /**
     * get the maximum number of databases for this file (cached, not read
     * from header page)
     */
    ham_u16_t get_max_databases_cached() {
        return (m_max_databases_cached);
    }

    /**
     * set the maximum number of databases for this file (cached, not written
     * to header page)
     */
    void set_max_databases_cached(ham_u16_t md) {
        m_max_databases_cached=md;
    }

    /** set the 'active' flag of this Environment */
    void set_active(bool a) {
        m_is_active=a;
    }

    /** check whether this environment has been opened/created.  */
    bool is_active() {
        return (m_is_active);
    }

    /** returns true if this Environment is private to a Database 
     * (was implicitly created in ham_create/ham_open) */
    bool is_private();

    /** set the dirty-flag - this is the same as db_set_dirty() */
    void set_dirty(bool dirty) {
        get_header_page()->set_dirty(dirty);
    }

    /** get the dirty-flag */
    bool is_dirty() {
        return (get_header_page()->is_dirty());
    }

    /**
     * Get the private data of the specified database stored at index @a i; 
     * interpretation of the data is up to the backend.
     */
    db_indexdata_t *get_indexdata_ptr(int i);

    /** get the linked list of all file-level filters */
    ham_file_filter_t *get_file_filter() {
        return (m_file_filters);
    }

    /** set the linked list of all file-level filters */
    void set_file_filter(ham_file_filter_t *f) {
        m_file_filters=f;
    }

    /** get the maximum number of databases for this file */
    ham_u16_t get_max_databases() {
        env_header_t *hdr=(env_header_t*)
                    (get_header_page()->get_payload());
        return (ham_db2h16(hdr->_max_databases));
    }

    /** set the maximum number of databases for this file */
    void set_max_databases(ham_u16_t md) {
        get_header()->_max_databases=md;
    }

    /** get the page size from the header page */
    // TODO can be private? 
    ham_size_t get_persistent_pagesize() {
	    return (ham_db2h32(get_header()->_pagesize));
    }

    /** set the page size in the header page */
    // TODO can be private? 
    void set_persistent_pagesize(ham_size_t ps)	{
	    get_header()->_pagesize=ham_h2db32(ps);
    }

    /** get a reference to the DB FILE (global) statistics */
    ham_runtime_statistics_globdata_t *get_global_perf_data() {
        return (&m_perf_data);
    }

    /** set the 'magic' field of a file header */
    void set_magic(ham_u8_t m1, ham_u8_t m2, ham_u8_t m3, ham_u8_t m4) {
	    get_header()->_magic[0]=m1;
	    get_header()->_magic[1]=m2;
	    get_header()->_magic[2]=m3;
	    get_header()->_magic[3]=m4;
    }

    /** returns true if the magic matches */
    bool compare_magic(ham_u8_t m1, ham_u8_t m2, ham_u8_t m3, ham_u8_t m4) {
        if (get_header()->_magic[0]!=m1)
            return (false);
        if (get_header()->_magic[1]!=m2)
            return (false);
        if (get_header()->_magic[2]!=m3)
            return (false);
        if (get_header()->_magic[3]!=m4)
            return (false);
        return (true);
    }

    /** get byte @a i of the 'version'-header */
    ham_u8_t get_version(ham_size_t idx) {
        env_header_t *hdr=(env_header_t *)
                    (get_header_page()->get_payload());
        return (envheader_get_version(hdr, idx));
    }

    /** set the version of a file header */
    void set_version(ham_u8_t a, ham_u8_t b, ham_u8_t c, ham_u8_t d) {
	    get_header()->_version[0]=a;
	    get_header()->_version[1]=b;
	    get_header()->_version[2]=c;
	    get_header()->_version[3]=d;
    }

    /** get the serial number */
    ham_u32_t get_serialno() {
        env_header_t *hdr=(env_header_t*)
                    (get_header_page()->get_payload());
        return (ham_db2h32(hdr->_serialno));
    }

    /** set the serial number */
    void set_serialno(ham_u32_t n) {
        env_header_t *hdr=(env_header_t*)
                    (get_header_page()->get_payload());
        hdr->_serialno=ham_h2db32(n);
    }

    /** get the freelist object of the database */
    FreelistPayload *get_freelist_payload();

    /** set the logfile directory */
    void set_log_directory(const std::string &dir) {
        m_log_directory=dir;
    }

    /** get the logfile directory */
    const std::string &get_log_directory() {
        return (m_log_directory);
    }

    /** get the blob manager */
    BlobManager *get_blob_manager() {
        return (&m_blob_manager);
    }

    /** get the duplicate manager */
    DuplicateManager *get_duplicate_manager() {
        return (&m_duplicate_manager);
    }

    /** flushes the committed transactions to disk */
    ham_status_t flush_committed_txns(bool dontlock);

    /** get the mutex */
    Mutex &get_mutex() {
        return (m_mutex);
    }

    /** either flush committed Transaction to disk or, if available,
     * signal the worker thread (TODO - if there's a commit, then there's
     * ALWAYS a worker thread, right?) */
    ham_status_t signal_commit();

    /** set the worker thread 
     * TODO move this into an implementation class */
    void set_worker_thread(Worker *thread) {
        m_worker_thread = thread;
    }

    /** get the worker thread 
     * TODO move this into an implementation class */
    Worker *get_worker_thread() {
        return (m_worker_thread);
    }

  private:
    /** a mutex for this Environment */
    Mutex m_mutex;

    /** the filename of the environment file */
    std::string m_filename;

    /** the 'mode' parameter of ham_env_create_ex */
    ham_u32_t m_file_mode;

    /** the current transaction ID */
    ham_u64_t m_txn_id;

	/** the user-provided context data */
	void *m_context;

    /** the device (either a file or an in-memory-db) */
    Device *m_device;

    /** the cache */
    Cache *m_cache;

    /** the memory allocator */
    Allocator *m_alloc;

    /** the file header page */
    Page *m_hdrpage;

    /** the head of the transaction list (the oldest transaction) */
    Transaction *m_oldest_txn;

    /** the tail of the transaction list (the youngest/newest transaction) */
    Transaction *m_newest_txn;

    /** the physical log */
    Log *m_log;

    /** the logical journal */
    Journal *m_journal;

    /** the Freelist manages the free space in the file */
    Freelist *m_freelist;

    /** the Environment flags - a combination of the persistent flags
     * and runtime flags */
    ham_u32_t m_flags;

    /** a linked list of all open databases */
    Database *m_databases;

    /** the changeset - a list of all pages that were modified during
     * one database operation */
    Changeset m_changeset;

    /** the pagesize which was specified when the env was created */
    ham_size_t m_pagesize;

    /** the cachesize which was specified when the env was created/opened */
    ham_u64_t m_cachesize;

    /** the max. number of databases which was specified when the env 
     * was created */
	ham_u16_t m_max_databases_cached;

	/** true after this object is already in use */
	bool m_is_active;

#if HAM_ENABLE_REMOTE
    /** libcurl remote handle */
    void *m_curl;
#endif

    /** linked list of all file-level filters */
    ham_file_filter_t *m_file_filters;

	/** some freelist algorithm specific run-time data */
	ham_runtime_statistics_globdata_t m_perf_data;

    /** the directory of the log file and journal files */
    std::string m_log_directory;

    /** the BlobManager */
    BlobManager m_blob_manager;

    /** the DuplicateManager */
    DuplicateManager m_duplicate_manager;

    /** the worker thread for flushing committed Transactions */
    Worker *m_worker_thread;
};

/**
 * fetch a page.
 *
 * This is like db_fetch_page, but only for those cases when there's
 * no Database handle
 */
extern ham_status_t
env_fetch_page(Page **page_ref, Environment *env, 
        ham_offset_t address, ham_u32_t flags);

/**
 * allocate a page.
 *
 * This is like db_alloc_page, but only for those cases when there's
 * no Database handle
 */
extern ham_status_t
env_alloc_page(Page **page_ref, Environment *env,
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
env_append_txn(Environment *env, Transaction *txn);

/**
 * remove a transaction to this Environment
 */
extern void
env_remove_txn(Environment *env, Transaction *txn);

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

} // namespace ham

#endif /* HAM_ENV_H__ */
