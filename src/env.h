/*
 * Copyright (C) 2005-2013 Christoph Rupp (chris@crupp.de).
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

#include <map>
#include <string>

#include <ham/hamsterdb.h>

#ifdef HAM_ENABLE_REMOTE
#  include "protocol/protocol.h"
#endif

#include "endianswap.h"
#include "error.h"
#include "page.h"
#include "changeset.h"
#include "blob_manager.h"
#include "duplicates.h"
#include "mutex.h"
#ifdef HAM_ENABLE_REMOTE
#  include "protocol/protocol.h"
#endif

/**
 * A helper structure; ham_env_t is declared in ham/hamsterdb.h as an
 * opaque C structure, but internally we use a C++ class. The ham_env_t
 * struct satisfies the C compiler, and internally we just cast the pointers.
 */
struct ham_env_t {
  int dummy;
};

namespace hamsterdb {

/** An internal database flag - env handle is remote */
#define DB_IS_REMOTE                        0x00200000

#include "packstart.h"

/**
 * the persistent file header
 */
typedef HAM_PACK_0 struct HAM_PACK_1
{
  /** magic cookie - always "ham\0" */
  ham_u8_t  _magic[4];

  /** version information - major, minor, rev, file */
  ham_u8_t  _version[4];

  /** serial number */
  ham_u32_t _serialno;

  /** size of the page */
  ham_u32_t _pagesize;

  /** maximum number of databases for this environment */
  ham_u16_t _max_databases;

  /** reserved */
  ham_u16_t _reserved1;

  /*
   * following here:
   *
   * 1. the private data of the index btree(s)
   *      -> see get_descriptor()
   *
   * 2. the freelist data
   *      -> see get_freelist_payload()
   */
} HAM_PACK_2 PEnvHeader;

#include "packstop.h"

class PBtreeDescriptor;
class PageManager;
struct PFreelistPayload;

/**
 * the Environment structure
 */
class Environment
{
  public:
    /** A map of all opened Databases */
    typedef std::map<ham_u16_t, Database *> DatabaseMap;

    /** default constructor initializes all members */
    Environment();

    /** destructor */
    virtual ~Environment();

    /** initialize and create a new Environment */
    virtual ham_status_t create(const char *filename, ham_u32_t flags,
            ham_u32_t mode, ham_size_t pagesize, ham_size_t cachesize,
            ham_u16_t maxdbs) = 0;

    /** initialize and open a new Environment */
    virtual ham_status_t open(const char *filename, ham_u32_t flags,
            ham_size_t cachesize) = 0;

    /** rename a database in the Environment */
    virtual ham_status_t rename_db(ham_u16_t oldname, ham_u16_t newname,
            ham_u32_t flags) = 0;

    /** erase a database from the Environment */
    virtual ham_status_t erase_db(ham_u16_t name, ham_u32_t flags) = 0;

    /** get all database names */
    virtual ham_status_t get_database_names(ham_u16_t *names,
            ham_size_t *count) = 0;

    /** get environment parameters */
    virtual ham_status_t get_parameters(ham_parameter_t *param) = 0;

    /** flush the environment */
    virtual ham_status_t flush(ham_u32_t flags) = 0;

    /** create a database in the environment */
    virtual ham_status_t create_db(Database **db, ham_u16_t dbname,
            ham_u32_t flags, const ham_parameter_t *param) = 0;

    /** open a database in the environment */
    virtual ham_status_t open_db(Database **db, ham_u16_t dbname,
            ham_u32_t flags, const ham_parameter_t *param) = 0;

    /** create a transaction in this environment */
    virtual ham_status_t txn_begin(Transaction **txn, const char *name,
            ham_u32_t flags) = 0;

    /** aborts a transaction */
    virtual ham_status_t txn_abort(Transaction *txn, ham_u32_t flags) = 0;

    /** commits a transaction */
    virtual ham_status_t txn_commit(Transaction *txn, ham_u32_t flags) = 0;

    /** close the Environment */
    virtual ham_status_t close(ham_u32_t flags) = 0;

    /** get the filename */
    const std::string &get_filename() {
      return (m_filename);
    }

    /** set the filename */
    void set_filename(const std::string &filename) {
      m_filename = filename;
    }

    /** get the unix file mode */
    ham_u32_t get_file_mode() const {
      return (m_file_mode);
    }

    /** set the unix file mode */
    void set_file_mode(ham_u32_t mode) {
      m_file_mode = mode;
    }

    /** get the user-provided context pointer */
    void *get_context_data() {
      return (m_context);
    }

    /** set the user-provided context pointer */
    void set_context_data(void *ctxt) {
      m_context = ctxt;
    }

    /** get the current transaction ID */
    // TODO move to LocalEnvironment
    ham_u64_t get_txn_id() const {
      return (m_txn_id);
    }

    /** set the current transaction ID */
    void set_txn_id(ham_u64_t id) {
      m_txn_id = id;
    }

    /** get the device */
    Device *get_device() {
      return (m_device);
    }

    /** get the header page */
    Page *get_header_page() {
      return (m_hdrpage);
    }

    /** set the header page */
    void set_header_page(Page *page) {
      m_hdrpage = page;
    }

    /** get a pointer to the header data */
    // TODO move to LocalEnvironment
    PEnvHeader *get_header() {
      return ((PEnvHeader *)(get_header_page()->get_payload()));
    }

    /** get the oldest transaction */
    Transaction *get_oldest_txn() {
      return (m_oldest_txn);
    }

    /** set the oldest transaction */
    void set_oldest_txn(Transaction *txn) {
      m_oldest_txn = txn;
    }

    /** get the newest transaction */
    Transaction *get_newest_txn() {
      return (m_newest_txn);
    }

    /** set the newest transaction */
    void set_newest_txn(Transaction *txn) {
      m_newest_txn = txn;
    }

    /** get the log object */
    // TODO move to LocalEnvironment
    Log *get_log() {
      return (m_log);
    }

    /** set the log object */
    void set_log(Log *log) {
      m_log = log;
    }

    /** get the journal */
    // TODO move to LocalEnvironment
    Journal *get_journal() {
      return (m_journal);
    }

    /** set the journal */
    void set_journal(Journal *j) {
      m_journal = j;
    }

    /** get the flags */
    ham_u32_t get_flags() const {
      return (m_flags);
    }

    /** set the flags */
    void set_flags(ham_u32_t flags) {
      m_flags = flags;
    }

    /** get the current changeset */
    Changeset &get_changeset() {
      return (m_changeset);
    }

    /** get the pagesize as specified in ham_env_create */
    ham_size_t get_pagesize() const {
      return (m_pagesize);
    }

    /** get the size of the usable persistent payload of a page */
    ham_size_t get_usable_pagesize() const {
      return (get_pagesize() - Page::sizeof_persistent_header);
    }

    /** set the pagesize as specified in ham_env_create */
    void set_pagesize(ham_size_t ps) {
      m_pagesize = ps;
    }

    /** Returns the size of the occupied portion in the header structure;
     * the remaining data is the size of the freelist */
    ham_size_t sizeof_full_header();

    /**
     * get the maximum number of databases for this file (cached, not read
     * from header page)
     * TODO get rid of this
     */
    ham_u16_t get_max_databases_cached() const {
      return (m_max_databases_cached);
    }

    /**
     * set the maximum number of databases for this file (cached, not written
     * to header page)
     * TODO get rid of this
     */
    void set_max_databases_cached(ham_u16_t md) {
      m_max_databases_cached = md;
    }

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
     * interpretation of the data is up to the btree.
     */
    PBtreeDescriptor *get_descriptor(int i);

    /** get the maximum number of databases for this file */
    ham_u16_t get_max_databases() {
      PEnvHeader *hdr = (PEnvHeader*)(get_header_page()->get_payload());
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
    void set_persistent_pagesize(ham_size_t ps) {
      get_header()->_pagesize=ham_h2db32(ps);
    }

    /** set the 'magic' field of a file header */
    void set_magic(ham_u8_t m1, ham_u8_t m2, ham_u8_t m3, ham_u8_t m4) {
      get_header()->_magic[0] = m1;
      get_header()->_magic[1] = m2;
      get_header()->_magic[2] = m3;
      get_header()->_magic[3] = m4;
    }

    /** returns true if the magic matches */
    bool verify_magic(ham_u8_t m1, ham_u8_t m2, ham_u8_t m3, ham_u8_t m4) {
      if (get_header()->_magic[0] != m1)
        return (false);
      if (get_header()->_magic[1] != m2)
        return (false);
      if (get_header()->_magic[2] != m3)
        return (false);
      if (get_header()->_magic[3] != m4)
        return (false);
      return (true);
    }

    /** get byte @a i of the 'version'-header */
    ham_u8_t get_version(ham_size_t idx) {
      PEnvHeader *hdr = (PEnvHeader *)(get_header_page()->get_payload());
      return (hdr->_version[idx]);
    }

    /** set the version of a file header */
    void set_version(ham_u8_t a, ham_u8_t b, ham_u8_t c, ham_u8_t d) {
      get_header()->_version[0] = a;
      get_header()->_version[1] = b;
      get_header()->_version[2] = c;
      get_header()->_version[3] = d;
    }

    /** get the serial number */
    ham_u32_t get_serialno() {
      PEnvHeader *hdr = (PEnvHeader*)(get_header_page()->get_payload());
      return (ham_db2h32(hdr->_serialno));
    }

    /** set the serial number */
    void set_serialno(ham_u32_t n) {
      PEnvHeader *hdr = (PEnvHeader *)(get_header_page()->get_payload());
      hdr->_serialno = ham_h2db32(n);
    }

    /** get the freelist object of the database */
    PFreelistPayload *get_freelist_payload();

    /** set the logfile directory */
    void set_log_directory(const std::string &dir) {
      m_log_directory = dir;
    }

    /** enables AES encryption */
    void enable_encryption(const ham_u8_t *key) {
      m_encryption_enabled = true;
      ::memcpy(m_encryption_key, key, sizeof(m_encryption_key));
    }

    // returns true if encryption is enabled
    bool is_encryption_enabled() const {
      return (m_encryption_enabled);
    }

    // returns the encryption key
    const ham_u8_t *get_encryption_key() const {
      return (m_encryption_key);
    }

    /** get the logfile directory */
    const std::string &get_log_directory() {
      return (m_log_directory);
    }

    /** get the blob manager */
    BlobManager *get_blob_manager() {
      return (m_blob_manager);
    }

    /** get the duplicate manager */
    DuplicateManager *get_duplicate_manager() {
      return (&m_duplicate_manager);
    }

    /** flushes the committed transactions to disk */
    // TODO move to LocalEnvironment
    ham_status_t flush_committed_txns();

    /** get the mutex */
    Mutex &get_mutex() {
      return (m_mutex);
    }

    /** get the Database Map */
    /* TODO make this private */
    DatabaseMap &get_database_map() {
      return (m_database_map);
    }

    /** add a new transaction to this Environment */
    void append_txn(Transaction *txn);

    /** remove a transaction from this Environment */
    void remove_txn(Transaction *txn);

    /*
     * increments the lsn and returns the incremended value; if the lsn
     * overflows, HAM_LIMITS_REACHED is returned
     *
     * only works if a journal is created! Otherwise assert(0)
     */
    // TODO move to LocalEnvironment
    ham_status_t get_incremented_lsn(ham_u64_t *lsn);

    /** Retrieve the PageManager instance */
    PageManager *get_page_manager() {
      return (m_page_manager);
    }

    // Fills in the current metrics
    void get_metrics(ham_env_metrics_t *metrics) const;

  protected:
    /** the BlobManager */
    // TODO move to LocalEnvironment
    BlobManager *m_blob_manager;

    /** The PageManager instance */
    // TODO move to LocalEnvironment
    PageManager *m_page_manager;

    /** the device (either a file or an in-memory-db) */
    Device *m_device;

  private:
    /** Flushes a single, committed transaction to disk */
    // TODO move to LocalEnvironment
    ham_status_t flush_txn(Transaction *txn);

    /** a mutex for this Environment */
    Mutex m_mutex;

    /** the filename of the environment file */
    std::string m_filename;

    /** the 'mode' parameter of ham_env_create */
    ham_u32_t m_file_mode;

    /** the current transaction ID */
    // TODO move to LocalEnvironment
    ham_u64_t m_txn_id;

    /** the user-provided context data */
    void *m_context;

    /** a map of all opened Databases */
    DatabaseMap m_database_map;

    /** the file header page */
    // TODO move to LocalEnvironment
    Page *m_hdrpage;

    /** the head of the transaction list (the oldest transaction) */
    Transaction *m_oldest_txn;

    /** the tail of the transaction list (the youngest/newest transaction) */
    Transaction *m_newest_txn;

    /** the physical write-ahead log */
    // TODO move to LocalEnvironment
    Log *m_log;

    /** the logical journal */
    // TODO move to LocalEnvironment
    Journal *m_journal;

    /** the Environment flags - a combination of the persistent flags
     * and runtime flags */
    ham_u32_t m_flags;

    /** the changeset - a list of all pages that were modified during
     * the current database operation */
    // TODO move to LocalEnvironment
    Changeset m_changeset;

    /** the pagesize which was specified when the env was created */
    ham_size_t m_pagesize;

    /** the max. number of databases which was specified when the env
     * was created */
    ham_u16_t m_max_databases_cached;

    /** the directory of the log file and journal files */
    std::string m_log_directory;

    /** the DuplicateManager */
    // TODO move to LocalEnvironment
    DuplicateManager m_duplicate_manager;

    // true if AES encryption is enabled
    bool m_encryption_enabled;

    // the AES encryption key
    ham_u8_t m_encryption_key[16];
};

/**
 * The Environment implementation for local file access
 */
class LocalEnvironment : public Environment
{
  public:
    /** initialize and create a new Environment */
    virtual ham_status_t create(const char *filename, ham_u32_t flags,
            ham_u32_t mode, ham_size_t pagesize, ham_size_t cachesize,
            ham_u16_t maxdbs);

    /** initialize and open a new Environment */
    virtual ham_status_t open(const char *filename, ham_u32_t flags,
            ham_size_t cachesize);

    /** rename a database in the Environment */
    virtual ham_status_t rename_db(ham_u16_t oldname, ham_u16_t newname,
            ham_u32_t flags);

    /** erase a database from the Environment */
    virtual ham_status_t erase_db(ham_u16_t name, ham_u32_t flags);

    /** get all database names */
    virtual ham_status_t get_database_names(ham_u16_t *names,
            ham_size_t *count);

    /** get environment parameters */
    virtual ham_status_t get_parameters(ham_parameter_t *param);

    /** flush the environment */
    virtual ham_status_t flush(ham_u32_t flags);

    /** create a database in the environment */
    virtual ham_status_t create_db(Database **db, ham_u16_t dbname,
            ham_u32_t flags, const ham_parameter_t *param);

    /** open a database in the environment */
    virtual ham_status_t open_db(Database **db, ham_u16_t dbname,
            ham_u32_t flags, const ham_parameter_t *param);

    /** create a transaction in this environment */
    virtual ham_status_t txn_begin(Transaction **txn, const char *name,
            ham_u32_t flags);

    /** aborts a transaction */
    virtual ham_status_t txn_abort(Transaction *txn, ham_u32_t flags);

    /** commits a transaction */
    virtual ham_status_t txn_commit(Transaction *txn, ham_u32_t flags);

    /** close the Environment */
    virtual ham_status_t close(ham_u32_t flags);

  private:
    /** runs the recovery process */
    ham_status_t recover(ham_u32_t flags);
};

/**
 * The Environment implementation for remote file access
 */
#ifdef HAM_ENABLE_REMOTE

class RemoteEnvironment : public Environment
{
  public:
    /** constructor */
    RemoteEnvironment()
      : Environment(), m_curl(0) {
      set_flags(get_flags() | DB_IS_REMOTE);
    }

    /** initialize and create a new Environment */
    virtual ham_status_t create(const char *filename, ham_u32_t flags,
            ham_u32_t mode, ham_size_t pagesize, ham_size_t cachesize,
            ham_u16_t maxdbs);

    /** initialize and open a new Environment */
    virtual ham_status_t open(const char *filename, ham_u32_t flags,
            ham_size_t cachesize);

    /** rename a database in the Environment */
    virtual ham_status_t rename_db(ham_u16_t oldname, ham_u16_t newname,
            ham_u32_t flags);

    /** erase a database from the Environment */
    virtual ham_status_t erase_db(ham_u16_t name, ham_u32_t flags);

    /** get all database names */
    virtual ham_status_t get_database_names(ham_u16_t *names,
            ham_size_t *count);

    /** get environment parameters */
    virtual ham_status_t get_parameters(ham_parameter_t *param);

    /** flush the environment */
    virtual ham_status_t flush(ham_u32_t flags);

    /** create a database in the environment */
    virtual ham_status_t create_db(Database **db, ham_u16_t dbname,
            ham_u32_t flags, const ham_parameter_t *param);

    /** open a database in the environment */
    virtual ham_status_t open_db(Database **db, ham_u16_t dbname,
            ham_u32_t flags, const ham_parameter_t *param);

    /** create a transaction in this environment */
    virtual ham_status_t txn_begin(Transaction **txn, const char *name,
            ham_u32_t flags);

    /** aborts a transaction */
    virtual ham_status_t txn_abort(Transaction *txn, ham_u32_t flags);

    /** commits a transaction */
    virtual ham_status_t txn_commit(Transaction *txn, ham_u32_t flags);

    /** close the Environment */
    virtual ham_status_t close(ham_u32_t flags);

    /** perform a curl request */
    ham_status_t perform_request(Protocol *request, Protocol **reply);

  private:
    /** libcurl remote handle */
    void *m_curl;
};

#endif

} // namespace hamsterdb

#endif /* HAM_ENV_H__ */
