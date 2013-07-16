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

#ifndef HAM_ENV_LOCAL_H__
#define HAM_ENV_LOCAL_H__

#include <ham/hamsterdb.h>

#include "env.h"
#include "changeset.h"
#include "duplicates.h"
#include "env_header.h"

namespace hamsterdb {

class PageManager;
class BlobManager;

//
// The Environment implementation for local file access
//
class LocalEnvironment : public Environment
{
  public:
    LocalEnvironment();

    // Virtual destructor can be overwritten in derived classes
    virtual ~LocalEnvironment();

    // Returns the Device object
    Device *get_device() {
      return (m_device);
    }

    // Returns the Environment's header object with the persistent configuration
    EnvironmentHeader *get_header() {
      return (m_header);
    }

    // Returns the current changeset (stores all modified pages of the current
    // btree modification)
    Changeset &get_changeset() {
      return (m_changeset);
    }

    // Returns the blob manager
    BlobManager *get_blob_manager() {
      return (m_blob_manager);
    }

    // Returns the PageManager instance
    PageManager *get_page_manager() {
      return (m_page_manager);
    }

    // Returns the duplicate manager
    DuplicateManager *get_duplicate_manager() {
      return (&m_duplicate_manager);
    }

    // Returns the Log
    Log *get_log() {
      return (m_log);
    }

    // Sets the Log; only for testing!
    void test_set_log(Log *log) {
      m_log = log;
    }

    // Returns the Journal
    Journal *get_journal() {
      return (m_journal);
    }

    // Sets the Journal; only for testing!
    void test_set_journal(Journal *journal) {
      m_journal = journal;
    }

    // Increments the lsn and returns the incremented value. If the journal
    // is disabled then a dummy value |1| is returned.
    ham_u64_t get_incremented_lsn();

    // Increments the global transaction ID and returns the new value. 
    ham_u64_t get_incremented_txn_id() {
      return (++m_txn_id);
    }

    // Returns the current transaction ID; only for testing!
    ham_u64_t test_get_txn_id() const {
      return (m_txn_id);
    }

    // Sets the current transaction ID; used by the Journal to
    // reset the original txn id during recovery.
    void set_txn_id(ham_u64_t id) {
      m_txn_id = id;
    }

    // Returns the pagesize as specified in ham_env_create
    ham_size_t get_pagesize() const {
      return (m_pagesize);
    }

    // Returns the size of the usable persistent payload of a page
    // (pagesize minus the overhead of the page header)
    ham_size_t get_usable_pagesize() const {
      return (get_pagesize() - Page::sizeof_persistent_header);
    }

    // Sets the dirty-flag of the header page and adds the header page
    // to the Changelog (if recovery is enabled)
    void mark_header_page_dirty() {
      Page *page = m_header->get_header_page();
      page->set_dirty(true);
      if (get_flags() & HAM_ENABLE_RECOVERY)
        m_changeset.add_page(page);
    }

    // Get the private data of the specified database stored at index |i|;
    // interpretation of the data is up to the Btree.
    PBtreeHeader *get_btree_descriptor(int i);

    // Returns the freelist payload stored in the header page.
    // |psize| will contain the payload size, unless the pointer is NULL.
    PFreelistPayload *get_freelist_payload(ham_size_t *psize = 0);

    // Returns the logfile directory
    const std::string &get_log_directory() {
      return (m_log_directory);
    }

    // Sets the logfile directory 
    void set_log_directory(const std::string &dir) {
      m_log_directory = dir;
    }

    // Enables AES encryption
    void enable_encryption(const ham_u8_t *key) {
      m_encryption_enabled = true;
      ::memcpy(m_encryption_key, key, sizeof(m_encryption_key));
    }

    // Returns true if encryption is enabled
    bool is_encryption_enabled() const {
      return (m_encryption_enabled);
    }

    // Returns the AES encryption key
    const ham_u8_t *get_encryption_key() const {
      return (m_encryption_key);
    }

    // Creates a new Environment (ham_env_create)
    virtual ham_status_t create(const char *filename, ham_u32_t flags,
            ham_u32_t mode, ham_size_t pagesize, ham_size_t cachesize,
            ham_u16_t maxdbs);

    // Opens a new Environment (ham_env_open)
    virtual ham_status_t open(const char *filename, ham_u32_t flags,
            ham_size_t cachesize);

    // Renames a database in the Environment (ham_env_rename_db)
    virtual ham_status_t rename_db(ham_u16_t oldname, ham_u16_t newname,
            ham_u32_t flags);

    // Erases (deletes) a database from the Environment (ham_env_erase_db)
    virtual ham_status_t erase_db(ham_u16_t name, ham_u32_t flags);

    // Returns all database names (ham_env_get_database_names)
    virtual ham_status_t get_database_names(ham_u16_t *names,
            ham_size_t *count);

    // Returns environment parameters and flags (ham_env_get_parameters)
    virtual ham_status_t get_parameters(ham_parameter_t *param);

    // Flushes the environment and its databases to disk (ham_env_flush)
    virtual ham_status_t flush(ham_u32_t flags);

    // Creates a new database in the environment (ham_env_create_db)
    virtual ham_status_t create_db(Database **db, ham_u16_t dbname,
            ham_u32_t flags, const ham_parameter_t *param);

    // Opens an existing database in the environment (ham_env_open_db)
    virtual ham_status_t open_db(Database **db, ham_u16_t dbname,
            ham_u32_t flags, const ham_parameter_t *param);

    // Begins a new transaction (ham_txn_begin)
    virtual ham_status_t txn_begin(Transaction **txn, const char *name,
            ham_u32_t flags);

    // Aborts a transaction (ham_txn_abort)
    virtual ham_status_t txn_abort(Transaction *txn, ham_u32_t flags);

    // Commits a transaction (ham_txn_commit)
    virtual ham_status_t txn_commit(Transaction *txn, ham_u32_t flags);

    // Closes the Environment (ham_env_close)
    virtual ham_status_t close(ham_u32_t flags);

    // Fills in the current metrics
    virtual void get_metrics(ham_env_metrics_t *metrics) const;

    // Flushes all committed transactions to disk
    ham_status_t flush_committed_txns();

  private:
    // Flushes a single, committed transaction to disk
    ham_status_t flush_txn(Transaction *txn);

    // Runs the recovery process
    ham_status_t recover(ham_u32_t flags);

    // The Environment's header page/configuration
    EnvironmentHeader *m_header;

    // The device instance (either a file or an in-memory-db)
    Device *m_device;

    // The changeset - a list of all pages that were modified during
    // the current database operation
    Changeset m_changeset;

    // The BlobManager instance
    BlobManager *m_blob_manager;

    // The PageManager instance
    PageManager *m_page_manager;

    // The DuplicateManager
    DuplicateManager m_duplicate_manager;

    // The physical write-ahead log
    Log *m_log;

    // The logical journal
    Journal *m_journal;

    // The current transaction ID
    ham_u64_t m_txn_id;

    // The directory with the log file and journal files
    std::string m_log_directory;

    // True if AES encryption is enabled
    bool m_encryption_enabled;

    // The AES encryption key
    ham_u8_t m_encryption_key[16];

    // The pagesize which was specified when the env was created
    ham_size_t m_pagesize;
};

} // namespace hamsterdb

#endif /* HAM_ENV_LOCAL_H__ */
