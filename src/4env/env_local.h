/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 * All Rights Reserved.
 *
 * NOTICE: All information contained herein is, and remains the property
 * of Christoph Rupp and his suppliers, if any. The intellectual and
 * technical concepts contained herein are proprietary to Christoph Rupp
 * and his suppliers and may be covered by Patents, patents in process,
 * and are protected by trade secret or copyright law. Dissemination of
 * this information or reproduction of this material is strictly forbidden
 * unless prior written permission is obtained from Christoph Rupp.
 */

/*
 * @exception_safe: unknown
 * @thread_safe: unknown
 */

#ifndef HAM_ENV_LOCAL_H
#define HAM_ENV_LOCAL_H

#include "ham/hamsterdb.h"

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!
#include "1base/scoped_ptr.h"
#include "3changeset/changeset.h"
#include "3journal/journal.h"
#include "4env/env.h"
#include "4env/env_header.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

class PBtreeHeader;
class PFreelistPayload;
class Journal;
class PageManager;
class BlobManager;
class LocalTransaction;

//
// The Environment implementation for local file access
//
class LocalEnvironment : public Environment
{
  public:
    LocalEnvironment(EnvironmentConfiguration &config);

    // Returns the Device object
    Device *get_device() {
      return (m_device.get());
    }

    // Returns the Environment's header object with the persistent configuration
    EnvironmentHeader *get_header() {
      return (m_header.get());
    }

    // Returns the current changeset (stores all modified pages of the current
    // btree modification)
    Changeset &get_changeset() {
      return (m_changeset);
    }

    // Returns the blob manager
    BlobManager *get_blob_manager() {
      return (m_blob_manager.get());
    }

    // Returns the PageManager instance
    PageManager *get_page_manager() {
      return (m_page_manager.get());
    }

    // Returns the Journal
    Journal *get_journal() {
      return (m_journal.get());
    }

    // Sets the Journal; only for testing!
    void test_set_journal(Journal *journal) {
      m_journal.reset(journal);
    }

    // Increments the lsn and returns the incremented value. If the journal
    // is disabled then a dummy value |1| is returned.
    uint64_t get_incremented_lsn();

    // Returns the page_size as specified in ham_env_create
    uint32_t get_page_size() const {
      return ((uint32_t)m_config.page_size_bytes);
    }

    // Returns the size of the usable persistent payload of a page
    // (page_size minus the overhead of the page header)
    uint32_t get_usable_page_size() const {
      return (get_page_size() - Page::kSizeofPersistentHeader);
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

    // Creates a new Environment (ham_env_create)
    virtual ham_status_t create();

    // Opens a new Environment (ham_env_open)
    virtual ham_status_t open();

    // Renames a database in the Environment (ham_env_rename_db)
    virtual ham_status_t rename_db(uint16_t oldname, uint16_t newname,
                    uint32_t flags);

    // Erases (deletes) a database from the Environment (ham_env_erase_db)
    virtual ham_status_t erase_db(uint16_t name, uint32_t flags);

    // Returns all database names (ham_env_get_database_names)
    virtual ham_status_t get_database_names(uint16_t *names,
                    uint32_t *count);

    // Returns environment parameters and flags (ham_env_get_parameters)
    virtual ham_status_t get_parameters(ham_parameter_t *param);

    // Flushes the environment and its databases to disk (ham_env_flush)
    virtual ham_status_t flush(uint32_t flags);

    // Creates a new database in the environment (ham_env_create_db)
    virtual ham_status_t create_db(Database **db, DatabaseConfiguration &config,
                    const ham_parameter_t *param);

    // Opens an existing database in the environment (ham_env_open_db)
    virtual ham_status_t open_db(Database **db, DatabaseConfiguration &config,
                    const ham_parameter_t *param);

    // Begins a new transaction (ham_txn_begin)
    virtual Transaction *txn_begin(const char *name, uint32_t flags);

    // Closes the Environment (ham_env_close)
    virtual ham_status_t close(uint32_t flags);

    // Fills in the current metrics
    virtual void get_metrics(ham_env_metrics_t *metrics) const;

  private:
    // Runs the recovery process
    void recover(uint32_t flags);

    // The Environment's header page/configuration
    ScopedPtr<EnvironmentHeader> m_header;

    // The device instance (either a file or an in-memory-db)
    ScopedPtr<Device> m_device;

    // The changeset - a list of all pages that were modified during
    // the current database operation
    Changeset m_changeset;

    // The BlobManager instance
    ScopedPtr<BlobManager> m_blob_manager;

    // The PageManager instance
    ScopedPtr<PageManager> m_page_manager;

    // The logical journal
    ScopedPtr<Journal> m_journal;
};

} // namespace hamsterdb

#endif /* HAM_ENV_LOCAL_H */
