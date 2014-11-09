/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
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
#include "2lsn_manager/lsn_manager.h"
#include "3journal/journal.h"
#include "4env/env.h"
#include "4env/env_header.h"
#include "4env/env_local_test.h"
#include "4context/context.h"

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
struct MessageBase;

//
// The Environment implementation for local file access
//
class LocalEnvironment : public Environment
{
  public:
    LocalEnvironment(EnvironmentConfiguration &config);

    // Returns the Device object
    Device *device() {
      return (m_device.get());
    }

    // Returns the Environment's header object with the persistent configuration
    EnvironmentHeader *header() {
      return (m_header.get());
    }

    // Returns the blob manager
    BlobManager *blob_manager() {
      return (m_blob_manager.get());
    }

    // Returns the PageManager instance
    PageManager *page_manager() {
      return (m_page_manager.get());
    }

    // Returns the Journal
    Journal *journal() {
      return (m_journal.get());
    }

    // Returns the lsn manager
    LsnManager *lsn_manager() {
      return (&m_lsn_manager);
    }

    // The transaction manager
    TransactionManager *txn_manager() {
      return (m_txn_manager.get());
    }

    // Increments the lsn and returns the incremented value
    uint64_t next_lsn() {
      return (m_lsn_manager.next());
    }

    // Returns a test gateway
    LocalEnvironmentTest test();

  protected:
    // Creates a new Environment (ham_env_create)
    virtual ham_status_t do_create();

    // Opens a new Environment (ham_env_open)
    virtual ham_status_t do_open();

    // Returns all database names (ham_env_get_database_names)
    virtual ham_status_t do_get_database_names(uint16_t *names,
                    uint32_t *count);

    // Returns environment parameters and flags (ham_env_get_parameters)
    virtual ham_status_t do_get_parameters(ham_parameter_t *param);

    // Flushes the environment and its databases to disk (ham_env_flush)
    virtual ham_status_t do_flush(uint32_t flags);

    // Creates a new database in the environment (ham_env_create_db)
    virtual ham_status_t do_create_db(Database **db,
                    DatabaseConfiguration &config,
                    const ham_parameter_t *param);

    // Opens an existing database in the environment (ham_env_open_db)
    virtual ham_status_t do_open_db(Database **db,
                    DatabaseConfiguration &config,
                    const ham_parameter_t *param);

    // Renames a database in the Environment (ham_env_rename_db)
    virtual ham_status_t do_rename_db(uint16_t oldname, uint16_t newname,
                    uint32_t flags);

    // Erases (deletes) a database from the Environment (ham_env_erase_db)
    virtual ham_status_t do_erase_db(uint16_t name, uint32_t flags);

    // Begins a new transaction (ham_txn_begin)
    virtual Transaction *do_txn_begin(const char *name, uint32_t flags);

    // Commits a transaction (ham_txn_commit)
    virtual ham_status_t do_txn_commit(Transaction *txn, uint32_t flags);

    // Commits a transaction (ham_txn_abort)
    virtual ham_status_t do_txn_abort(Transaction *txn, uint32_t flags);

    // Closes the Environment (ham_env_close)
    virtual ham_status_t do_close(uint32_t flags);

    // Fills in the current metrics
    virtual void do_fill_metrics(ham_env_metrics_t *metrics) const;

  private:
    friend class LocalEnvironmentTest;

    // Runs the recovery process
    void recover(uint32_t flags);

    // Get the btree configuration of the database #i, where |i| is a
    // zero-based index
    PBtreeHeader *btree_header(int i);

    // Sets the dirty-flag of the header page and adds the header page
    // to the Changeset (if recovery is enabled)
    void mark_header_page_dirty(Context *context) {
      Page *page = m_header->header_page();
      page->set_dirty(true);
      if (get_flags() & HAM_ENABLE_RECOVERY)
        context->changeset.put(page);
    }

    // The Environment's header page/configuration
    ScopedPtr<EnvironmentHeader> m_header;

    // The device instance (either a file or an in-memory-db)
    ScopedPtr<Device> m_device;

    // The BlobManager instance
    ScopedPtr<BlobManager> m_blob_manager;

    // The PageManager instance
    ScopedPtr<PageManager> m_page_manager;

    // The logical journal
    ScopedPtr<Journal> m_journal;

    // The lsn manager
    LsnManager m_lsn_manager;
};

} // namespace hamsterdb

#endif /* HAM_ENV_LOCAL_H */
