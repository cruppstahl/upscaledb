/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
    LocalEnvironment();

    // Virtual destructor can be overwritten in derived classes
    virtual ~LocalEnvironment();

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
    ham_u64_t get_incremented_lsn();

    // Returns the page_size as specified in ham_env_create
    ham_u32_t get_page_size() const {
      return (m_page_size);
    }

    // Returns the size of the usable persistent payload of a page
    // (page_size minus the overhead of the page header)
    ham_u32_t get_usable_page_size() const {
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

    // Set threshold for switching journal files
    void set_journal_switch_threshold(ham_u32_t journal_switch_threshold) {
      m_journal_switch_threshold = journal_switch_threshold;
    }

    // Creates a new Environment (ham_env_create)
    virtual ham_status_t create(const char *filename, ham_u32_t flags,
                    ham_u32_t mode, size_t page_size, ham_u64_t cache_size,
                    ham_u16_t maxdbs, ham_u64_t file_size_limit);

    // Opens a new Environment (ham_env_open)
    virtual ham_status_t open(const char *filename, ham_u32_t flags,
                    ham_u64_t cache_size, ham_u64_t file_size_limit);

    // Renames a database in the Environment (ham_env_rename_db)
    virtual ham_status_t rename_db(ham_u16_t oldname, ham_u16_t newname,
                    ham_u32_t flags);

    // Erases (deletes) a database from the Environment (ham_env_erase_db)
    virtual ham_status_t erase_db(ham_u16_t name, ham_u32_t flags);

    // Returns all database names (ham_env_get_database_names)
    virtual ham_status_t get_database_names(ham_u16_t *names,
                    ham_u32_t *count);

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
    virtual Transaction *txn_begin(const char *name, ham_u32_t flags);

    // Closes the Environment (ham_env_close)
    virtual ham_status_t close(ham_u32_t flags);

    // Fills in the current metrics
    virtual void get_metrics(ham_env_metrics_t *metrics) const;

  private:
    // Runs the recovery process
    void recover(ham_u32_t flags);

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

    // The directory with the log file and journal files
    std::string m_log_directory;

    // True if AES encryption is enabled
    bool m_encryption_enabled;

    // The AES encryption key
    ham_u8_t m_encryption_key[16];

    // The page_size which was specified when the env was created
    size_t m_page_size;

    // Journal switch threshold
    ham_u32_t m_journal_switch_threshold;
};

} // namespace hamsterdb

#endif /* HAM_ENV_LOCAL_H */
