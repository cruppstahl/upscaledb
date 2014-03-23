/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 */

#ifndef HAM_ENV_LOCAL_H__
#define HAM_ENV_LOCAL_H__

#include <ham/hamsterdb.h>

#include "env.h"
#include "changeset.h"
#include "env_header.h"

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

    // Creates a new Environment (ham_env_create)
    virtual ham_status_t create(const char *filename, ham_u32_t flags,
                    ham_u32_t mode, ham_u32_t page_size, ham_u64_t cache_size,
                    ham_u16_t maxdbs);

    // Opens a new Environment (ham_env_open)
    virtual ham_status_t open(const char *filename, ham_u32_t flags,
                    ham_u64_t cache_size);

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

    // The logical journal
    Journal *m_journal;

    // The directory with the log file and journal files
    std::string m_log_directory;

    // True if AES encryption is enabled
    bool m_encryption_enabled;

    // The AES encryption key
    ham_u8_t m_encryption_key[16];

    // The page_size which was specified when the env was created
    ham_u32_t m_page_size;
};

} // namespace hamsterdb

#endif /* HAM_ENV_LOCAL_H__ */
