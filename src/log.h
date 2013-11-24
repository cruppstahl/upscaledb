/*
 * Copyright (C) 2005-2011 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

/*
 * @brief Structures and routines for physical logging/recovery
 *
 * The physical WAL stores modifications on page level. It is required
 * since several logical operations are not atomic - i.e. SMOs (Btree structure
 * modification operations). The physical log only stores "redo" information
 * of the last operation that was written to the Btree (either an
 * insert or an erase). We do not need "undo" information - all "undo"
 * related logic is part of the journal, not of the log, and only committed
 * operations are written to the log.
 *
 * In later versions of hamsterdb we may be able to get rid of the log
 * alltogether, if we manage to make SMO's atomic as well (tricky, but
 * can be done at least for some of them).
 */

#ifndef HAM_LOG_H__
#define HAM_LOG_H__

#include "os.h"

namespace hamsterdb {

class Page;
class ByteArray;
class LocalEnvironment;

//
// The write-ahead Log object
//
class Log
{
  public:
    // the header magic - first bytes in every file
    static const ham_u32_t HEADER_MAGIC = (('h' << 24) | ('l' << 16)
                                         | ('o' << 8) | 'g');

#include "packstart.h"

    //
    // The header structure of a log file
    //
    HAM_PACK_0 struct HAM_PACK_1 PEnvironmentHeader {
      PEnvironmentHeader()
        : magic(0), _reserved(0), lsn(0) {
      }

      // the magic
      ham_u32_t magic;

      // a reserved field - for padding
      ham_u32_t _reserved;

      // the last used lsn
      ham_u64_t lsn;
    } HAM_PACK_2;

    //
    // A entry in the log file
    //
    HAM_PACK_0 struct HAM_PACK_1 PEntry {
      PEntry()
        : lsn(0), flags(0), _reserved(0), offset(0), data_size(0) {
      }

      // the lsn of this entry
      ham_u64_t lsn;

      // the flags of this entry, see below
      ham_u32_t flags;

      // a reserved value
      ham_u32_t _reserved;

      // the address of the modified page
      ham_u64_t offset;

      // the size of the data
      ham_u64_t data_size;
    } HAM_PACK_2;

#include "packstop.h"

    // flags for PEntry::flags
    enum {
      kChangesetIsComplete = 1
    };

    // an "iterator" structure for traversing the log files
    typedef ham_u64_t Iterator;

    // Constructor
    Log(LocalEnvironment *env)
      : m_env(env), m_lsn(0), m_fd(HAM_INVALID_FD) {
    }

    ~Log() {
      close(true);
    }

    // Creates a new log
    void create();

    // Opens an existing log
    void open();

    // Checks if the log is empty
    bool is_empty() {
      if (m_fd == HAM_INVALID_FD)
        return (true);

      ham_u64_t size = os_get_filesize(m_fd);
      if (size && size != sizeof(Log::PEnvironmentHeader))
        return (false);
      return (true);
    }

    // Adds the write-ahead information of a page
    void append_page(Page *page, ham_u64_t lsn, ham_u32_t page_count);

    // Retrieves the current lsn
    ham_u64_t get_lsn() {
      return (m_lsn);
    }

    // Retrieves the file handle (for unittests)
    ham_fd_t test_get_fd() {
      return (m_fd);
    }

    // clears the logfile
    //
    // This is called after every checkpoint (which is immediately after each
    // txn_commit or txn_abort)
    void clear() {
      os_truncate(m_fd, sizeof(Log::PEnvironmentHeader));

      // after truncate, the file pointer is far beyond the new end of file;
      // reset the file pointer, or the next write will resize the file to
      // the original size
      os_seek(m_fd, sizeof(Log::PEnvironmentHeader), HAM_OS_SEEK_SET);
    }

    // Flushes the log file to disk
    void flush() {
      os_flush(m_fd);
    }

    // Closes the log, frees all allocated resources.
    //
    // If |noclear| is true then the log will not be clear()ed. This is
    // required for debugging.
    void close(bool noclear = false);

    // Performs the recovery by walking through the log from start to end
    // and writing all modified pages.
    void recover();

  private:
	friend struct LogFixture;
	friend struct LogHighLevelFixture;

    // Returns the next log entry
    //
    // |iter| must be initialized with zeroes for the first call.
    // |data| returns the data of the entry.
    //
    // Returns SUCCESS and an empty entry (lsn is zero) after the last element.
    void get_entry(Log::Iterator *iter, Log::PEntry *entry, ByteArray *buffer);

    // Appends a log entry for a page modification
    //
    // Invoked by |Log::append_page()| to save the new
    // content of the specified page.
    void append_write(ham_u64_t lsn, ham_u32_t flags, ham_u64_t offset,
                    ham_u8_t *data, ham_u32_t size);

    // Returns the path of the log file
    std::string get_path();

    // Writes a byte buffer to the log file
    void append_entry(Log::PEntry *entry, ham_u32_t size);

    // References the Environment this log file is for
    LocalEnvironment *m_env;

    // The current lsn
    ham_u64_t m_lsn;

    // The file descriptor of the log file
    ham_fd_t m_fd;
};

} // namespace hamsterdb

#endif /* HAM_LOG_H__ */
