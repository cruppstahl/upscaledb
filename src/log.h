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

/**
 * @brief structures and routines for physical logging/recovery
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

#include "internal_fwd_decl.h"


#include "os.h"

namespace hamsterdb {

/**
 * a Log object
 */
class Log
{
  public:
    /** the magic of the header */
    static const ham_u32_t HEADER_MAGIC = (('h' << 24) | ('l' << 16)
                                         | ('o' << 8) | 'g');

#include "packstart.h"

    /**
     * the header structure of a log file
     */
    HAM_PACK_0 struct HAM_PACK_1 PHeader
    {
      PHeader() : magic(0), _reserved(0), lsn(0) { }

      /* the magic */
      ham_u32_t magic;

      /* a reserved field */
      ham_u32_t _reserved;

      /* the last used lsn */
      ham_u64_t lsn;
    } HAM_PACK_2;

    /**
     * a log file entry
     */
    HAM_PACK_0 struct HAM_PACK_1 PEntry
    {
      PEntry() : lsn(0), flags(0), _reserved(0), offset(0), data_size(0) { }

      /** the lsn of this entry */
      ham_u64_t lsn;

      /** the flags of this entry, see below */
      ham_u32_t flags;

      /** a reserved value */
      ham_u32_t _reserved;

      /** the offset of this operation */
      ham_u64_t offset;

      /** the size of the data */
      ham_u64_t data_size;
    } HAM_PACK_2;

#include "packstop.h"

    /** flags for PEntry::flags */
    static const ham_u32_t CHANGESET_IS_COMPLETE = 1;

    /** an "iterator" structure for traversing the log files */
    typedef ham_u64_t Iterator;

    /** constructor */
    Log(Environment *env, ham_u32_t flags = 0)
      : m_env(env), m_flags(flags), m_lsn(0), m_fd(HAM_INVALID_FD) {
    }

    /** create a new log */
    ham_status_t create();

    /** open an existing log */
    ham_status_t open();

    /** checks if the log is empty */
    bool is_empty() {
      ham_u64_t size;

      if (m_fd == HAM_INVALID_FD)
        return (true);

      ham_status_t st = os_get_filesize(m_fd, &size);
      if (st)
        return (st ? false : true); /* TODO throw */
      if (size && size != sizeof(Log::PHeader))
        return (false);

      return (true);
    }

    /** adds an AFTER-image of a page */
    ham_status_t append_page(Page *page, ham_u64_t lsn, ham_size_t page_count);

    /** retrieves the current lsn */
    ham_u64_t get_lsn() {
      return (m_lsn);
    }

    /** retrieves the file handle (for unittests) */
    ham_fd_t get_fd() {
      return (m_fd);
    }

    /**
     * clears the logfile
     *
     * invoked after every checkpoint (which is immediately after each
     * txn_commit or txn_abort)
     */
    ham_status_t clear() {
      ham_status_t st = os_truncate(m_fd, sizeof(Log::PHeader));
      if (st)
        return (st);

      /* after truncate, the file pointer is far beyond the new end of file;
       * reset the file pointer, or the next write will resize the file to
       * the original size */
      return (os_seek(m_fd, sizeof(Log::PHeader), HAM_OS_SEEK_SET));
    }

    /** flush the logfile to disk */
    ham_status_t flush() {
      return (os_flush(m_fd));
    }

    /**
     * closes the log, frees all allocated resources.
     *
     * if @a noclear is true then the log will not be clear()ed. This is
     * useful for debugging.
     */
    ham_status_t close(bool noclear = false);

    /** do the recovery */
    ham_status_t recover();

  private:
	friend class LogTest;
	friend class LogHighLevelTest;

    /**
     * returns the next log entry
     *
     * iter must be initialized with zeroes for the first call
     *
     * 'data' returns the data of the entry, or NULL if there is no data.
     * The memory has to be freed by the caller.
     *
     * returns SUCCESS and an empty entry (lsn is zero) after the last element.
     */
    ham_status_t get_entry(Log::Iterator *iter, Log::PEntry *entry,
                         ham_u8_t **data);

    /**
     * append a log entry for a page modification
     *
     * @note invoked by @ref Log::append_page() to save the new
     * content of the specified page.
     *
     * @sa Log::append_page
     */
    ham_status_t append_write(ham_u64_t lsn, ham_u32_t flags,
                    ham_u64_t offset, ham_u8_t *data, ham_size_t size);

    /** returns the path of the log file */
    std::string get_path();

    /** writes a byte buffer to the logfile */
    ham_status_t append_entry(Log::PEntry *entry, ham_size_t size);

    /** references the Environment this log file is for */
    Environment *m_env;

    /** the log flags - unused so far */
    ham_u32_t m_flags;

    /** the current lsn */
    ham_u64_t m_lsn;

    /** the file descriptor of the log file */
    ham_fd_t m_fd;
};


} // namespace hamsterdb

#endif /* HAM_LOG_H__ */
