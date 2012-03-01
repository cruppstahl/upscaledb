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
 * The physical logging stores modifications on page level. It is required
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


#include "packstart.h"

/**
 * a Log object
 */
class Log 
{
  public:
    /** the magic of the header */
    static const ham_u32_t HEADER_MAGIC=(('h'<<24)|('l'<<16)|('o'<<8)|'g');

    /**
     * the header structure of a log file
     */
    HAM_PACK_0 struct HAM_PACK_1 Header
    {
        Header() : magic(0), _reserved(0), lsn(0) { };
    
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
    HAM_PACK_0 struct HAM_PACK_1 Entry
    {
        Entry() : lsn(0), flags(0), _reserved(0), offset(0), data_size(0) { };

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

    /** flags for Entry::flags */
    static const ham_u32_t CHANGESET_IS_COMPLETE = 1;

    /** an "iterator" structure for traversing the log files */
    typedef ham_offset_t Iterator;

    /** constructor */
    Log(Environment *env, ham_u32_t flags=0);

    /** create a new log */
    ham_status_t create(void);

    /** open an existing log */
    ham_status_t open(void);

    /** checks if the log is empty */
    bool is_empty(void);

    /** adds an AFTER-image of a page */
    ham_status_t append_page(Page *page, ham_u64_t lsn, 
                ham_size_t page_count);

    /** retrieves the current lsn */
    ham_u64_t get_lsn(void) {
        return (m_lsn);
    }

    /** retrieves the file handle (for unittests) */
    ham_fd_t get_fd(void) {
        return (m_fd);
    }

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
    ham_status_t get_entry(Log::Iterator *iter, Log::Entry *entry,
                ham_u8_t **data);

    /** 
     * clears the logfile 
     *
     * invoked after every checkpoint (which is immediately after each 
     * txn_commit or txn_abort) 
     */
    ham_status_t clear(void);

    /** 
     * closes the log, frees all allocated resources. 
     *
     * if @a noclear is true then the log will not be clear()ed. This is 
     * useful for debugging.
     */
    ham_status_t close(ham_bool_t noclear=false);

    /** do the recovery */
    ham_status_t recover();

    /** flush the logfile to disk */
    ham_status_t flush();

    /**
     * append a log entry for a page modification
     *
     * @note invoked by @ref Log::append_page() to save the new 
     * content of the specified page.
     *
     * @sa Log::append_page
     */
    ham_status_t append_write(ham_u64_t lsn, ham_u32_t flags, 
                    ham_offset_t offset, ham_u8_t *data, ham_size_t size);

    /** returns the path of the log file */
    std::string get_path();

  private:
    /** writes a byte buffer to the logfile */
    ham_status_t append_entry(Log::Entry *entry, ham_size_t size);

    /** references the Environment this log file is for */
    Environment *m_env;

    /** the log flags - unused so far */
    ham_u32_t m_flags;

    /** the current lsn */
    ham_u64_t m_lsn;

    /** the file descriptor of the log file */
    ham_fd_t m_fd;
};

#include "packstop.h"


#endif /* HAM_LOG_H__ */
