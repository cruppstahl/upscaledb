/*
 * Copyright (C) 2005-2010 Christoph Rupp (chris@crupp.de).
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


#ifdef __cplusplus
extern "C" {
#endif 


#include "packstart.h"

/**
 * the header structure of a log file
 */
typedef HAM_PACK_0 struct HAM_PACK_1 log_header_t
{
    /* the magic */
    ham_u32_t _magic;

    /* a reserved field */
    ham_u32_t _reserved;

    /* the last used lsn */
    ham_u64_t _lsn;

} HAM_PACK_2 log_header_t;

#include "packstop.h"

#define HAM_LOG_HEADER_MAGIC                  (('h'<<24)|('l'<<16)|('o'<<8)|'g')

/* get the log header magic */
#define log_header_get_magic(l)                 (l)->_magic

/* set the log header magic */
#define log_header_set_magic(l, m)              (l)->_magic=m

/* get the last used lsn */
#define log_header_get_lsn(l)                   (l)->_lsn

/* set the last used lsn */
#define log_header_set_lsn(l, lsn)              (l)->_lsn=lsn

#include "packstart.h"

/**
 * a log file entry
 */
typedef HAM_PACK_0 struct HAM_PACK_1 log_entry_t
{
    /** the lsn of this entry */
    ham_u64_t _lsn;

    /** the flags of this entry; the lowest 8 bits are the 
     * type of this entry, see below */
    ham_u32_t _flags;

    /** a reserved value */
    ham_u32_t _reserved;

    /** the offset of this operation */
    ham_u64_t _offset;

    /** the size of the data */
    ham_u64_t _data_size;

} HAM_PACK_2 log_entry_t;

#include "packstop.h"

/** 
* @defgroup log_entry_type_set the different types of log entries
* @{
*/

/** Write ahead the contents of a page */
#define LOG_ENTRY_TYPE_WRITE                    1

/**
 * @}
 */

/* get the lsn */
#define log_entry_get_lsn(l)                    (l)->_lsn

/* set the lsn */
#define log_entry_set_lsn(l, lsn)               (l)->_lsn=lsn

/* get the offset of this entry */
#define log_entry_get_offset(l)                 (l)->_offset

/* set the offset of this entry */
#define log_entry_set_offset(l, o)              (l)->_offset=o

/* get the size of this entry */
#define log_entry_get_data_size(l)              (l)->_data_size

/* set the size of this entry */
#define log_entry_set_data_size(l, s)           (l)->_data_size=s

/* get the flags of this entry */
#define log_entry_get_flags(l)                  (l)->_flags

/* set the flags of this entry */
#define log_entry_set_flags(l, f)               (l)->_flags=f

/* get the type of this entry */
#define log_entry_get_type(l)                   ((l)->_flags&0xf)

/* set the type of this entry */
#define log_entry_set_type(l, t)                (l)->_flags|=(t)

/**
 * a Log object
 */
class ham_log_t 
{
  public:
    /** an "iterator" structure for traversing the log files */
    typedef ham_offset_t log_iterator_t;

    /** constructor */
    ham_log_t(ham_env_t *env, ham_u32_t flags=0);

    /** create a new log */
    ham_status_t create(void);

    /** open an existing log */
    ham_status_t open(void);

    /** checks if the log is empty */
    bool is_empty(void);

    /** adds an AFTER-image of a page */
    ham_status_t append_page(ham_page_t *page, ham_u64_t lsn);

    /** clears the logfile */
    ham_status_t clear(void);

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
    ham_status_t get_entry(ham_log_t::log_iterator_t *iter, log_entry_t *entry,
                ham_u8_t **data);

    /** 
     * closes the log, frees all allocated resources. 
     *
     * if @a noclear is true then the log will not be clear()ed. This is 
     * useful for debugging.
     */
    ham_status_t close(ham_bool_t noclear=false);

    /** do the recovery */
    ham_status_t recover(void);

    /** flush the logfile to disk */
    ham_status_t flush(void);

    /**
     * append a log entry for @ref LOG_ENTRY_TYPE_WRITE.
     *
     * @note invoked by @ref ham_log_t::append_page() to save the new 
     * content of the specified page.
     *
     * @sa ham_log_t::append_page
     */
    ham_status_t append_write(ham_u64_t lsn, ham_offset_t offset, 
                    ham_u8_t *data, ham_size_t size);

  private:
    /** writes a byte buffer to the logfile */
    ham_status_t append_entry(log_entry_t *entry, ham_size_t size);

    /** references the Environment this log file is for */
    ham_env_t *m_env;

    /** the log flags - unused so far */
    ham_u32_t m_flags;

    /** the current lsn */
    ham_u64_t m_lsn;

    /** the file descriptor of the log file */
    ham_fd_t m_fd;

};


#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_LOG_H__ */
