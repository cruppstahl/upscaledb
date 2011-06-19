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

    /** the transaction id */
    ham_u64_t _txn_id;

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

/* get the transaction ID */
#define log_entry_get_txn_id(l)                 (l)->_txn_id

/* set the transaction ID */
#define log_entry_set_txn_id(l, id)             (l)->_txn_id=id

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
struct ham_log_t 
{
    /** the lsn of this entry */
    ham_u64_t _lsn;

    /** the allocator object */
    mem_allocator_t *_alloc;

    /** references the Environment this log file is for */
    ham_env_t *_env;

    /** the log flags - unused so far */
    ham_u32_t _flags;

    /** the file descriptor of the log file */
    ham_fd_t _fd;

};

/* get the lsn */
#define log_get_lsn(l)                          (l)->_lsn

/* set the lsn */
#define log_set_lsn(l, lsn)                     (l)->_lsn=lsn

/** get the allocator */
#define log_get_allocator(l)                    (l)->_alloc

/** set the allocator */
#define log_set_allocator(l, a)                 (l)->_alloc=(a)

/** get the environment */
#define log_get_env(l)                          (l)->_env

/** set the environment */
#define log_set_env(l, a)                       (l)->_env=(a)

/** get the log flags */
#define log_get_flags(l)                        (l)->_flags

/** set the log flags */
#define log_set_flags(l, f)                     (l)->_flags=(f)

/** get the file descriptor */
#define log_get_fd(l)                           (l)->_fd

/** set the file descriptor */
#define log_set_fd(l, fd)                       (l)->_fd=fd

/**
 * this function creates a new ham_log_t object
 */
extern ham_status_t
log_create(ham_env_t *env, ham_u32_t mode, ham_u32_t flags, ham_log_t **log);

/**
 * this function opens an existing log
 */
extern ham_status_t
log_open(ham_env_t *env, ham_u32_t flags, ham_log_t **log);

/**
 * returns true if the log is empty
 */
extern ham_status_t
log_is_empty(ham_log_t *log, ham_bool_t *isempty);

/**
 * appends an entry to the log
 */
extern ham_status_t
log_append_entry(ham_log_t *log, log_entry_t *entry, ham_size_t size);

/**
 * append a log entry for @ref LOG_ENTRY_TYPE_WRITE.
 *
 * @note invoked by @ref log_append_page() to save the new 
 * content of the specified page.
 *
 * @sa log_append_page
 */
extern ham_status_t
log_append_write(ham_log_t *log, ham_txn_t *txn, ham_u64_t lsn,
        ham_offset_t offset, ham_u8_t *data, ham_size_t size);

/**
 * clears the logfile to zero, removes all entries
 */
extern ham_status_t
log_clear(ham_log_t *log);

/**
 * an "iterator" structure for traversing the log files
 */
typedef struct log_iterator_t 
{
    /** the offset in the file of the NEXT entry */
    ham_offset_t _offset;

} log_iterator_t;

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
extern ham_status_t
log_get_entry(ham_log_t *log, log_iterator_t *iter, log_entry_t *entry,
                ham_u8_t **data);

/**
 * closes the log, frees all allocated resources
 */
extern ham_status_t
log_close(ham_log_t *log, ham_bool_t noclear);

/**
 * adds an AFTER-image of a page
 */
extern ham_status_t
log_append_page(ham_log_t *log, ham_page_t *page, ham_u64_t lsn);

/**
 * do the recovery
 */
extern ham_status_t
log_recover(ham_log_t *log);

/**
 * flush the logfile to disk
 */
extern ham_status_t
log_flush(ham_log_t *log);


#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_LOG_H__ */
