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
 * @brief logging/recovery routines
 *
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

} HAM_PACK_2 log_header_t;

#include "packstop.h"

#define HAM_LOG_HEADER_MAGIC                  (('h'<<24)|('l'<<16)|('o'<<8)|'g')

/* get the log header magic */
#define log_header_get_magic(l)                 (l)->_magic

/* set the log header magic */
#define log_header_set_magic(l, m)              (l)->_magic=m

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

/** mark the start of a new transaction */
#define LOG_ENTRY_TYPE_TXN_BEGIN                1 
/** mark the end of an aborted transaction */
#define LOG_ENTRY_TYPE_TXN_ABORT                2 
/** mark the end of an committed transaction */
#define LOG_ENTRY_TYPE_TXN_COMMIT               3 
/** save the original page data, before it is modified */
#define LOG_ENTRY_TYPE_PREWRITE                 4 
/** save the new, modified page data. The page is not yet written to disk; 
 * that will only happen once a @ref LOG_ENTRY_TYPE_FLUSH_PAGE happens. */
#define LOG_ENTRY_TYPE_WRITE                    5 
/** set a checkpoint: a point where we will be sure the entire database is flushed to disk */
#define LOG_ENTRY_TYPE_CHECKPOINT               7 
/** mark a page being flushed from the page cache; as this will be a 
 * modified page (otherwise the explicit flush would not occur), we can be 
 * sure to find a @ref LOG_ENTRY_TYPE_WRITE entry in the log history and, 
 * maybe, a @ref LOG_ENTRY_TYPE_PREWRITE before that (new pages obtained 
 * by expanding the database file are generally not 'prewritten' as they will 
 * contain arbitrary garbage before first use. */
#define LOG_ENTRY_TYPE_FLUSH_PAGE               8 

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
    /** the allocator object */
    mem_allocator_t *_alloc;

	/** references the environment (database) this log file is for; may be NULL */
	ham_env_t *_env;

    /** the log flags - unused so far */
    ham_u32_t _flags;

    /** the index of the file descriptor we are currently writing to */
    ham_size_t _current_fd;

    /** the two file descriptors */
    ham_fd_t _fd[2];

    /** for counting all open transactions in the files */
    ham_size_t _open_txn[2];

    /** for counting all closed transactions in the files */
    ham_size_t _closed_txn[2];

    /** the last used lsn */
    ham_u64_t _lsn;

    /** the lsn of the previous checkpoint */
    ham_u64_t _last_cp_lsn;

    /** when having more than these transactions in one logfile, we 
     * swap the files */
    ham_size_t _threshold;

    /** an internal "state" of the log; used to track whether we're
     * currently inserting a checkpoint or not */
    ham_u32_t _state;

    /** a cached data blob which is used for a 2-step overwrite */
    ham_u8_t *_overwrite_data;
    ham_size_t _overwrite_size;

};

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

/** get the index of the current file */
#define log_get_current_fd(l)                   (l)->_current_fd

/** set the index of the current file */
#define log_set_current_fd(l, c)                (l)->_current_fd=c

/** get a file descriptor */
#define log_get_fd(l, i)                        (l)->_fd[i]

/** set a file descriptor */
#define log_set_fd(l, i, fd)                    (l)->_fd[i]=fd

/** get the number of open transactions */
#define log_get_open_txn(l, i)                  (l)->_open_txn[i]

/** set the number of open transactions */
#define log_set_open_txn(l, i, c)               (l)->_open_txn[i]=(c)

/** get the number of closed transactions */
#define log_get_closed_txn(l, i)                (l)->_closed_txn[i]

/** set the number of closed transactions */
#define log_set_closed_txn(l, i, c)             (l)->_closed_txn[i]=(c)

/** get the last used lsn */
#define log_get_lsn(l)                          (l)->_lsn

/** set the last used lsn */
#define log_set_lsn(l, lsn)                     (l)->_lsn=(lsn)

/** increment the last used lsn */
#define log_increment_lsn(l)                    (l)->_lsn++

/** get the lsn of the last checkpoint */
#define log_get_last_checkpoint_lsn(l)          (l)->_last_cp_lsn

/** set the lsn of the last checkpoint */
#define log_set_last_checkpoint_lsn(l, lsn)     (l)->_last_cp_lsn=(lsn)

/** get the threshold */
#define log_get_threshold(l)                    (l)->_threshold

/** set the threshold */
#define log_set_threshold(l, t)                 (l)->_threshold=(t)

/** get the state */
#define log_get_state(l)                        (l)->_state

/** set the state */
#define log_set_state(l, s)                     (l)->_state=(s)

/** get the overwrite-data */
#define log_get_overwrite_data(l)               (l)->_overwrite_data

/** set the overwrite-data */
#define log_set_overwrite_data(l, d)            (l)->_overwrite_data=(d)

/** get the overwrite-size */
#define log_get_overwrite_size(l)               (l)->_overwrite_size

/** set the overwrite-size */
#define log_set_overwrite_size(l, s)            (l)->_overwrite_size=(s)

/** current state bits: during a CHECKPOINT */
#define LOG_STATE_CHECKPOINT                    0x0001

/** current state bits: during a DATABASE EXPANSION */
#define LOG_STATE_DB_EXPANSION                  0x0002

/**
 * this function creates a new ham_log_t object
 *
 * the first parameter 'db' can be NULL
 */
extern ham_status_t
ham_log_create(mem_allocator_t *alloc, ham_env_t *env,
		const char *dbpath, 
        ham_u32_t mode, ham_u32_t flags, ham_log_t **log);

/**
 * this function opens an existing log
 */
extern ham_status_t
ham_log_open(mem_allocator_t *alloc, ham_env_t *env, 
		const char *dbpath, 
		ham_u32_t flags, ham_log_t **log);

/**
 * returns true if the log is empty
 */
extern ham_status_t
ham_log_is_empty(ham_log_t *log, ham_bool_t *isempty);

/**
 * appends an entry to the log
 */
extern ham_status_t
ham_log_append_entry(ham_log_t *log, int fdidx, log_entry_t *entry, 
        ham_size_t size);

/**
 * append a log entry for LOG_ENTRY_TYPE_TXN_BEGIN
 */
extern ham_status_t
ham_log_append_txn_begin(ham_log_t *log, struct ham_txn_t *txn);

/**
 * append a log entry for LOG_ENTRY_TYPE_TXN_ABORT
 */
extern ham_status_t
ham_log_append_txn_abort(ham_log_t *log, struct ham_txn_t *txn);

/**
 * append a log entry for LOG_ENTRY_TYPE_TXN_COMMIT
 */
extern ham_status_t
ham_log_append_txn_commit(ham_log_t *log, struct ham_txn_t *txn);

/**
 * append a log entry for LOG_ENTRY_TYPE_CHECKPOINT
 */
extern ham_status_t
ham_log_append_checkpoint(ham_log_t *log);

/**
append a log entry for LOG_ENTRY_TYPE_FLUSH_PAGE

Process the signal that a page is about to be written to the
device: save the page to the log file which is linked with
that page's database transaction, then flush that log file
to ensure crash recovery.

@note The only time this signal is not delivered is when the
      database starts a new transaction by generating a new
	  checkpoint.

	  At that time pages may be flushed to disc, but we will
	  be sure those pages are already covered by the previous
	  (by now already closed and flushed) transaction log/flush.

@sa page_flush
 */
extern ham_status_t
ham_log_append_flush_page(ham_log_t *log, struct ham_page_t *page);

/**
 * append a log entry for @ref LOG_ENTRY_TYPE_WRITE.

 @note invoked by @ref ham_log_add_page_after() to save the new content of the specified page.

 @sa ham_log_add_page_after
 */
extern ham_status_t
ham_log_append_write(ham_log_t *log, ham_txn_t *txn, ham_offset_t offset,
                ham_u8_t *data, ham_size_t size);

/**
 * append a log entry for @ref LOG_ENTRY_TYPE_PREWRITE.

 @note invoked by @ref ham_log_add_page_before() to preserve the original content of the specified page.

 @sa ham_log_add_page_before
 */
extern ham_status_t
ham_log_append_prewrite(ham_log_t *log, ham_txn_t *txn, ham_offset_t offset,
                ham_u8_t *data, ham_size_t size);

/**
 * clears the logfile to zero, removes all entries
 */
extern ham_status_t
ham_log_clear(ham_log_t *log);

/**
 * an "iterator" structure for traversing the log files
 */
typedef struct {

    /** selects the file descriptor [0..1] */
    int _fdidx;

    /** which file descriptor did we start with? [0..1] */
    int _fdstart;

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
ham_log_get_entry(ham_log_t *log, log_iterator_t *iter, log_entry_t *entry,
                ham_u8_t **data);

/**
 * closes the log, frees all allocated resources
 */
extern ham_status_t
ham_log_close(ham_log_t *log, ham_bool_t noclear);

/**
 * adds a BEFORE-image of a page (if necessary)
 * @sa ham_log_append_prewrite
 */
extern ham_status_t
ham_log_add_page_before(ham_page_t *page);

/**
 * adds an AFTER-image of a page
 */
extern ham_status_t
ham_log_add_page_after(ham_page_t *page);

/**
 * do the recovery
 */
extern ham_status_t
ham_log_recover(ham_log_t *log, ham_device_t *device, ham_env_t *env);

/**
 * recreate the page and remove all uncommitted changes 
 */
extern ham_status_t
ham_log_recreate(ham_log_t *log, ham_page_t *page);

/**
Mark the start of a database storage expansion: this needs to be
set any time the persistent store is increased through allocating
one or more new pages.

@sa env_reserve_space
@sa ham_log_mark_db_expansion_end
@sa ham_log_is_db_expansion
*/
extern void
ham_log_mark_db_expansion_start(ham_env_t *env);

/**
Mark the end of a database storage expansion phase which was initiated 
when @ref ham_log_mark_db_expansion_start had been invoked before.

@sa env_reserve_space
@sa ham_log_mark_db_expansion_start
@sa ham_log_is_db_expansion
*/
extern void
ham_log_mark_db_expansion_end(ham_env_t *env);

/**
Check whether we are currently in the database storage expansion state:
when we are, certain page operations can be simplified as we are merely 
adding free storage pages.

Nevertheless, this state can occur as part of a larger transaction, which
complicates matters a tad when said transaction is aborted: the file
resize operations performed as part of the storage expansion operation
<em>can not be undone</em>. To ensure the log processing will be aware
at the time of recovery, we must log the storage expansion separately 
from the coordinating transaction itself.

@sa env_reserve_space
@sa ham_log_mark_db_expansion_end
@sa ham_log_mark_db_expansion_start
*/
extern ham_bool_t
ham_log_is_db_expansion(ham_env_t *env);

#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_LOG_H__ */
