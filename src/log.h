/**
 * Copyright (C) 2005-2008 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See file COPYING.GPL2 and COPYING.GPL3 for License information.
 *
 *
 * logging/recovery routines
 *
 */

#ifndef HAM_LOG_H__
#define HAM_LOG_H__

#ifdef __cplusplus
extern "C" {
#endif 

#include <ham/types.h>
#include "mem.h"
#include "device.h"

struct ham_txn_t;
struct ham_page_t;

/**
 * the header structure of a log file
 */
typedef struct {
    /* the magic */
    ham_u32_t _magic;

    /* a reserved field */
    ham_u32_t _reserved;

} log_header_t;

#define HAM_LOG_HEADER_MAGIC                  (('h'<<24)|('l'<<16)|('o'<<8)|'g')

/* get the log header magic */
#define log_header_get_magic(l)                 (l)->_magic

/* set the log header magic */
#define log_header_set_magic(l, m)              (l)->_magic=m

/**
 * a log file entry
 */
typedef struct {
    /* the lsn of this entry */
    ham_u64_t _lsn;

    /* the previous lsn of this transaction */
    ham_u64_t _prev_lsn;

    /* the transaction id */
    ham_u64_t _txn_id;

    /* the flags of this entry; the lowest 8 bits are the 
     * type of this entry, see below */
    ham_u32_t _flags;

    /* a reserved value */
    ham_u32_t _reserved;

    /* the offset of this operation */
    ham_offset_t _offset;

    /* the size of the data */
    ham_u64_t _data_size;

} log_entry_t;

/* 
 * the different types of entries
 */
#define LOG_ENTRY_TYPE_TXN_BEGIN                1
#define LOG_ENTRY_TYPE_TXN_ABORT                2
#define LOG_ENTRY_TYPE_TXN_COMMIT               3
#define LOG_ENTRY_TYPE_PREWRITE                 4
#define LOG_ENTRY_TYPE_WRITE                    5
#define LOG_ENTRY_TYPE_OVERWRITE                6
#define LOG_ENTRY_TYPE_CHECKPOINT               7
#define LOG_ENTRY_TYPE_FLUSH_PAGE               8

/* get the lsn */
#define log_entry_get_lsn(l)                    (l)->_lsn

/* set the lsn */
#define log_entry_set_lsn(l, lsn)               (l)->_lsn=lsn

/* get the previous lsn of the current transaction */
#define log_entry_get_prev_lsn(l)               (l)->_prev_lsn

/* set the previous lsn of the current transaction */
#define log_entry_set_prev_lsn(l, lsn)          (l)->_prev_lsn=lsn

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

/*
 * a Log object
 */
typedef struct {
    /* the allocator object */
    mem_allocator_t *_alloc;

    /* the log flags - unused so far */
    ham_u32_t _flags;

    /* the index of the file descriptor we are currently writing to */
    ham_size_t _current_fd;

    /* the two file descriptors */
    ham_fd_t _fd[2];

    /* for counting all open transactions in the files */
    ham_size_t _open_txn[2];

    /* for counting all closed transactions in the files */
    ham_size_t _closed_txn[2];

    /* the last used lsn */
    ham_u64_t _lsn;

    /* the lsn of the previous checkpoint */
    ham_u64_t _last_cp_lsn;

    /* when having more than these transactions in one logfile, we 
     * swap the files */
    ham_size_t _threshold;

    /* an internal "state" of the log; used to track whether we're
     * currently inserting a checkpoint or not */
    ham_u32_t _state;

    /* a cached data blob which is used for a 2-step overwrite */
    ham_u8_t *_overwrite_data;
    ham_size_t _overwrite_size;

} ham_log_t;

/* get the allocator */
#define log_get_allocator(l)                    (l)->_alloc

/* set the allocator */
#define log_set_allocator(l, a)                 (l)->_alloc=a

/* get the log flags */
#define log_get_flags(l)                        (l)->_flags

/* set the log flags */
#define log_set_flags(l, f)                     (l)->_flags=f

/* get the index of the current file */
#define log_get_current_fd(l)                   (l)->_current_fd

/* set the index of the current file */
#define log_set_current_fd(l, c)                (l)->_current_fd=c

/* get a file descriptor */
#define log_get_fd(l, i)                        (l)->_fd[i]

/* set a file descriptor */
#define log_set_fd(l, i, fd)                    (l)->_fd[i]=fd

/* get the number of open transactions */
#define log_get_open_txn(l, i)                  (l)->_open_txn[i]

/* set the number of open transactions */
#define log_set_open_txn(l, i, c)               (l)->_open_txn[i]=c

/* get the number of closed transactions */
#define log_get_closed_txn(l, i)                (l)->_closed_txn[i]

/* set the number of closed transactions */
#define log_set_closed_txn(l, i, c)             (l)->_closed_txn[i]=c

/* get the last used lsn */
#define log_get_lsn(l)                          (l)->_lsn

/* set the last used lsn */
#define log_set_lsn(l, lsn)                     (l)->_lsn=lsn

/* get the lsn of the last checkpoint */
#define log_get_last_checkpoint_lsn(l)          (l)->_last_cp_lsn

/* set the lsn of the last checkpoint */
#define log_set_last_checkpoint_lsn(l, lsn)     (l)->_last_cp_lsn=lsn

/* get the threshold */
#define log_get_threshold(l)                    (l)->_threshold

/* set the threshold */
#define log_set_threshold(l, t)                 (l)->_threshold=t

/* get the state */
#define log_get_state(l)                        (l)->_state

/* set the state */
#define log_set_state(l, s)                     (l)->_state=s

/* get the overwrite-data */
#define log_get_overwrite_data(l)               (l)->_overwrite_data

/* set the overwrite-data */
#define log_set_overwrite_data(l, d)            (l)->_overwrite_data=d

/* get the overwrite-size */
#define log_get_overwrite_size(l)               (l)->_overwrite_size

/* set the overwrite-size */
#define log_set_overwrite_size(l, s)            (l)->_overwrite_size=s

/* current state: during a CHECKPOINT */
#define LOG_STATE_CHECKPOINT                    1

/*
 * this function creates a new ham_log_t object
 *
 * the first parameter 'db' can be NULL
 */
extern ham_status_t
ham_log_create(mem_allocator_t *alloc, const char *dbpath, 
        ham_u32_t mode, ham_u32_t flags, ham_log_t **log);

/*
 * this function opens an existing log
 */
extern ham_status_t
ham_log_open(mem_allocator_t *alloc, const char *dbpath, ham_u32_t flags,
        ham_log_t **log);

/*
 * returns true if the log is empty
 */
extern ham_status_t
ham_log_is_empty(ham_log_t *log, ham_bool_t *isempty);

/*
 * appends an entry to the log
 */
extern ham_status_t
ham_log_append_entry(ham_log_t *log, int fdidx, void *entry, ham_size_t size);

/*
 * append a log entry for LOG_ENTRY_TYPE_TXN_BEGIN
 */
extern ham_status_t
ham_log_append_txn_begin(ham_log_t *log, struct ham_txn_t *txn);

/*
 * append a log entry for LOG_ENTRY_TYPE_TXN_ABORT
 */
extern ham_status_t
ham_log_append_txn_abort(ham_log_t *log, struct ham_txn_t *txn);

/*
 * append a log entry for LOG_ENTRY_TYPE_TXN_COMMIT
 */
extern ham_status_t
ham_log_append_txn_commit(ham_log_t *log, struct ham_txn_t *txn);

/*
 * append a log entry for LOG_ENTRY_TYPE_CHECKPOINT
 */
extern ham_status_t
ham_log_append_checkpoint(ham_log_t *log);

/*
 * append a log entry for LOG_ENTRY_TYPE_FLUSH_PAGE
 */
extern ham_status_t
ham_log_append_flush_page(ham_log_t *log, struct ham_page_t *page);

/*
 * append a log entry for LOG_ENTRY_TYPE_WRITE
 */
extern ham_status_t
ham_log_append_write(ham_log_t *log, ham_txn_t *txn, ham_offset_t offset,
                ham_u8_t *data, ham_size_t size);

/*
 * append a log entry for LOG_ENTRY_TYPE_PREWRITE
 */
extern ham_status_t
ham_log_append_prewrite(ham_log_t *log, ham_txn_t *txn, ham_offset_t offset,
                ham_u8_t *data, ham_size_t size);

/*
 * append a log entry for LOG_ENTRY_TYPE_OVERWRITE
 */
extern ham_status_t
ham_log_append_overwrite(ham_log_t *log, ham_txn_t *txn, ham_offset_t offset,
        const ham_u8_t *old_data, const ham_u8_t *new_data, ham_size_t size);

/*
 * prepare a log entry for LOG_ENTRY_TYPE_OVERWRITE
 */
extern ham_status_t
ham_log_prepare_overwrite(ham_log_t *log, const ham_u8_t *old_data, 
                ham_size_t size);

/*
 * finalize a previously prepared log entry for LOG_ENTRY_TYPE_OVERWRITE
 */
extern ham_status_t
ham_log_finalize_overwrite(ham_log_t *log, ham_txn_t *txn, ham_offset_t offset,
                const ham_u8_t *new_data, ham_size_t size);

/*
 * clears the logfile to zero, removes all entries
 */
extern ham_status_t
ham_log_clear(ham_log_t *log);

/*
 * an "iterator" structure for traversing the log files
 */
typedef struct {

    /* selects the file descriptor */
    ham_u32_t _fdidx;

    /* the offset in the file of the NEXT entry */
    ham_offset_t _offset;

} log_iterator_t;

/*
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

/*
 * closes the log, frees all allocated resources
 */
extern ham_status_t
ham_log_close(ham_log_t *log, ham_bool_t noclear);

/*
 * adds a BEFORE-image of a page (if necessary)
 */
extern ham_status_t
ham_log_add_page_before(ham_page_t *page);

/*
 * adds an AFTER-image of a page
 */
extern ham_status_t
ham_log_add_page_after(ham_page_t *page);

/*
 * adds an AFTER-image of a part of a page
 */
extern ham_status_t
ham_log_add_page_after_range(ham_page_t *page, ham_size_t offset, 
                ham_size_t length);

/*
 * do the recovery
 */
extern ham_status_t
ham_log_recover(ham_log_t *log, ham_device_t *device);

/*
 * a PAGE_FLUSH entry; each entry stores the 
 * page-ID and the lsn of the last flush of this page
 */
typedef struct
{
    ham_u64_t page_id;
    ham_u64_t lsn;
} log_flush_entry_t;

/*
 * a transaction entry; each entry stores the 
 * txn-ID and the state of the txn - either committed or not (= aborted)
 */
typedef struct
{
    ham_u64_t txn_id;
    int state; 
} log_txn_entry_t;

#define TXN_STATE_ABORTED       0
#define TXN_STATE_COMMITTED     1

/*
 * build a list of all transactions and their state at the moment
 * of the "crash", and a list of all pages
 *
 * actually an internal function, but it's externalized so the unittests
 * can access it
 */
extern ham_status_t
ham_log_recover_prepare(ham_log_t *log, log_txn_entry_t **txn_list, 
        ham_size_t *txn_list_size, log_flush_entry_t **flush_list,
        ham_size_t *flush_list_size, ham_u64_t *last_checkpoint);


#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_LOG_H__ */
