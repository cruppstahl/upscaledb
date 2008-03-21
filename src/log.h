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
#include "db.h"

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

    /* the size of this entry */
    ham_u64_t _size;

    /* the flags of this entry; the lowest 8 bits are the 
     * type of this entry, see below */
    ham_u32_t _flags;

    /* a reserved value */
    ham_u32_t _reserved;

    /* the offset of the last checkpoint */
    ham_offset_t _last_checkpoint;

    /* the data - this is a raw buffer, which has to be interpreted
     * according to the type/flags */
    ham_u8_t _data[8];

} log_entry_t;

/* 
 * the different types of entries
 */
#define LOG_ENTRY_TYPE_TXN_BEGIN                1
#define LOG_ENTRY_TYPE_TXN_ABORT                2
#define LOG_ENTRY_TYPE_TXN_COMMIT               3
#define LOG_ENTRY_TYPE_WRITE                    4
#define LOG_ENTRY_TYPE_OVERWRITE                5
#define LOG_ENTRY_TYPE_CHECKPOINT               6
#define LOG_ENTRY_TYPE_FLUSH_PAGE               7

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

/* get the size of this entry */
#define log_entry_get_size(l)                   (l)->_size

/* set the size of this entry */
#define log_entry_set_size(l, s)                (l)->_size=s

/* get the flags of this entry */
#define log_entry_get_flags(l)                  (l)->_flags

/* set the flags of this entry */
#define log_entry_set_flags(l, f)               (l)->_flags=f

/* get the type of this entry */
#define log_entry_get_type(l)                   ((l)->_flags&0xf)

/* set the type of this entry */
#define log_entry_set_type(l, t)                (l)->_flags|=(t)

/* get the offset of the previous checkpoint */
#define log_entry_get_last_checkpoint(l)        (l)->_last_checkpoint

/* set the offset of the previous checkpoint */
#define log_entry_set_last_checkpoint(l, cp)    (l)->_last_checkpoint=cp

/* get the data-pointer */
#define log_entry_get_data(l)                   (&(l)->_data[0])


/*
 * a Log object
 */
typedef struct {
    /* the currently active database object */
    ham_db_t *_db;

    /* the log flags - unused so far */
    ham_u32_t _flags;

    /* the two file handles */
    ham_fd_t _fd[2];

    /* the last used lsn */
    ham_u64_t _lsn;

} ham_log_t;

/* get the database pointer */
#define log_get_db(l)                           (l)->_db

/* set the database pointer */
#define log_set_db(l, db)                       (l)->_db=db

/* get the log flags */
#define log_get_flags(l)                        (l)->_flags

/* set the log flags */
#define log_set_flags(l, f)                     (l)->_flags=f

/* get a file descriptor */
#define log_get_fd(l, i)                        (l)->_fd[i]

/* set a file descriptor */
#define log_set_fd(l, i, fd)                    (l)->_fd[i]=fd

/* get the last used lsn */
#define log_get_lsn(l)                          (l)->_lsn

/* set the last used lsn */
#define log_set_lsn(l, lsn)                     (l)->_lsn=lsn

/* swap the file descriptors */
#define log_swap_fds(l)                 do {                                   \
                                            ham_fd_t tmp=log_get_fd(l, 0);     \
                                            log_set_fd(l, 0, log_get_fd(l, 1));\
                                            log_set_fd(l, 1, tmp);             \
                                        } while(0);

/*
 * this function creates a new ham_log_t object
 */
extern ham_log_t *
ham_log_create(ham_db_t *db, const char *dbpath, 
        ham_u32_t mode, ham_u32_t flags);

/*
 * this function opens an existing log
 */
extern ham_log_t *
ham_log_open(ham_db_t *db, const char *dbpath, ham_u32_t flags);

/*
 * returns true if the log is empty
 */
extern ham_status_t
ham_log_is_empty(ham_log_t *log, ham_bool_t *isempty);

/*
 * closes the log, frees all allocated resources
 */
extern ham_status_t
ham_log_close(ham_log_t *log);


#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_LOG_H__ */
