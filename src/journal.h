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
 * @brief routines for the journal - writing, reading, recovering
 *
 */

#ifndef HAM_JOURNAL_H__
#define HAM_JOURNAL_H__

#include "internal_fwd_decl.h"


#ifdef __cplusplus
extern "C" {
#endif 


#include "packstart.h"

/**
 * the header structure of a journal file
 */
typedef HAM_PACK_0 struct HAM_PACK_1 journal_header_t
{
    /* the magic */
    ham_u32_t _magic;

    /* a reserved field */
    ham_u32_t _reserved;

    /** the last used lsn */
    ham_u64_t _lsn;

} HAM_PACK_2 journal_header_t;

#include "packstop.h"

#define HAM_JOURNAL_HEADER_MAGIC              (('h'<<24)|('j'<<16)|('o'<<8)|'1')

/* get the journal header magic */
#define journal_header_get_magic(j)                 (j)->_magic

/* set the journal header magic */
#define journal_header_set_magic(j, m)              (j)->_magic=m

/* get the last used lsn */
#define journal_header_get_lsn(j)                   (j)->_lsn

/* set the last used lsn */
#define journal_header_set_lsn(j, l)                (j)->_lsn=l

#include "journal_entries.h"

/** 
* @defgroup journal_entry_type_set the different types of journal entries
* @{
*/

/** mark the start of a new transaction */
#define JOURNAL_ENTRY_TYPE_TXN_BEGIN                1 
/** mark the end of an aborted transaction */
#define JOURNAL_ENTRY_TYPE_TXN_ABORT                2 
/** mark the end of an committed transaction */
#define JOURNAL_ENTRY_TYPE_TXN_COMMIT               3 
/** mark an insert operation */
#define JOURNAL_ENTRY_TYPE_INSERT                   4 
/** mark an erase operation */
#define JOURNAL_ENTRY_TYPE_ERASE                    5 

/**
 * @}
 */


/**
 * a Journal object
 */
struct journal_t 
{
    /** the allocator object */
    mem_allocator_t *_alloc;

	/** references the Environment this journal file is for */
	ham_env_t *_env;

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

    /** when having more than these Transactions in one file, we 
     * swap the files */
    ham_size_t _threshold;
};

/** get the allocator */
#define journal_get_allocator(j)                    (j)->_alloc

/** set the allocator */
#define journal_set_allocator(j, a)                 (j)->_alloc=(a)

/** get the environment */
#define journal_get_env(j)                          (j)->_env

/** set the environment */
#define journal_set_env(j, a)                       (j)->_env=(a)

/** get the journal flags */
#define journal_get_flags(j)                        (j)->_flags

/** set the journal flags */
#define journal_set_flags(j, f)                     (j)->_flags=(f)

/** get the index of the current file */
#define journal_get_current_fd(j)                   (j)->_current_fd

/** set the index of the current file */
#define journal_set_current_fd(j, c)                (j)->_current_fd=c

/** get a file descriptor */
#define journal_get_fd(j, i)                        (j)->_fd[i]

/** set a file descriptor */
#define journal_set_fd(j, i, fd)                    (j)->_fd[i]=fd

/** get the number of open transactions */
#define journal_get_open_txn(j, i)                  (j)->_open_txn[i]

/** set the number of open transactions */
#define journal_set_open_txn(j, i, c)               (j)->_open_txn[i]=(c)

/** get the number of closed transactions */
#define journal_get_closed_txn(j, i)                (j)->_closed_txn[i]

/** set the number of closed transactions */
#define journal_set_closed_txn(j, i, c)             (j)->_closed_txn[i]=(c)

/** get the last used lsn */
#define journal_get_lsn(j)                          (j)->_lsn

/** set the last used lsn */
#define journal_set_lsn(j, lsn)                     (j)->_lsn=(lsn)

/** increment the last used lsn */
#define journal_increment_lsn(j)                    (j)->_lsn++

/** get the lsn of the last checkpoint */
#define journal_get_last_checkpoint_lsn(j)          (j)->_last_cp_lsn

/** set the lsn of the last checkpoint */
#define journal_set_last_checkpoint_lsn(j, lsn)     (j)->_last_cp_lsn=(lsn)

/** get the threshold */
#define journal_get_threshold(j)                    (j)->_threshold

/** set the threshold */
#define journal_set_threshold(j, t)                 (j)->_threshold=(t)

/**
 * This function creates a new journal_t object
 */
extern ham_status_t
journal_create(ham_env_t *env, ham_u32_t mode, ham_u32_t flags, 
                journal_t **journal);

/**
 * This function opens an existing journal
 */
extern ham_status_t
journal_open(ham_env_t *env, ham_u32_t flags, journal_t **journal);

/**
 * Returns true if the journal is empty
 */
extern ham_status_t
journal_is_empty(journal_t *journal, ham_bool_t *isempty);

/**
 * Appends an entry to the journal
 */
extern ham_status_t
journal_append_entry(journal_t *journal, int fdidx, 
            journal_entry_t *entry, void *aux, ham_size_t size);

/**
 * Force the journal flushed to storage device
 */
extern ham_status_t
journal_flush(journal_t *journal, int fdidx);

/**
 * Append a journal entry for ham_txn_begin/JOURNAL_ENTRY_TYPE_TXN_BEGIN
 */
extern ham_status_t
journal_append_txn_begin(journal_t *journal, struct ham_txn_t *txn);

/**
 * Append a journal entry for ham_txn_abort/JOURNAL_ENTRY_TYPE_TXN_ABORT
 */
extern ham_status_t
journal_append_txn_abort(journal_t *journal, struct ham_txn_t *txn);

/**
 * Append a journal entry for ham_txn_commit/JOURNAL_ENTRY_TYPE_TXN_COMMIT
 */
extern ham_status_t
journal_append_txn_commit(journal_t *journal, struct ham_txn_t *txn);

/**
 * Append a journal entry for ham_insert/JOURNAL_ENTRY_TYPE_INSERT
 */
extern ham_status_t
journal_append_insert(journal_t *journal, ham_txn_t *txn, ham_key_t *key, 
                ham_record_t *record, ham_u32_t flags);

/**
 * Append a journal entry for ham_erase/JOURNAL_ENTRY_TYPE_ERASE
 */
extern ham_status_t
journal_append_erase(journal_t *journal, ham_txn_t *txn, ham_key_t *key,
                ham_u32_t dupe, ham_u32_t flags);

/**
 * Empties the journal, removes all entries
 */
extern ham_status_t
journal_clear(journal_t *journal);

/**
 * An "iterator" structure for traversing the journal files
 */
typedef struct {

    /** selects the file descriptor [0..1] */
    int _fdidx;

    /** which file descriptor did we start with? [0..1] */
    int _fdstart;

    /** the offset in the file of the NEXT entry */
    ham_offset_t _offset;

} journal_iterator_t;

/**
 * Sequentially returns the next journal entry, starting with the oldest entry
 *
 * iter must be initialized with zeroes for the first call
 *
 * 'aux' returns the auxiliary data of the entry, or NULL if there is no data. 
 * It's either a structure of type journal_entry_insert_t or 
 * journal_entry_erase_t.
 *
 * The memory of 'aux' has to be freed by the caller.
 *
 * returns SUCCESS and an empty entry (lsn is zero) after the last element.
 */
extern ham_status_t
journal_get_entry(journal_t *journal, journal_iterator_t *iter, 
                journal_entry_t *entry, void **aux);

/**
 * Closes the journal, frees all allocated resources
 */
extern ham_status_t
journal_close(journal_t *journal, ham_bool_t noclear);

/**
 * Recovers! All committed Transactions will be re-applied
 */
extern ham_status_t
journal_recover(journal_t *journal);


#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_JOURNAL_H__ */
