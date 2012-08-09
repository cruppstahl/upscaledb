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
 * @brief Routines for the journal - writing, reading, recovering
 * 
 * The journal is a logical logfile. It stores high-level information about
 * the database operations (unlike the (physical) log which stores low-level
 * information about modified pages.
 *
 */

#ifndef HAM_JOURNAL_H__
#define HAM_JOURNAL_H__

#include "internal_fwd_decl.h"
#include "mem.h"
#include "env.h"
#include "os.h"


#include "journal_entries.h"

#include "packstart.h"

/**
 * a Journal object
 */
class Journal
{
    static const int JOURNAL_DEFAULT_THRESHOLD = 16;

  public:
    static const ham_u32_t HEADER_MAGIC = ('h'<<24)|('j'<<16)|('o'<<8)|'1';

    enum {
      /** mark the start of a new transaction */
      ENTRY_TYPE_TXN_BEGIN  = 1,
      /** mark the end of an aborted transaction */
      ENTRY_TYPE_TXN_ABORT  = 2,
      /** mark the end of an committed transaction */
      ENTRY_TYPE_TXN_COMMIT = 3,
      /** mark an insert operation */
      ENTRY_TYPE_INSERT     = 4,
      /** mark an erase operation */
      ENTRY_TYPE_ERASE      = 5
    };

    /**
     * the header structure of a journal file
     */
    HAM_PACK_0 struct HAM_PACK_1 Header {
      Header() : magic(0), _reserved(0), lsn(0) { }

      /** the magic */
      ham_u32_t magic;
    
      /* a reserved field */
      ham_u32_t _reserved;
    
      /** the last used lsn */
      ham_u64_t lsn;
    } HAM_PACK_2;

    /**
     * An "iterator" structure for traversing the journal files
     */
    struct Iterator {
      Iterator() : fdidx(0), fdstart(0), offset(0) { }

      /** selects the file descriptor [0..1] */
      int fdidx;

      /** which file descriptor did we start with? [0..1] */
      int fdstart;

      /** the offset in the file of the NEXT entry */
      ham_offset_t offset;
    };

    /** constructor */
    Journal(Environment *env)
      : m_env(env), m_current_fd(0), m_lsn(1), m_last_cp_lsn(0), 
        m_threshold(JOURNAL_DEFAULT_THRESHOLD) {
      m_fd[0] = HAM_INVALID_FD;
      m_fd[1] = HAM_INVALID_FD;
      m_open_txn[0] = 0;
      m_open_txn[1] = 0;
      m_closed_txn[0] = 0;
      m_closed_txn[1] = 0;
    }

    /** creates a new journal */
    ham_status_t create();

    /** opens an existing journal */
    ham_status_t open();

    /** checks if the journal is empty */
    bool is_empty() {
      ham_offset_t size;

      if (m_fd[0] == m_fd[1] && m_fd[1] == HAM_INVALID_FD)
        return (true);

      for (int i = 0; i < 2; i++) {
        ham_status_t st = os_get_filesize(m_fd[i], &size);
        if (st)
          return (false); /* TODO throw exception */
        if (size && size != sizeof(Header))
          return (false);
      }

      return (true);
    }

    /* appends a journal entry for ham_txn_begin/ENTRY_TYPE_TXN_BEGIN */
    ham_status_t append_txn_begin(Transaction *txn, Environment *env, 
                const char *name, ham_u64_t lsn);

    /** appends a journal entry for 
     * ham_txn_abort/ENTRY_TYPE_TXN_ABORT */
    ham_status_t append_txn_abort(Transaction *txn, ham_u64_t lsn);

    /** appends a journal entry for 
     * ham_txn_commit/ENTRY_TYPE_TXN_COMMIT */
    ham_status_t append_txn_commit(Transaction *txn, ham_u64_t lsn);

    /** appends a journal entry for ham_insert/ENTRY_TYPE_INSERT */
    ham_status_t append_insert(Database *db, Transaction *txn, 
                ham_key_t *key, ham_record_t *record, ham_u32_t flags, 
                ham_u64_t lsn);

    /** appends a journal entry for ham_erase/ENTRY_TYPE_ERASE */
    ham_status_t append_erase(Database *db, Transaction *txn, 
                ham_key_t *key, ham_u32_t dupe, ham_u32_t flags, ham_u64_t lsn);

    /** empties the journal, removes all entries */
    ham_status_t clear() {
      ham_status_t st; 

      for (int i = 0; i < 2; i++) {
        if ((st = clear_file(i)))
          return (st);
      }
      return (0);
    }

    /** Closes the journal, frees all allocated resources */
    ham_status_t close(bool noclear = false);

    /**
     * Recovery! All committed Transactions will be re-applied, all others
     * are automatically aborted
     */
    ham_status_t recover();

    /** get the lsn and increment it */
    ham_u64_t get_incremented_lsn() {
      return (m_lsn++);
    }

  private:
    friend class JournalTest;

    /** gets the lsn; only required for unittests */
    ham_u64_t get_lsn() {
      return (m_lsn);
    }

    /** returns the path of the journal file */
    std::string get_path(int i);

    /**
     * Sequentially returns the next journal entry, starting with 
     * the oldest entry.
     *
     * iter must be initialized with zeroes for the first call
     *
     * 'aux' returns the auxiliary data of the entry, or NULL.
     * 'aux' is either a structure of type journal_entry_insert_t or 
     * journal_entry_erase_t.
     * The memory of 'aux' has to be freed by the caller.
     *
     * returns SUCCESS and an empty entry (lsn is zero) after the last element.
     */
    ham_status_t get_entry(Iterator *iter, JournalEntry *entry, void **aux);

    /** appends an entry to the journal */
    ham_status_t append_entry(int fdidx,
                void *ptr1 = 0, ham_size_t ptr1_size = 0,
                void *ptr2 = 0, ham_size_t ptr2_size = 0,
                void *ptr3 = 0, ham_size_t ptr3_size = 0,
                void *ptr4 = 0, ham_size_t ptr4_size = 0,
                void *ptr5 = 0, ham_size_t ptr5_size = 0) {
        return (os_writev(m_fd[fdidx], ptr1, ptr1_size,
                    ptr2, ptr2_size, ptr3, ptr3_size, 
                    ptr4, ptr4_size, ptr5, ptr5_size));
    }

    /** clears a single file */
    ham_status_t clear_file(int idx);

    /** helper function for the allocator */
    void *allocate(ham_size_t size) {
      return (m_env->get_allocator()->alloc(size));
    }

    /** helper function for the allocator */
    void alloc_free(void *ptr) {
      return (m_env->get_allocator()->free(ptr));
    }

	/** references the Environment this journal file is for */
	Environment *m_env;

    /** the index of the file descriptor we are currently writing to */
    ham_size_t m_current_fd;

    /** the two file descriptors */
    ham_fd_t m_fd[2];

    /** for counting all open transactions in the files */
    ham_size_t m_open_txn[2];

    /** for counting all closed transactions in the files */
    ham_size_t m_closed_txn[2];

    /** the last used lsn */
    ham_u64_t m_lsn;

    /** the lsn of the previous checkpoint */
    ham_u64_t m_last_cp_lsn;

    /** when having more than these Transactions in one file, we 
     * swap the files */
    ham_size_t m_threshold;
};

#include "packstop.h"


#endif /* HAM_JOURNAL_H__ */
