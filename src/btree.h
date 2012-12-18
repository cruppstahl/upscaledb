/*
 * Copyright (C) 2005-2012 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

/**
 * @brief the btree-btree
 *
 */

#ifndef HAM_BTREE_H__
#define HAM_BTREE_H__

#include "internal_fwd_decl.h"

#include "endianswap.h"

#include "btree_cursor.h"
#include "btree_key.h"
#include "db.h"
#include "util.h"
#include "btree_stats.h"
#include "statistics.h"

namespace ham {

/** hamsterdb Backend Node/Page Enumerator Status Codes */
enum {
  /** continue with the traversal */
  HAM_ENUM_CONTINUE                 = 0,

  /** do not not descend another level (or from page to key traversal) */
  HAM_ENUM_DO_NOT_DESCEND           = 1,

  /** stop the traversal entirely */
  HAM_ENUM_STOP                     = 2
};

/** Backend Node/Page Enumerator State Codes */
enum {
  /** descend one level; param1 is an integer value with the new level */
  HAM_ENUM_EVENT_DESCEND            = 1,

  /** start of a new page; param1 points to the page */
  HAM_ENUM_EVENT_PAGE_START         = 2,

  /** end of a new page; param1 points to the page */
  HAM_ENUM_EVENT_PAGE_STOP          = 3,

  /** an item in the page; param1 points to the key; param2 is the index
   * of the key in the page */
  HAM_ENUM_EVENT_ITEM               = 4
};

/**
 * a callback function for enumerating the index nodes/pages using the
 * @ref Backend::enumerate callback/method.
 *
 * @param event one of the @ref ham_cb_event state codes
 *
 * @return one of the @ref ham_cb_status values or a @ref ham_status_codes
 *     error code when an error occurred.
 */
typedef ham_status_t (*ham_enumerate_cb_t)(int event, void *param1,
          void *param2, void *context);

class LocalDatabase;

class BtreeIndex
{
  public:
    /** constructor; creates and initializes a new Backend */
    BtreeIndex(LocalDatabase *db, ham_u32_t flags = 0);

    /**
     * create and initialize a btree
     *
     * @remark this function is called after the ham_db_t structure
     * was allocated and the file was opened
     */
    ham_status_t create(ham_u16_t keysize, ham_u32_t flags);

    /**
     * open and initialize a btree
     *
     * @remark this function is called after the ham_db_t structure
     * was allocated and the file was opened
     */
    ham_status_t open(ham_u32_t flags);

    /**
     * close the btree
     *
     * @remark this function is called before the file is closed
     */
    void close(ham_u32_t flags = 0) { /* nop */ }

    /**
     * find a key in the index
     */
    ham_status_t find(Transaction *txn, Cursor *cursor,
            ham_key_t *key, ham_record_t *record, ham_u32_t flags);

    /**
     * insert (or update) a key in the index
     *
     * the btree is responsible for inserting or updating the
     * record. (see blob.h for blob management functions)
     */
    ham_status_t insert(Transaction *txn, ham_key_t *key,
            ham_record_t *record, ham_u32_t flags);

    /**
     * erase a key in the index
     */
    ham_status_t erase(Transaction *txn, ham_key_t *key,
            ham_u32_t flags);

    /**
     * iterate the whole tree and enumerate every item
     */
    ham_status_t enumerate(ham_enumerate_cb_t cb, void *context);

    /**
     * verify the whole tree
     */
    ham_status_t check_integrity();

    /**
     * estimate the number of keys per page, given the keysize
     */
    ham_status_t calc_keycount_per_page(ham_size_t *keycount,
                    ham_u16_t keysize);

    /**
     * uncouple all cursors from a page
     *
     * @remark this is called whenever the page is deleted or
     * becoming invalid
     */
    ham_status_t uncouple_all_cursors(Page *page, ham_size_t start);

    /**
     * looks up a key, points cursor to this key
     * TODO merge with find()
     */
    ham_status_t find_cursor(Transaction *txn, Cursor *cursor,
            ham_key_t *key, ham_record_t *record, ham_u32_t flags);

    /**
     * inserts a key, points cursor to the new key
     * TODO merge with insert()
     */
    ham_status_t insert_cursor(Transaction *txn, ham_key_t *key,
            ham_record_t *record, Cursor *cursor, ham_u32_t flags);

    /**
     * erases the key that the cursor points to
     * TODO merge with erase()
     */
    ham_status_t erase_cursor(Transaction *txn, ham_key_t *key,
            Cursor *cursor, ham_u32_t flags);

    /** get the database pointer */
    LocalDatabase *get_db() {
      return m_db;
    }

    /** get the key size */
    ham_u16_t get_keysize() const {
      return m_keysize;
    }

    /** set the key size */
    void set_keysize(ham_u16_t keysize) {
      m_keysize = keysize;
    }

    /** get the flags */
    ham_u32_t get_flags() const {
      return m_flags;
    }

    /** set the flags */
    void set_flags(ham_u32_t flags) {
      m_flags = flags;
    }

    /** same as above, but only erases a single duplicate */
    // TODO make this private or merge with erase()
    ham_status_t erase_duplicate(Transaction *txn, ham_key_t *key,
              ham_u32_t dupe_id, ham_u32_t flags);

    // TODO make this private
    ham_status_t free_page_extkeys(Page *page, ham_u32_t flags);

    /** get the address of the root node */
    ham_u64_t get_rootpage() const {
      return m_rootpage;
    }

    /** set the address of the root node */
    void set_rootpage(ham_u64_t rp) {
      m_rootpage = rp;
    }

    /** get maximum number of keys per (internal) node */
    ham_u16_t get_maxkeys() const {
      return m_maxkeys;
    }

    /** set maximum number of keys per (internal) node */
    void set_maxkeys(ham_u16_t maxkeys) {
      m_maxkeys = maxkeys;
    }

    /** get minimum number of keys per node - less keys require merge or shift*/
    ham_u16_t get_minkeys() const {
      return m_maxkeys / 2;
    }

    /** getter for keydata1 */
    ByteArray *get_keyarena1() {
      return &m_keydata1;
    }

    /** getter for keydata2 */
    ByteArray *get_keyarena2() {
      return &m_keydata2;
    }

    /** get hinter */
    // TODO make this private
    BtreeStatistics *get_statistics() {
      return (&m_statistics);
    }

  private:
    friend class BtreeCheckAction;
    friend class BtreeFindAction;
    friend class BtreeEraseAction;
    friend class BtreeInsertAction;
    friend class BtreeCursor;
    friend class MiscTest;
    friend class KeyTest;

    /** calculate the "maxkeys" values - the limit of keys per page */
    ham_size_t calc_maxkeys(ham_size_t pagesize, ham_u16_t keysize);

    /**
     * flushes the btree's meta information to the index data;
     * this does not flush the whole index!
     */
    ham_status_t flush_metadata();

    /**
     * find the child page for a key
     *
     * @return returns the child page in @a page_ref
     * @remark if @a idxptr is a valid pointer, it will store the anchor index
     *    of the loaded page
     */
    ham_status_t find_internal(Page *page, ham_key_t *key, Page **page_ref,
                    ham_s32_t *idxptr = 0);

    /**
     * search a leaf node for a key
     *
     * !!!
     * only works with leaf nodes!!
     *
     * @return returns the index of the key, or -1 if the key was not found, or
     *     another negative @ref ham_status_codes value when an
     *     unexpected error occurred.
     */
    ham_s32_t find_leaf(Page *page, ham_key_t *key, ham_u32_t flags);

    /**
     * perform a binary search for the smallest element which is >= the
     * key. also returns the comparison value in cmp; if *cmp == 0 then 
     * the keys are equal
     */
    ham_status_t get_slot(Page *page, ham_key_t *key, ham_s32_t *slot,
                    int *cmp = 0);

    /**
     * compare a public key (ham_key_t, LHS) to an internal key indexed in a
     * page
     *
     * @return -1, 0, +1: lhs < rhs, lhs = rhs, lhs > rhs
     * @return values less than -1 is a ham_status_t error
     *
     */
    int compare_keys(Page *page, ham_key_t *lhs, ham_u16_t rhs);

    /**
     * create a preliminary copy of an @ref BtreeKey key to a @ref ham_key_t
     * in such a way that @ref db->compare_keys can use the data and optionally
     * call @ref db->get_extended_key on this key to obtain all key data, when
     * this is an extended key.
     *
     * @param which specifies whether keydata1 (which = 0) or keydata2 is used
     * to store the pointer in the btree. The pointers are kept to avoid
     * permanent re-allocations (improves performance)
     */
    ham_status_t prepare_key_for_compare(int which, BtreeKey *src,
                    ham_key_t *dest);

    /**
     * copies an internal key;
     * allocates memory unless HAM_KEY_USER_ALLOC is set
     */
    ham_status_t copy_key(const BtreeKey *source, ham_key_t *dest);

    /**
     * read a key
     *
     * @a dest must have been initialized before calling this function; the
     * dest->data space will be reused when the specified size is large enough;
     * otherwise the old dest->data will be ham_mem_free()d and a new
     * space allocated.
     *
     * This can save superfluous heap free+allocation actions in there.
     *
     * @note
     * This routine can cope with HAM_KEY_USER_ALLOC-ated 'dest'-inations.
    // TODO use arena; get rid of txn parameter
     */
    ham_status_t read_key(Transaction *txn, BtreeKey *source,
                    ham_key_t *dest);

    /**
     * read a record
     *
     * @param rid same as record->_rid, if key is not TINY/SMALL. Otherwise,
     * and if HAM_DIRECT_ACCESS is set, we use the rid-pointer to the
     * original record ID
     *
     * flags: either 0 or HAM_DIRECT_ACCESS
    // TODO use arena; get rid of txn parameter
     */
    ham_status_t read_record(Transaction *txn, ham_record_t *record,
                    ham_u64_t *ridptr, ham_u32_t flags);


    /** pointer to the database object */
    LocalDatabase *m_db;

    /** the keysize of this btree index */
    ham_u16_t m_keysize;

    /** the persistent flags of this btree index */
    ham_u32_t m_flags;

    /** address of the root-page */
    ham_offset_t m_rootpage;

    /** maximum keys in an internal page */
    ham_u16_t m_maxkeys;

    /**
     * two pointers for managing key data; these pointers are used to
     * avoid frequent mallocs in key_compare_pub_to_int() etc
     */
    ByteArray m_keydata1;
    ByteArray m_keydata2;

    /** the hinter */
    BtreeStatistics m_statistics;
};


} // namespace ham

#endif /* HAM_BTREE_H__ */
