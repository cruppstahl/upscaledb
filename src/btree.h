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
 * @brief the btree-backend
 *
 */

#ifndef HAM_BTREE_H__
#define HAM_BTREE_H__

#include "internal_fwd_decl.h"

#include "endianswap.h"

#include "backend.h"
#include "btree_cursor.h"
#include "btree_key.h"
#include "db.h"
#include "util.h"
#include "btree_stats.h"
#include "statistics.h"

namespace ham {

class BtreeBackend : public Backend
{
  public:
    /** constructor; creates and initializes a new Backend */
    BtreeBackend(Database *db, ham_u32_t flags = 0);

    virtual ~BtreeBackend() { }

    /** same as above, but only erases a single duplicate */
    // TODO make this private
    ham_status_t erase_duplicate(Transaction *txn, ham_key_t *key,
              ham_u32_t dupe_id, ham_u32_t flags);

    // TODO make this private
    ham_status_t cursor_erase_fasttrack(Transaction *txn,
              btree_cursor_t *cursor);

    // TODO make this private
    ham_status_t free_page_extkeys(Page *page, ham_u32_t flags);

    /** flush the backend */
    // TODO make this protected
    virtual ham_status_t do_flush_indexdata();

    /** get the address of the root node */
    ham_u64_t get_rootpage() {
      return (m_rootpage);
    }

    /** set the address of the root node */
    void set_rootpage(ham_u64_t rp) {
      m_rootpage = rp;
    }

    /** get maximum number of keys per (internal) node */
    ham_u16_t get_maxkeys() {
      return (m_maxkeys);
    }

    /** set maximum number of keys per (internal) node */
    void set_maxkeys(ham_u16_t maxkeys) {
      m_maxkeys = maxkeys;
    }

    /** get minimum number of keys per node - less keys require merge or shift*/
    ham_u16_t get_minkeys() {
      return (m_maxkeys / 2);
    }

    /** getter for keydata1 */
    ByteArray *get_keyarena1() {
      return &m_keydata1;
    }

    /** getter for keydata2 */
    ByteArray *get_keyarena2() {
      return &m_keydata2;
    }

    /** find a key in the index */
    // TODO make this protected
    virtual ham_status_t do_find(Transaction *txn, Cursor *cursor,
                    ham_key_t *key, ham_record_t *record, ham_u32_t flags);

    /** get hinter */
    // TODO make this private
    BtreeStatistics *get_statistics() {
      return (&m_statistics);
    }

    /**
     * find the child page for a key
     *
     * @return returns the child page in @a page_ref
     * @remark if @a idxptr is a valid pointer, it will store the anchor index
     *    of the loaded page
     *
    // TODO make this private
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
    
    // TODO make this private
     */
    ham_s32_t find_leaf(Page *page, ham_key_t *key, ham_u32_t flags);

    /**
     * perform a binary search for the smallest element which is >= the
     * key. also returns the comparison value in cmp; if *cmp == 0 then 
     * the keys are equal
    // TODO make this private
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
     // TODO make this private
     */
    int compare_keys(Page *page, ham_key_t *lhs, ham_u16_t rhs);

    /**
     * create a preliminary copy of an @ref btree_key_t key to a @ref ham_key_t
     * in such a way that @ref db->compare_keys can use the data and optionally
     * call @ref db->get_extended_key on this key to obtain all key data, when
     * this is an extended key.
     *
     * @param which specifies whether keydata1 (which = 0) or keydata2 is used
     * to store the pointer in the backend. The pointers are kept to avoid
     * permanent re-allocations (improves performance)
     *
     * Used in conjunction with @ref btree_release_key_after_compare
     // TODO make this private
     */
    ham_status_t prepare_key_for_compare(int which, btree_key_t *src,
                    ham_key_t *dest);

    /**
     * copies an internal key
     *
     * allocates memory unless HAM_KEY_USER_ALLOC is set
     // TODO make this private
     */
    ham_status_t copy_key(const btree_key_t *source, ham_key_t *dest);

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
         // TODO make this private
         // TODO use arena; get rid of txn parameter
     */
    ham_status_t read_key(Transaction *txn, btree_key_t *source,
                    ham_key_t *dest);

    /**
     * read a record
     *
     * @param rid same as record->_rid, if key is not TINY/SMALL. Otherwise,
     * and if HAM_DIRECT_ACCESS is set, we use the rid-pointer to the
     * original record ID
     *
     * flags: either 0 or HAM_DIRECT_ACCESS
         // TODO make this private
         // TODO use arena; get rid of txn parameter
     */
    ham_status_t read_record(Transaction *txn, ham_record_t *record,
                    ham_u64_t *ridptr, ham_u32_t flags);

  protected:
    /** creates a new backend */
    virtual ham_status_t do_create(ham_u16_t keysize, ham_u32_t flags);

    /** open and initialize a backend */
    virtual ham_status_t do_open(ham_u32_t flags);

    /** close the backend */
    virtual void do_close(ham_u32_t flags);

    /** insert (or update) a key in the index */
    virtual ham_status_t do_insert(Transaction *txn, ham_key_t *key,
                     ham_record_t *record, ham_u32_t flags);

    /** erase a key in the index */
    virtual ham_status_t do_erase(Transaction *txn, ham_key_t *key,
                     ham_u32_t flags);

    /** iterate the whole tree and enumerate every item */
    virtual ham_status_t do_enumerate(ham_enumerate_cb_t cb, void *context);

    /** verify the whole tree */
    virtual ham_status_t do_check_integrity();

    /** estimate the number of keys per page, given the keysize */
    virtual ham_status_t do_calc_keycount_per_page(ham_size_t *keycount,
                    ham_u16_t keysize);

    /** uncouple all cursors from a page */
    virtual ham_status_t do_uncouple_all_cursors(Page *page, ham_size_t start);

    /** same as above, but sets the cursor position to the new item */
    virtual ham_status_t do_insert_cursor(Transaction *txn, ham_key_t *key,
                    ham_record_t *record, Cursor *cursor, ham_u32_t flags);

    /** same as erase(), but with a coupled cursor */
    virtual ham_status_t do_erase_cursor(Transaction *txn, ham_key_t *key,
                    Cursor *cursor, ham_u32_t flags);

  private:
    /** calculate the "maxkeys" values - the limit of keys per page */
    ham_size_t calc_maxkeys(ham_size_t pagesize, ham_u16_t keysize);

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
