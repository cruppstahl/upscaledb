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

namespace ham {

/** a macro for getting the minimum number of keys */
#define btree_get_minkeys(maxkeys)      (maxkeys/2)

/** defines the maximum number of keys per node */
#define MAX_KEYS_PER_NODE               0xFFFFU /* max(ham_u16_t) */


class BtreeBackend : public Backend
{
  public:
    /** constructor; creates and initializes a new Backend */
    BtreeBackend(Database *db, ham_u32_t flags=0);

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
                            ham_key_t *key, ham_record_t *record,
                            ham_u32_t flags);

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
                            ham_record_t *record, btree_cursor_t *cursor,
                            ham_u32_t flags);

    /** same as erase(), but with a coupled cursor */
    virtual ham_status_t do_erase_cursor(Transaction *txn, ham_key_t *key,
                            btree_cursor_t *cursor, ham_u32_t flags);

  private:
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
};


#include "packstart.h"

/**
 * A btree-node; it spans the persistent part of a Page:
 *
 * <pre>
 * btree_node_t *btp=(btree_node_t *)page->_u._pers.payload;
 * </pre>
 */
typedef HAM_PACK_0 struct HAM_PACK_1 btree_node_t
{
  /**
   * flags of this node - flags are always the first member
   * of every page - regardless of the backend.
   *
   * Currently only used for the page type.
   *
   * @sa page_type_codes
   */
  ham_u16_t _flags;

  /** number of used entries in the node */
  ham_u16_t _count;

  /** address of left sibling */
  ham_offset_t _left;

  /** address of right sibling */
  ham_offset_t _right;

  /**
   * address of child node whose items are smaller than all items
   * in this node
   */
  ham_offset_t _ptr_left;

  /** the entries of this node */
  btree_key_t _entries[1];

} HAM_PACK_2 btree_node_t;

#include "packstop.h"

/** get the number of entries of a btree-node */
#define btree_node_get_count(btp)      (ham_db2h16(btp->_count))

/** set the number of entries of a btree-node */
#define btree_node_set_count(btp, c)     btp->_count=ham_h2db16(c)

/** get the left sibling of a btree-node */
#define btree_node_get_left(btp)       (ham_db2h_offset(btp->_left))

/** check if a btree node is a leaf node */
#define btree_node_is_leaf(btp)        (!(btree_node_get_ptr_left(btp)))

/** set the left sibling of a btree-node */
#define btree_node_set_left(btp, l)      btp->_left=ham_h2db_offset(l)

/** get the right sibling of a btree-node */
#define btree_node_get_right(btp)      (ham_db2h_offset(btp->_right))

/** set the right sibling of a btree-node */
#define btree_node_set_right(btp, r)     btp->_right=ham_h2db_offset(r)

/** get the ptr_left of a btree-node */
#define btree_node_get_ptr_left(btp)     (ham_db2h_offset(btp->_ptr_left))

/** set the ptr_left of a btree-node */
#define btree_node_set_ptr_left(btp, r)    btp->_ptr_left=ham_h2db_offset(r)

/** get a btree_node_t from a Page */
#define page_get_btree_node(p)      ((btree_node_t *)p->get_payload())

/**
 * find the child page for a key
 *
 * @return returns the child page in @a page_ref
 * @remark if @a idxptr is a valid pointer, it will store the anchor index
 *    of the loaded page
 */
extern ham_status_t
btree_traverse_tree(Page **page_ref, ham_s32_t *idxptr,
                    Database *db, Page *page, ham_key_t *key);

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
extern ham_s32_t
btree_node_search_by_key(Database *db, Page *page, ham_key_t *key,
        ham_u32_t flags);

/**
 * get entry @a i of a btree node
 */
#define btree_node_get_key(db, node, i)               \
  ((btree_key_t *)&((const char *)(node)->_entries)         \
      [(db_get_keysize(db)+db_get_int_key_header_size())*(i)])

/**
 * get offset of entry @a i - add this to page->get_self() for
 * the absolute offset of the key in the file
 */
#define btree_node_get_key_offset(page, i)              \
   ((page)->get_self()+Page::sizeof_persistent_header+      \
   OFFSETOF(btree_node_t, _entries)                 \
   /* ^^^ sizeof(btree_key_t) WITHOUT THE -1 !!! */ +         \
   (db_get_int_key_header_size()+db_get_keysize((page)->get_db()))*(i))

/**
 * get the slot of an element in the page
 * also returns the comparison value in cmp; if *cmp == 0 then the keys are
 * equal
 */
extern ham_status_t
btree_get_slot(Database *db, Page *page,
    ham_key_t *key, ham_s32_t *slot, int *cmp);

/**
 * calculate the "maxkeys" values
 */
extern ham_size_t
btree_calc_maxkeys(ham_size_t pagesize, ham_u16_t keysize);

/**
 * compare a public key (ham_key_t, LHS) to an internal key indexed in a
 * page
 *
 * @return -1, 0, +1 or higher positive values are the result of a successful
 *     key comparison (0 if both keys match, -1 when LHS < RHS key, +1
 *     when LHS > RHS key).
 *
 * @return values less than -1 are @ref ham_status_t error codes and indicate
 *     a failed comparison execution: these are listed in
 *     @ref ham_status_codes .
 *
 * @sa ham_status_codes
 */
extern int
btree_compare_keys(Database *db, Page *page,
        ham_key_t *lhs, ham_u16_t rhs);

/**
 * create a preliminary copy of an @ref btree_key_t key to a @ref ham_key_t
 * in such a way that @ref db->compare_keys can use the data and optionally
 * call @ref db->get_extended_key on this key to obtain all key data, when this
 * is an extended key.
 *
 * @param which specifies whether keydata1 (which = 0) or keydata2 is used
 * to store the pointer in the backend. The pointers are kept to avoid
 * permanent re-allocations (improves performance)
 *
 * Used in conjunction with @ref btree_release_key_after_compare
 */
extern ham_status_t
btree_prepare_key_for_compare(Database *db, int which, btree_key_t *src,
        ham_key_t *dest);

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
 */
extern ham_status_t
btree_read_key(Database *db, Transaction *txn, btree_key_t *source,
        ham_key_t *dest);

/**
 * read a record
 *
 * @param rid same as record->_rid, if key is not TINY/SMALL. Otherwise,
 * and if HAM_DIRECT_ACCESS is set, we use the rid-pointer to the
 * original record ID
 *
 * flags: either 0 or HAM_DIRECT_ACCESS
 */
extern ham_status_t
btree_read_record(Database *db, Transaction *txn, ham_record_t *record,
        ham_u64_t *ridptr, ham_u32_t flags);

/**
 * copy a key
 *
 * returns 0 if memory can not be allocated, or a pointer to @a dest.
 * uses ham_mem_malloc() - memory in dest->key has to be freed by the caller
 *
 * @a dest must have been initialized before calling this function; the
 * dest->data space will be reused when the specified size is large enough;
 * otherwise the old dest->data will be ham_mem_free()d and a new space
 * allocated.
 *
 * This can save superfluous heap free+allocation actions in there.
 *
 * @note
 * This routine can cope with HAM_KEY_USER_ALLOC-ated 'dest'-inations.
 *
 * @note
 * When an error is returned the 'dest->data'
 * pointer is either NULL or still pointing at allocated space (when
 * HAM_KEY_USER_ALLOC was not set).
 */
extern ham_status_t
btree_copy_key_int2pub(Database *db, const btree_key_t *source,
        ham_key_t *dest);

} // namespace ham

#endif /* HAM_BTREE_H__ */
