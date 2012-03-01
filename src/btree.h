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


/**
 * the backend structure for a b+tree 
 *
 * @remark doesn't need packing, because it's not persistent; 
 * see comment before ham_backend_t for an explanation.
 */
struct ham_btree_t;
typedef struct ham_btree_t ham_btree_t;

#include "packstart.h"

HAM_PACK_0 struct HAM_PACK_1 ham_btree_t 
{
    /** the common declarations of all backends */
    BACKEND_DECLARATIONS(ham_btree_t);

    /** address of the root-page */
    ham_offset_t _rootpage;

    /** maximum keys in an internal page */
    ham_u16_t _maxkeys;

    /**
     * two pointers for managing key data; these pointers are used to
     * avoid frequent mallocs in key_compare_pub_to_int() etc
     */
    void *_keydata1;
    void *_keydata2;

} HAM_PACK_2;

#include "packstop.h"

/** get the address of the root node */
#define btree_get_rootpage(be)          (ham_db2h_offset((be)->_rootpage))

/** set the address of the root node */
#define btree_set_rootpage(be, rp)      (be)->_rootpage=ham_h2db_offset(rp)

/** get maximum number of keys per (internal) node */
#define btree_get_maxkeys(be)           (ham_db2h16((be)->_maxkeys))

/** set maximum number of keys per (internal) node */
#define btree_set_maxkeys(be, s)        (be)->_maxkeys=ham_h2db16(s)

/** getter for keydata1 */
#define btree_get_keydata1(be)          (be)->_keydata1

/** setter for keydata1 */
#define btree_set_keydata1(be, p)       (be)->_keydata1=(p)

/** getter for keydata2 */
#define btree_get_keydata2(be)          (be)->_keydata2

/** setter for keydata2 */
#define btree_set_keydata2(be, p)       (be)->_keydata2=(p)

/** a macro for getting the minimum number of keys */
#define btree_get_minkeys(maxkeys)      (maxkeys/2)

/** defines the maximum number of keys per node */
#define MAX_KEYS_PER_NODE				0xFFFFU /* max(ham_u16_t) */


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
#define btree_node_get_count(btp)            (ham_db2h16(btp->_count))

/** set the number of entries of a btree-node */
#define btree_node_set_count(btp, c)         btp->_count=ham_h2db16(c)

/** get the left sibling of a btree-node */
#define btree_node_get_left(btp)             (ham_db2h_offset(btp->_left))

/** check if a btree node is a leaf node */
#define btree_node_is_leaf(btp)              (!(btree_node_get_ptr_left(btp)))

/** set the left sibling of a btree-node */
#define btree_node_set_left(btp, l)          btp->_left=ham_h2db_offset(l)

/** get the right sibling of a btree-node */
#define btree_node_get_right(btp)            (ham_db2h_offset(btp->_right))

/** set the right sibling of a btree-node */
#define btree_node_set_right(btp, r)         btp->_right=ham_h2db_offset(r)

/** get the ptr_left of a btree-node */
#define btree_node_get_ptr_left(btp)         (ham_db2h_offset(btp->_ptr_left))

/** set the ptr_left of a btree-node */
#define btree_node_set_ptr_left(btp, r)      btp->_ptr_left=ham_h2db_offset(r)

/** get a btree_node_t from a Page */
#define page_get_btree_node(p)          ((btree_node_t *)p->get_payload())

/**
 * "constructor" - initializes a new ham_btree_t object
 *
 * @return a pointer to a new created B+-tree backend 
 *
 * @remark flags are from @ref ham_env_open_db() or @ref ham_env_create_db()
 */
extern ham_backend_t *
btree_create(Database *db, ham_u32_t flags);

/**
 * search the btree structures for a record
 *
 * @remark this function returns HAM_SUCCESS and returns 
 * the record ID @a rid, if the @a key was found; otherwise 
 * an error code is returned 
 *
 * @remark this function is exported through the backend structure.
 */
extern ham_status_t 
btree_find(ham_btree_t *be, ham_key_t *key,
           ham_record_t *record, ham_u32_t flags);

/**
 * same as above, but sets the cursor to the position
 */
extern ham_status_t 
btree_find_cursor(ham_btree_t *be, btree_cursor_t *cursor, 
           ham_key_t *key, ham_record_t *record, ham_u32_t flags);

/**
 * insert a new tuple (key/record) in the tree
 */
extern ham_status_t
btree_insert(ham_btree_t *be, ham_key_t *key, 
        ham_record_t *record, ham_u32_t flags);

/**
 * same as above, but sets the cursor position to the new item
 */
extern ham_status_t
btree_insert_cursor(ham_btree_t *be, ham_key_t *key, 
        ham_record_t *record, btree_cursor_t *cursor, ham_u32_t flags);

/**
 * erase a key from the tree
 */
extern ham_status_t
btree_erase(ham_btree_t *be, ham_key_t *key, ham_u32_t flags);

/**
 * same as above, but with a coupled cursor
 */
extern ham_status_t
btree_erase_cursor(ham_btree_t *be, ham_key_t *key, btree_cursor_t *cursor, 
        ham_u32_t flags);

/**
 * same as above, but assumes that the cursor is coupled to a leaf page 
 * and the key can be removed without rebalancing the tree
 */
extern ham_status_t
btree_cursor_erase_fasttrack(ham_btree_t *be, btree_cursor_t *cursor);

/**
 * same as above, but only erases a single duplicate
 */
extern ham_status_t
btree_erase_duplicate(ham_btree_t *be, ham_key_t *key, ham_u32_t dupe_id, 
        ham_u32_t flags);

/**
 * enumerate all items
 */
extern ham_status_t
btree_enumerate(ham_btree_t *be, ham_enumerate_cb_t cb,
        void *context);

/**                                                                 
 * verify the whole tree                                            
 */                                                                 
extern ham_status_t
btree_check_integrity(ham_btree_t *be);

/**
 * find the child page for a key
 *
 * @return returns the child page in @a page_ref
 * @remark if @a idxptr is a valid pointer, it will store the anchor index 
 *      of the loaded page
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
 *         another negative @ref ham_status_codes value when an 
 *         unexpected error occurred.
 */
extern ham_s32_t 
btree_node_search_by_key(Database *db, Page *page, ham_key_t *key, 
                ham_u32_t flags);

/**
 * get entry @a i of a btree node
 */
#define btree_node_get_key(db, node, i)                             \
    ((btree_key_t *)&((const char *)(node)->_entries)                 \
            [(db_get_keysize(db)+db_get_int_key_header_size())*(i)])

/**
 * get offset of entry @a i - add this to page->get_self() for
 * the absolute offset of the key in the file
 */
#define btree_node_get_key_offset(page, i)                          \
     ((page)->get_self()+Page::sizeof_persistent_header+            \
     OFFSETOF(btree_node_t, _entries)                               \
     /* ^^^ sizeof(btree_key_t) WITHOUT THE -1 !!! */ +               \
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
 * close all cursors in this Database
 */
extern ham_status_t 
btree_close_cursors(Database *db, ham_u32_t flags);

/**
 * compare a public key (ham_key_t, LHS) to an internal key indexed in a
 * page
 *
 * @return -1, 0, +1 or higher positive values are the result of a successful 
 *         key comparison (0 if both keys match, -1 when LHS < RHS key, +1 
 *         when LHS > RHS key).
 *
 * @return values less than -1 are @ref ham_status_t error codes and indicate 
 *         a failed comparison execution: these are listed in 
 *         @ref ham_status_codes .
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
btree_read_key(Database *db, btree_key_t *source, ham_key_t *dest);

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
btree_read_record(Database *db, ham_record_t *record, ham_u64_t *rid,
                ham_u32_t flags);

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


#endif /* HAM_BTREE_H__ */
