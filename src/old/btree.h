/**
 * Copyright (C) 2005, 2006 Christoph Rupp (chris@crupp.de)
 * see file COPYING for licence information
 *
 * the btree-backend
 *
 */

#ifndef HAM_BTREE_H__
#define HAM_BTREE_H__

#ifdef __cplusplus
extern "C" {
#endif 

#include "backend.h"

/**
 * the backend structure for a b+tree 
 */
struct ham_btree_t;
typedef struct ham_btree_t ham_btree_t;
struct ham_btree_t
{
    /**
     * the common declaratons of all backends
     */
    BACKEND_DECLARATIONS(ham_btree_t)

    /**
     * address of the root-page 
     */
    ham_offset_t _rootpage;
};

/**
 * a macro for getting the minimum number of keys
 */
#define btree_get_minkeys(maxkeys)      (maxkeys/2)

/**
 * convenience macro to get the database pointer of a ham_btree_t-structure
 */
#define btree_get_db(be)                (be)->_db

/**
 * get the address of the root node
 */
#define btree_get_rootpage(be)          (ham_db2h_offset((be)->_rootpage))

/**
 * set the address of the root node
 */
#define btree_set_rootpage(be, rp)      (be)->_rootpage=ham_h2db_offset(rp)

#include "packstart.h"

/**
 * an entry in a btree-node
 */
typedef HAM_PACK_0 struct HAM_PACK_1 btree_entry_t
{
    /**
     * the pointer of this entry
     */
    ham_offset_t _ptr;

    /**
     * the size of this entry
     */
    ham_u16_t _keysize;

    /**
     * the key
     */
    ham_u8_t _key[1];
    
} HAM_PACK_2 btree_entry_t;

/**
 * get the pointer of an btree-entry
 */
#define btree_entry_get_ptr(bte)                (ham_db2h_offset(bte->_ptr))

/**
 * set the pointer of an btree-entry
 */
#define btree_entry_set_ptr(bte, p)             bte->_ptr=ham_h2db_offset(p)

/**
 * get the size of an btree-entry
 */
#define btree_entry_get_size(bte)               (ham_db2h16(bte->_keysize))

/**
 * set the size of an btree-entry
 */
#define btree_entry_set_size(bte, s)            (bte)->_keysize=ham_h2db16(s)

/**
 * get the real size of the btree-entry
 */
#define btree_entry_get_real_size(db, bte)      \
       (btree_entry_get_size(bte)<db_get_keysize(db) \
        ? btree_entry_get_size(bte) \
        : db_get_keysize(db))

/**
 * get the flags of an btree-entry
 */
#define btree_entry_get_flags(bte)              (0)

/**
 * set the flags of an btree-entry
 */
#define btree_entry_set_flags(bte, f)           (void)

/**
 * get a pointer to the key of a btree-entry
 */
#define btree_entry_get_key(bte)                (bte->_key)

/**
 * a btree-node; it spans the persistent part of a ham_page_t:
 * btree_node_t *btp=(btree_node_t *)page->_u._pers;
 */
typedef HAM_PACK_0 struct HAM_PACK_1 btree_node_t
{
    /**
     * flags of this node - flags are always the first member
     * of every page - regardless of the backend
     */
    ham_u16_t _flags;

    /**
     * number of used entries in the node
     */
    ham_u16_t _count;

    /**
     * address of left sibling
     */
    ham_offset_t _left;

    /**
     * address of right sibling
     */
    ham_offset_t _right;

    /**
     * address of child node whose items are smaller than all items 
     * in this node 
     */
    ham_offset_t _ptr_left;

    /**
     * the entries of this node
     */
    btree_entry_t _entries[1];

} HAM_PACK_2 btree_node_t;

#include "packstop.h"

/**
 * get the number of entries of a btree-node
 */
#define btree_node_get_count(btp)            (ham_db2h16(btp->_count))

/**
 * set the number of entries of a btree-node
 */
#define btree_node_set_count(btp, c)         btp->_count=ham_h2db16(c)

/**
 * get the left sibling of a btree-node
 */
#define btree_node_get_left(btp)             (ham_db2h_offset(btp->_left))

/*
 * check if a btree node is a leaf node
 */
#define btree_node_is_leaf(btp)              (!(btree_node_get_ptr_left(btp)))

/**
 * set the left sibling of a btree-node
 */
#define btree_node_set_left(btp, l)          btp->_left=ham_h2db_offset(l)

/**
 * get the right sibling of a btree-node
 */
#define btree_node_get_right(btp)            (ham_db2h_offset(btp->_right))

/**
 * set the right sibling of a btree-node
 */
#define btree_node_set_right(btp, r)         btp->_right=ham_h2db_offset(r)

/**
 * get the ptr_left of a btree-node
 */
#define btree_node_get_ptr_left(btp)         (ham_db2h_offset(btp->_ptr_left))

/**
 * set the ptr_left of a btree-node
 */
#define btree_node_set_ptr_left(btp, r)      btp->_ptr_left=ham_h2db_offset(r)

/**
 * get a btree_node_t from a ham_page_t
 */
#define ham_page_get_btree_node(p)           ((btree_node_t *)p->_pers._payload)

/**
 * "constructor" - initializes a new ham_btree_t object
 *
 * @remark returns a status code
 * @remark flags are from ham_open() or ham_create()
 */
extern ham_status_t
btree_create(ham_btree_t *btree, ham_db_t *db, ham_u32_t flags);

/**
 * get entry #i of a btree node
 */
#define btree_node_get_entry(db, node, i)                           \
    ((btree_entry_t *)&((const char *)(node)->_entries)             \
            [(db_get_keysize((db))+sizeof(btree_entry_t)-1)*(i)])

/**
 * search a node for an key
 *
 * @return returns the index of the entry, with the first index being 1, 
 * the second index 2 etc. returns 0 if no match was found.
 *
 * @remark to check for errors, use db_get_error(). if an error is set,
 * the function failed.
 */
extern ham_u32_t
btree_node_search_by_key(ham_db_t *db, ham_page_t *page, ham_key_t *key);

/**
 * search a node for a pointer
 *
 * @return returns the index of the entry, with the first index being 1, 
 * the second index 2 etc. returns 0 if no match was found.
 */
extern ham_u32_t
btree_node_search_by_ptr(ham_db_t *db, btree_node_t *node, ham_offset_t ptr);

/**
 * search the btree structures for a record
 *
 * @remark this function returns HAM_SUCCESS and returns 
 * the record ID @rid, if the @key was found; otherwise 
 * an error code is returned 
 *
 * @remark this function is exported through the backend structure.
 */
extern ham_status_t 
btree_find(ham_btree_t *be, ham_txn_t *txn, ham_key_t *key, 
        ham_offset_t *rid, ham_u32_t flags);

/**
 * search a btree node for a key, and load the child node, if available.
 * returns the child node or 0 on error
 */
extern ham_page_t *
btree_find_child(ham_db_t *db, ham_txn_t *txn, ham_page_t *page, 
        ham_key_t *key);

/**
 * same as above, but returns the index of the child's anchor
 * entry in @a index
 */
extern ham_page_t *
btree_find_child2(ham_db_t *db, ham_txn_t *txn, ham_page_t *page, 
        ham_key_t *key, long *index);

/**
 * only returns the index of the child's anchor, but does not load the page
 */
extern long
btree_get_slot(ham_db_t *db, ham_page_t *page, ham_key_t *key);

/**
 * insert a new tuple (key/record) in the tree
 */
extern ham_status_t
btree_insert(ham_btree_t *be, ham_txn_t *txn, ham_key_t *key, 
        ham_record_t *record, ham_u32_t flags);

/**
 * erase a key from the tree
 */
extern ham_status_t
btree_erase(ham_btree_t *be, ham_txn_t *txn, ham_key_t *key, 
        ham_offset_t *rid, ham_u32_t flags);

/**
 * dump the whole tree to stdout
 */
extern ham_status_t
btree_dump(ham_btree_t *be, ham_txn_t *txn, ham_dump_cb_t cb);

/**
 * verify the whole tree
 */
extern ham_status_t
btree_check_integrity(ham_btree_t *be, ham_txn_t *txn);

#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_BTREE_H__ */
