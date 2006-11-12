/**
 * Copyright (C) 2005, 2006 Christoph Rupp (chris@crupp.de)
 * see file LICENSE for licence information
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
#include "keys.h"

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

    /**
     * maximum keys in an internal page 
     */
    ham_u16_t _maxkeys;

};

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

/* 
 * get maximum number of keys per (internal) node 
 */
#define btree_get_maxkeys(be)           (ham_db2h16((be)->_maxkeys))

/* 
 * set maximum number of keys per (internal) node 
 */
#define btree_set_maxkeys(be, s)        (be)->_maxkeys=ham_h2db16(s)

/**
 * a macro for getting the minimum number of keys
 *
 * note that we're relaxing the BTree rule - pages are now merged
 * when they have less than 4 keys. but i'm not really convinced that 
 * this has performance advantages...
#define btree_get_minkeys(maxkeys)      (maxkeys/2 < 4 ? maxkeys/2 : 4)
 */
#define btree_get_minkeys(maxkeys)      (maxkeys/2)


#include "packstart.h"

/**
 * a btree-node; it spans the persistent part of a ham_page_t:
 * btree_node_t *btp=(btree_node_t *)page->_u._pers.payload;
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
    key_t _entries[1];

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
#define ham_page_get_btree_node(p)      ((btree_node_t *)p->_pers->_s._payload)

/**
 * "constructor" - initializes a new ham_btree_t object
 *
 * @remark returns a status code
 * @remark flags are from ham_open() or ham_create()
 */
extern ham_status_t
btree_create(ham_btree_t *btree, ham_db_t *db, ham_u32_t flags);

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
           ham_record_t *record, ham_u32_t flags);

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
        ham_offset_t *rid, ham_u32_t *intflags, ham_u32_t flags);

/**
 * enumerate all items
 */
extern ham_status_t
btree_enumerate(ham_btree_t *be, ham_txn_t *txn, ham_enumerate_cb_t cb,
        void *context);

/**
 * verify the whole tree
 */
extern ham_status_t
btree_check_integrity(ham_btree_t *be, ham_txn_t *txn);

/**
 * find the child page for a key
 *
 * @return returns the child page
 * @remark if idxptr is a valid pointer, it will store the anchor index of the 
 *      loaded page
 */
extern ham_page_t *
btree_traverse_tree(ham_db_t *db, ham_txn_t *txn, ham_page_t *page, 
        ham_key_t *key, ham_s32_t *idxptr);

/**
 * search a leaf node for a key
 *
 * !!!
 * only works with leaf nodes!!
 *
 * @return returns the index of the key, or -1 if the key was not found
 */
extern ham_s32_t 
btree_node_search_by_key(ham_db_t *db, ham_txn_t *txn, 
        ham_page_t *page, ham_key_t *key);

/**
 * get entry #i of a btree node
 */
#define btree_node_get_key(db, node, i)                             \
    ((key_t *)&((const char *)(node)->_entries)                     \
            [(db_get_keysize((db))+sizeof(key_t)-1)*(i)])

/*
 * get the slot of an element in the page
 */
ham_status_t 
btree_get_slot(ham_db_t *db, ham_txn_t *txn, ham_page_t *page, 
        ham_key_t *key, ham_s32_t *slot);


#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_BTREE_H__ */
