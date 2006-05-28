/**
 * Copyright 2005, 2006 Christoph Rupp (chris@crupp.de)
 * see file LICENSE for license and copyright information
 *
 * btree searching
 *
 */

#include <string.h>
#include <ham/config.h>
#include "db.h"
#include "error.h"
#include "page.h"
#include "btree.h"
#include "mem.h"
#include "util.h"
#include "blob.h"

/*
 * the insert_scratchpad_t structure helps us to propagate return values
 * from the bottom of the tree to the root.
 */
typedef struct
{
    /*
     * the backend pointer
     */
    ham_btree_t *be;

    /*
     * the flags of the ham_insert()-call
     */
    ham_u32_t flags;

    /*
     * the transaction object
     */
    ham_txn_t *txn;

    /*
     * the record which is inserted
     */
    ham_record_t *record;

    /*
     * a key; this is used to propagate SMOs (structure modification
     * operations) from a child page to a parent page
     */
    ham_key_t key;

    /*
     * a RID; this is used to propagate SMOs (structure modification
     * operations) from a child page to a parent page
     */
    ham_offset_t rid;

} insert_scratchpad_t;

/*
 * return values
 */
#define SPLIT     1

/*
 * flags for my_insert_nosplit()
 */
#define NOFLUSH   1
#define OVERWRITE 2

/*
 * this is the function which does most of the work - traversing to a 
 * leaf, inserting the key using my_insert_in_page() 
 * and performing necessary SMOs. it works recursive.
 */
static ham_status_t
my_insert_recursive(ham_page_t *page, ham_key_t *key, 
        ham_offset_t rid, insert_scratchpad_t *scratchpad);

/*
 * this function inserts a key in a page
 */
static ham_status_t
my_insert_in_page(ham_page_t *page, ham_key_t *key, 
        ham_offset_t rid, ham_u32_t flags, 
        insert_scratchpad_t *scratchpad);

/*
 * insert a key in a page; the page MUST have free slots
 */
ham_status_t
my_insert_nosplit(ham_page_t *page, ham_txn_t *txn, ham_key_t *key, 
        ham_offset_t rid, ham_record_t *record, ham_u32_t flags);

/*
 * split a page and insert the new element
 */
static ham_status_t
my_insert_split(ham_page_t *page, ham_key_t *key, 
        ham_offset_t rid, ham_u32_t flags, 
        insert_scratchpad_t *scratchpad);


ham_status_t
btree_insert(ham_btree_t *be, ham_txn_t *txn, ham_key_t *key, 
        ham_record_t *record, ham_u32_t flags)
{
    ham_page_t *root;
    ham_status_t st;
    ham_offset_t rootaddr;
    ham_db_t *db=btree_get_db(be);
    insert_scratchpad_t scratchpad;

    /* 
     * initialize the scratchpad 
     */
    memset(&scratchpad, 0, sizeof(scratchpad));
    scratchpad.be=be;
    scratchpad.txn=txn;
    scratchpad.flags=flags;
    scratchpad.record=record;

    /* 
     * get the root-page...
     */
    rootaddr=btree_get_rootpage(be);
    ham_assert(rootaddr!=0, 0, 0);
    root=txn_fetch_page(scratchpad.txn, rootaddr, 0);

    /* 
     * ... and start the recursion 
     */
    st=my_insert_recursive(root, key, 0, &scratchpad);

    /*
     * if the root page was split, we have to create a new
     * root page.
     */
    if (st==SPLIT) {
        ham_page_t *newroot;
        btree_node_t *node;

        /*
         * allocate a new root page
         */
        newroot=txn_alloc_page(txn, 0); 

        /* 
         * insert the pivot element and the ptr_left
         */ 
        node=ham_page_get_btree_node(newroot);
        btree_node_set_ptr_left(node, rootaddr);
        /* TODO error check? */
        st=my_insert_nosplit(newroot, scratchpad.txn, &scratchpad.key, 
                scratchpad.rid, scratchpad.record, NOFLUSH);

        /*
         * set the new root page TODO was ist wenn st==error?
         */
        btree_set_rootpage(be, page_get_self(newroot));
        db_set_dirty(db, 1);
    }

    /*
     * release the scratchpad-memory
     */
    if (scratchpad.key.data)
        ham_mem_free(scratchpad.key.data);

    /* 
     * and return to caller
     */
    return (st);
}

static ham_status_t
my_insert_recursive(ham_page_t *page, ham_key_t *key, 
        ham_offset_t rid, insert_scratchpad_t *scratchpad)
{
    ham_status_t st;
    ham_page_t *child;
    ham_db_t *db=page_get_owner(page);
    btree_node_t *node=ham_page_get_btree_node(page);

    /*
     * if we've reached a leaf: insert the key
     */
    if (btree_node_is_leaf(node)) 
        return (my_insert_in_page(page, key, rid, 0, scratchpad));

    /*
     * otherwise traverse the root down to the leaf
     */
    child=btree_find_child(db, scratchpad->txn, page, key);
    ham_assert(child!=0, "guru meditation error", 0);

    /*
     * and call this function recursively
     */
    st=my_insert_recursive(child, key, rid, scratchpad);
    switch (st) {
        /*
         * if we're done, we're done
         */
        case HAM_SUCCESS:
            break;

        /*
         * if we tried to insert a duplicate key, we're done, too
         */
        case HAM_DUPLICATE_KEY:
            break;

        /*
         * the child was split, and we have to insert a new (key/rid)-tuple.
         */
        case SPLIT:
            st=my_insert_in_page(page, &scratchpad->key, 
                        scratchpad->rid, OVERWRITE, scratchpad);
            break;

        /*
         * every other return value is unexpected and shouldn't happen
         */
        default:
            db_set_error(db, st);
            break;
    }

    return (st);
}

static ham_status_t
my_insert_in_page(ham_page_t *page, ham_key_t *key, 
        ham_offset_t rid, ham_u32_t flags, 
        insert_scratchpad_t *scratchpad)
{
    ham_size_t maxkeys=db_get_maxkeys(page_get_owner(page));
    btree_node_t *node=ham_page_get_btree_node(page);

    ham_assert(maxkeys>1, "invalid result of db_get_maxkeys(): %d", maxkeys);

    /*
     * if we can insert the new key without splitting the page: 
     * my_insert_nosplit() will do the work for us
     */
    if (btree_node_get_count(node)<maxkeys) 
        return (my_insert_nosplit(page, scratchpad->txn, key, rid, 
                    scratchpad->record, flags));

    /*
     * otherwise, we have to split the page.
     * but BEFORE we split, we check if the key already exists!
     */
    if (btree_node_is_leaf(node)) {
        if (btree_node_search_by_key(page_get_owner(page), page, key)) {
            if (flags&OVERWRITE) 
                return (my_insert_nosplit(page, scratchpad->txn, key, rid, 
                        scratchpad->record, flags));
            else
                return (HAM_DUPLICATE_KEY);
        }
    }
    return (my_insert_split(page, key, rid, flags, scratchpad));
}

ham_status_t
my_insert_nosplit(ham_page_t *page, ham_txn_t *txn, ham_key_t *key, 
        ham_offset_t rid, ham_record_t *record, ham_u32_t flags)
{
    int cmp;
    ham_size_t i, count, keysize;
    btree_entry_t *bte;
    ham_db_t *db=page_get_owner(page);
    btree_node_t *node=ham_page_get_btree_node(page);
    ham_status_t st;
    ham_offset_t blobid=0;

    count=btree_node_get_count(node);
    keysize=db_get_keysize(db);

    /*
     * TODO this is subject to optimization...
     */
    for (i=0; i<count; i++) {
        bte=btree_node_get_entry(db, node, i);

        cmp=db_compare_keys(db, page,
                i, btree_entry_get_flags(bte), btree_entry_get_key(bte), 
                btree_entry_get_real_size(db, bte), btree_entry_get_size(bte),
                -1, key->_flags, key->data, key->size, key->size);
        if (db_get_error(db))
            return (db_get_error(db));

        /*
         * key exists already
         */
        if (cmp==0) {
            if (flags&OVERWRITE) {
                /* TODO no need to overwrite the key - it already exists! */
                /* ATTENTION with extended keys! need to be overwritten, too */
                /*memcpy(bte->_key, key->data, keysize);
                btree_entry_set_size(bte, key->size);
                page_set_dirty(page, 1);*/
                return (HAM_SUCCESS);
            }
            else
                return (HAM_DUPLICATE_KEY);
        }

        /*
         * we found the first key which is > then the new key
         */
        if (cmp>0) {
            /*
             * if we're in the leaf: insert the blob, and append the blob-id 
             * in this node; otherwise just insert the entry (rid)
             */
            if (btree_node_is_leaf(node)) {
                st=blob_allocate(db, txn, record->data, record->size, 0, &rid);
                if (st)
                    return (st);
            }

            /*
             * shift all keys one position to the right
             */
            memmove(((char *)bte)+sizeof(btree_entry_t)-1+keysize, bte,
                    (sizeof(btree_entry_t)-1+keysize)*(count-i));

            /*
             * also shift the extended keys, if we've written such a key
             */
            if (blobid) {
                ham_ext_key_t *extkey=page_get_extkeys(page);
                memmove(&extkey[i+1], &extkey[i],
                        sizeof(ham_ext_key_t)*(count-i));
            }

            /*
             * overwrite the old key, increment the page counter, set the 
             * dirty flag and return success
             */
            memcpy(bte->_key, key->data, key->size);
            if (btree_node_is_leaf(node) && key->size>db_get_keysize(db)) {
                ham_offset_t *p;
                ham_u8_t *prefix=bte->_key;
                blobid=db_ext_key_insert(db, txn, page, key);
                if (!blobid)
                    return (db_get_error(db));
                p=(ham_offset_t *)(prefix+(db_get_keysize(db)-
                        sizeof(ham_offset_t)));
                *p=ham_db2h_offset(blobid);
            }
            btree_entry_set_ptr(bte, rid);
            btree_entry_set_size(bte, key->size);
            page_set_dirty(page, 1);
            btree_node_set_count(node, count+1);
            return (HAM_SUCCESS);
        }
    }

    /*
     * if we're in the leaf: insert the blob, and append the blob-id 
     * in this node; otherwise just append the entry (rid)
     */
    if (btree_node_is_leaf(node)) {
        st=blob_allocate(db, txn, record->data, record->size, 0, &rid);
        if (st)
            return (st);
    }

    /*
     * we insert the extended key, if necessary
     * TODO evil code duplication!!
     */
    bte=btree_node_get_entry(db, node, count);
    memcpy(bte->_key, key->data, key->size);
    if (btree_node_is_leaf(node) && key->size>db_get_keysize(db)) {
        ham_offset_t *p;
        ham_u8_t *prefix=bte->_key;
        blobid=db_ext_key_insert(db, txn, page, key);
        if (!blobid)
            return (db_get_error(db));
        p=(ham_offset_t *)(prefix+(db_get_keysize(db)-
                sizeof(ham_offset_t)));
        *p=ham_db2h_offset(blobid);
    }
    btree_entry_set_ptr(bte, rid);
    btree_entry_set_size(bte, key->size);
    page_set_dirty(page, 1);
    btree_node_set_count(node, count+1);
    return (HAM_SUCCESS);
}

static ham_status_t
my_insert_split(ham_page_t *page, ham_key_t *key, 
        ham_offset_t rid, ham_u32_t flags, 
        insert_scratchpad_t *scratchpad)
{
    int cmp;
    ham_status_t st;
    ham_page_t *newpage, *oldsib;
    btree_entry_t *nbte, *obte;
    btree_node_t *nbtp, *obtp, *sbtp;
    ham_size_t count, pivot, keysize;
    ham_db_t *db=page_get_owner(page);
    ham_key_t pivotkey, oldkey;
    ham_offset_t pivotrid;

    keysize=db_get_keysize(db);

    /*
     * allocate a new page
     */
    newpage=txn_alloc_page(scratchpad->txn, 0); 

    /*
     * move half of the key/rid-tuples to the new page
     */
    nbtp=ham_page_get_btree_node(newpage);
    nbte=btree_node_get_entry(db, nbtp, 0);
    obtp=ham_page_get_btree_node(page);
    obte=btree_node_get_entry(db, obtp, 0);
    count=btree_node_get_count(obtp);
    pivot=count/2;

    /*
     * if old page has extended keys, we need them for the new page, too
     */
    if (page_get_extkeys(page)) {
        ham_ext_key_t *ext=(ham_ext_key_t *)ham_mem_alloc(db_get_maxkeys(db)*
                    sizeof(ham_ext_key_t));
        if (!ext) {
            db_set_error(db, HAM_OUT_OF_MEMORY);
            return (HAM_OUT_OF_MEMORY);
        }
        memset(ext, 0, db_get_maxkeys(db)*sizeof(ham_ext_key_t));
        page_set_extkeys(newpage, ext);
    }

    /*
     * if we split a leaf, we'll insert the pivot element in the leaf
     * page, too. in internal nodes, we don't insert it, but propagate
     * it to the parent node only.
     */
    if (btree_node_is_leaf(obtp)) {
        memcpy((char *)nbte,
               ((char *)obte)+(sizeof(btree_entry_t)-1+keysize)*pivot, 
               (sizeof(btree_entry_t)-1+keysize)*(count-pivot));
        if (page_get_extkeys(page)) {
            ham_ext_key_t *from, *to;
            from=page_get_extkeys(page);
            to=page_get_extkeys(newpage);
            memcpy(to, from+sizeof(ham_ext_key_t)*pivot,
                    sizeof(ham_ext_key_t)*(count-pivot));
            memset(from+sizeof(ham_ext_key_t)*pivot, 0, 
                    sizeof(ham_ext_key_t)*(count-pivot));
        }
    }
    else {
        memcpy((char *)nbte,
               ((char *)obte)+(sizeof(btree_entry_t)-1+keysize)*(pivot+1), 
               (sizeof(btree_entry_t)-1+keysize)*(count-pivot-1));
        if (page_get_extkeys(page)) {
            ham_ext_key_t *from, *to;
            from=page_get_extkeys(page);
            to=page_get_extkeys(newpage);
            memcpy(to, from+sizeof(ham_ext_key_t)*pivot+1,
                    sizeof(ham_ext_key_t)*(count-pivot-1));
            memset(from+sizeof(ham_ext_key_t)*pivot+1, 0, 
                    sizeof(ham_ext_key_t)*(count-pivot-1));
        }
    }
    
    /* 
     * store the pivot element, we'll need it later to propagate it 
     * to the parent page
     */
    nbte=btree_node_get_entry(db, obtp, pivot);

    oldkey.data=btree_entry_get_key(nbte);
    oldkey.size=btree_entry_get_size(nbte);
    if (!util_copy_key(&oldkey, &pivotkey)) {
        (void)page_io_free(scratchpad->txn, newpage);
        page_delete(newpage);
        db_set_error(db, HAM_OUT_OF_MEMORY);
        return (HAM_OUT_OF_MEMORY);
    }
    pivotrid=page_get_self(newpage);

    /*
     * adjust the page count
     */
    if (btree_node_is_leaf(obtp)) {
        btree_node_set_count(obtp, pivot);
        btree_node_set_count(nbtp, count-pivot);
    }
    else {
        btree_node_set_count(obtp, pivot);
        btree_node_set_count(nbtp, count-pivot-1);
    }

    /*
     * if we're in an internal page: fix the ptr_left of the new page
     * (it points to the ptr of the pivot key)
     */ 
    if (!btree_node_is_leaf(obtp)) {
        /* 
         * nbte still contains the pivot key 
         */
        btree_node_set_ptr_left(nbtp, btree_entry_get_ptr(nbte));
    }

    /*
     * insert the new element
     */
    cmp=db_compare_keys(db, page,
            pivot, btree_entry_get_flags(nbte), btree_entry_get_key(nbte), 
            btree_entry_get_real_size(db, nbte), btree_entry_get_size(nbte),
            -1, key->_flags, key->data, key->size, key->size);
    if (db_get_error(db))
        return (db_get_error(db));

    if (cmp<=0)
        st=my_insert_nosplit(newpage, scratchpad->txn, key, rid, 
                scratchpad->record, flags|NOFLUSH);
    else
        st=my_insert_nosplit(page, scratchpad->txn, key, rid, 
                scratchpad->record, flags|NOFLUSH);
    if (st)
        return (st);

    /*
     * fix the double-linked list of pages
     */
    if (btree_node_get_right(obtp)) 
        oldsib=txn_fetch_page(scratchpad->txn, btree_node_get_right(obtp), 0);
    else
        oldsib=0;
    btree_node_set_left (nbtp, page_get_self(page));
    btree_node_set_right(nbtp, btree_node_get_right(obtp));
    btree_node_set_right(obtp, page_get_self(newpage));
    if (oldsib) {
        sbtp=ham_page_get_btree_node(oldsib);
        btree_node_set_left(sbtp, page_get_self(newpage));
    }

    /*
     * mark the pages as dirty
     */
    if (oldsib) 
        page_set_dirty(oldsib, 1);
    page_set_dirty(newpage, 1);
    page_set_dirty(page, 1);

    /* 
     * propagate the pivot key to the parent page
     */
    if (scratchpad->key.data)
        ham_mem_free(scratchpad->key.data);
    scratchpad->key=pivotkey;
    scratchpad->rid=pivotrid;

    return (SPLIT);
}

