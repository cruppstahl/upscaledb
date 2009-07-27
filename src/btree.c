/**
 * Copyright (C) 2005-2008 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 *
 *
 * implementation of btree.h
 *
 */

#include "config.h"

#include <string.h>
#include "db.h"
#include "error.h"
#include "btree.h"
#include "keys.h"
#include "page.h" /* [i_a] */


ham_status_t 
btree_get_slot(ham_db_t *db, ham_page_t *page, 
        ham_key_t *key, ham_s32_t *slot, int *pcmp)
{
    int cmp = -1;
    btree_node_t *node=ham_page_get_btree_node(page);
    ham_s32_t r=btree_node_get_count(node)-1;
    ham_s32_t l=1;
    ham_s32_t i;
    ham_s32_t last;

    /*
     * perform a binary search for the *smallest* element, which 
     * is >= the key
     */
    last = MAX_KEYS_PER_NODE + 1;

    ham_assert(btree_node_get_count(node)>0, 
            ("node is empty"));

    /*
     * only one element in this node?
     */
    if (r==0) {
        cmp=key_compare_pub_to_int(page, key, 0);
        if (db_get_error(db))
            return (db_get_error(db));
        *slot=cmp<0 ? -1 : 0;
        goto bail;
    }

    for (;;) { // [i_a] compare is not needed     (while (r>=0))
        /* get the median item; if it's identical with the "last" item, 
         * we've found the slot */
        i=(l+r)/2;

        if (i==last) {
            *slot=i;
			cmp=1;
			ham_assert(i >= 0, (0));
			ham_assert(i < MAX_KEYS_PER_NODE + 1, (0));
            break;
        }
        
        /* compare it against the key */
        cmp=key_compare_pub_to_int(page, key, (ham_u16_t)i);
        if (db_get_error(db))
            return (db_get_error(db));

        /* found it? */
        if (cmp==0) {
            *slot=i;
            break;
        }

        /* if the key is bigger than the item: search "to the left" */
        if (cmp<0) {
            if (r==0) {
				ham_assert(i == 0, (0));
                *slot=-1;
                break;
            }
            r=i-1;
        }
        else {
            last=i;
            l=i+1;
        }
    }
    
bail:
    if (pcmp /* && *slot!=-1 */) {
		/*
		   [i_a] reduced the total number of key comparisons; this one is not 
		         needed any more, as it was only really required to 
                 compensate for the (i==last) conditional jump above.
				 So we can simply use 'cmp' as-is.
		*/
		*pcmp = cmp;
	}

    return (0);
}

static ham_size_t
my_calc_maxkeys(ham_size_t pagesize, ham_u16_t keysize)
{
    ham_size_t p, k, max;

    /* 
     * a btree page is always P bytes long, where P is the pagesize of 
     * the database. 
     */
    p=pagesize;

    /* every btree page has a header where we can't store entries */
    p-=OFFSETOF(btree_node_t, _entries);

    /* every page has a header where we can't store entries */
    p -= db_get_persistent_header_size();

    /*
     * compute the size of a key, k. 
     */
    k = keysize + db_get_int_key_header_size();

    /* 
     * make sure that MAX is an even number, otherwise we can't calculate
     * MIN (which is MAX/2)
     */
    max=p/k;
    return (max&1 ? max-1 : max);
}

static ham_status_t
my_fun_calc_keycount(ham_btree_t *be, ham_size_t *maxkeys, ham_u16_t keysize)
{
    ham_db_t *db=btree_get_db(be);

	if (keysize == 0)
	{
	    *maxkeys=btree_get_maxkeys(be);
	}
	else
	{
		/* 
		 * prevent overflow - maxkeys only has 16 bit! 
		 */
		*maxkeys=my_calc_maxkeys(db_get_pagesize(db), keysize);
		if (*maxkeys>MAX_KEYS_PER_NODE) {
			ham_trace(("keysize/pagesize ratio too high"));
			return (db_set_error(db, HAM_INV_KEYSIZE));
		}
	}

	return (0);
}

static ham_status_t 
my_fun_create(ham_btree_t *be, ham_u16_t keysize, ham_u32_t flags)
{
    ham_page_t *root;
    ham_size_t maxkeys;
    ham_db_t *db=btree_get_db(be);
    db_indexdata_t *indexdata=db_get_indexdata_ptr(db, 
                                db_get_indexdata_offset(db));

    /* 
     * prevent overflow - maxkeys only has 16 bit! 
     */
    maxkeys=my_calc_maxkeys(db_get_pagesize(db), keysize);
    if (maxkeys>MAX_KEYS_PER_NODE) {
        ham_trace(("keysize/pagesize ratio too high"));
        return (db_set_error(db, HAM_INV_KEYSIZE));
    }

    /*
     * allocate a new root page
     */
    root=db_alloc_page(db, PAGE_TYPE_B_ROOT, PAGE_IGNORE_FREELIST);
    if (!root)
        return (db_get_error(db));

    memset(page_get_raw_payload(root), 0, 
            sizeof(btree_node_t)+sizeof(ham_perm_page_union_t));

    /*
     * calculate the maximum number of keys for this page, 
     * and make sure that this number is even
     */
    btree_set_maxkeys(be, maxkeys);
    be_set_dirty(be, HAM_TRUE);
    be_set_keysize(be, keysize);
    be_set_flags(be, flags);

    btree_set_rootpage(be, page_get_self(root));

    index_set_max_keys(indexdata, maxkeys);
    index_set_keysize(indexdata, keysize);
    index_set_self(indexdata, page_get_self(root));
    index_set_flags(indexdata, flags);
    index_set_recno(indexdata, 0);
    db_set_dirty(db, 1);

    return (0);
}

static ham_status_t 
my_fun_open(ham_btree_t *be, ham_u32_t flags)
{
    ham_offset_t rootadd, recno;
    ham_u16_t maxkeys, keysize;
    ham_db_t *db=btree_get_db(be);
    db_indexdata_t *indexdata=db_get_indexdata_ptr(db, 
                                    db_get_indexdata_offset(db));

    /*
     * load root address and maxkeys (first two bytes are the
     * database name)
     */
    maxkeys = index_get_max_keys(indexdata);
    keysize = index_get_keysize(indexdata);
    rootadd = index_get_self(indexdata);
    flags = index_get_flags(indexdata);
    recno = index_get_recno(indexdata);

    btree_set_rootpage(be, rootadd);
    btree_set_maxkeys(be, maxkeys);
    be_set_keysize(be, keysize);
    be_set_flags(be, flags);
    be_set_recno(be, recno);

    return (0);
}

static ham_status_t
my_fun_flush(ham_btree_t *be)
{
    ham_db_t *db=btree_get_db(be);
    db_indexdata_t *indexdata=db_get_indexdata_ptr(db, 
                        db_get_indexdata_offset(db));

    /*
     * nothing todo if the backend was not touched
     */
    if (!be_is_dirty(be))
        return (0);

    /*
     * store root address and maxkeys (first two bytes are the
     * database name)
     */
    index_set_max_keys(indexdata, btree_get_maxkeys(be));
    index_set_keysize(indexdata, be_get_keysize(be));
    index_set_self(indexdata, btree_get_rootpage(be));
    index_set_flags(indexdata, be_get_flags(be));
    index_set_recno(indexdata, be_get_recno(be));

    db_set_dirty(db, HAM_TRUE);
    be_set_dirty(be, HAM_FALSE);

    return (0);
}

static ham_status_t
my_fun_close(ham_btree_t *be)
{
    /*
     * just flush the backend info if it's dirty
     */
    return (my_fun_flush(be));
}

static void
my_fun_delete(ham_btree_t *be)
{
    /*
     * nothing to do
     */
}

ham_status_t
btree_create(ham_btree_t *btree, ham_db_t *db, ham_u32_t flags)
{
    memset(btree, 0, sizeof(ham_btree_t));
    btree->_db=db;
    btree->_fun_create=my_fun_create;
    btree->_fun_open=my_fun_open;
    btree->_fun_close=my_fun_close;
    btree->_fun_flush=my_fun_flush;
    btree->_fun_delete=my_fun_delete;
    btree->_fun_find=btree_find;
    btree->_fun_insert=btree_insert;
    btree->_fun_erase=btree_erase;
    btree->_fun_enumerate=btree_enumerate;
#ifdef HAM_ENABLE_INTERNAL
    btree->_fun_check_integrity=btree_check_integrity;
    btree->_fun_calc_keycount=my_fun_calc_keycount;
#else
    btree->_fun_check_integrity=0;
	btree->_fun_calc_keycount=0;
#endif
    return (0);
}

ham_page_t *
btree_traverse_tree(ham_db_t *db, ham_page_t *page, 
        ham_key_t *key, ham_s32_t *idxptr)
{
    ham_status_t st;
    ham_s32_t slot;
    int_key_t *bte;
    btree_node_t *node=ham_page_get_btree_node(page);

    /*
     * make sure that we're not in a leaf page, and that the 
     * page is not empty
     */
    ham_assert(btree_node_get_count(node)>0, (0));
    ham_assert(btree_node_get_ptr_left(node)!=0, (0));

    st=btree_get_slot(db, page, key, &slot, 0);
    if (st)
        return (0);

    if (idxptr)
        *idxptr=slot;

    if (slot==-1)
        return (db_fetch_page(db, btree_node_get_ptr_left(node), 0));
    else {
        bte=btree_node_get_key(db, node, slot);
        ham_assert(key_get_flags(bte)==0 || 
                key_get_flags(bte)==KEY_IS_EXTENDED,
                ("invalid key flags 0x%x", key_get_flags(bte)));
        return (db_fetch_page(db, key_get_ptr(bte), 0));
    }
}

ham_s32_t 
btree_node_search_by_key(ham_db_t *db, ham_page_t *page, ham_key_t *key, 
                    ham_u32_t flags)
{
    int cmp; // [i_a]
    ham_s32_t slot;
    ham_status_t st;
    btree_node_t *node=ham_page_get_btree_node(page);

    db_set_error(db, 0);

	/* ensure the approx flag is NOT set by anyone yet */
	key_set_flags(key, key_get_flags(key) & ~KEY_IS_APPROXIMATE);

    if (btree_node_get_count(node)==0)
        return (-1);

    st=btree_get_slot(db, page, key, &slot, &cmp);
    if (st) {
        db_set_error(db, st);
        return (-1);
    }

	/*
	   'approximate matching'

	    When we get here and cmp != 0 and we're looking for LT/GT/LEQ/GEQ 
        key matches, this is where we need to do our prep work.

	    Yes, due to the flag tweak in a caller when we have (the usual) 
        multi-page DB table B+tree, both LT and GT flags are 'ON' here, 
        but let's not get carried way and assume that is always
	    like that. To elaborate a bit here: it may seem like doing something 
        simple the hard way, but in here, we do NOT know if there are 
        adjacent pages, so 'edge cases' like the scenarios 1, 2, and 5 below 
        should NOT return an error KEY_NOT_FOUND but instead produce a 
        valid slot AND, most important, the accompanying 'sign' (LT/GT) flags 
        for that slot, so that the outer call can analyze our response and 
        shift the key index into the left or right adjacent page, when such 
        is available. We CANNOT see that here, so we always should work with 
        both LT+GT enabled here.
	    And to make matters a wee bit more complex still: the one exception 
        to the above is when we have a single-page table: then we get 
        the actual GT/LT flags in here, as we're SURE there won't be any 
        left or right neighbour pages for us to shift into when the need 
        arrises.

	    Anyway, the purpose of the next section is to see if we have a 
        matching 'approximate' key AND feed the 'sign' (i.e. LT(-1) or 
        GT(+1)) back to the caller, who knows _exactly_ what the
	    user asked for and can thus take the proper action there.

	    Here, we are only concerned about determining which key index we 
        should produce, IFF we should produce a matching key.

	    Assume the following page layout, with two keys (values 2 and 4):

	  * index:
      *    [0]   [1]  
	  * +-+---+-+---+-+
	  * | | 2 | | 4 | |
	  * +-+---+-+---+-+

	    Various scenarios apply. For the key search (key ~ 1) i.e. (key=1, 
        flags=NEAR), we get this:

	    cmp = -1;
	    slot = -1;

	    hence we point here:

      *  |
      *  V
	  * +-+---+-+---+-+
	  * | | 2 | | 4 | |
	  * +-+---+-+---+-+

	    which is not a valid spot. Should we return a key? YES, since no key 
        is less than '1', but there exists a key '2' which fits as NEAR allows 
        for both LT and GT. Hence, this should be modified to become

	    slot=0
		sign=GT

      *     | ( slot++ )
      *     V
	  * +-+---+-+---+-+
	  * | | 2 | | 4 | |
	  * +-+---+-+---+-+


	    Second scenario: key <= 1, i.e. (key=1, flags=LEQ)
	    which gives us the same as above:

	   cmp = -1;
	   slot = -1;

	    hence we point here:

      *  |
      *  V
	  * +-+---+-+---+-+
	  * | | 2 | | 4 | |
	  * +-+---+-+---+-+

	    Should we return a valid slot by adjusting? Your common sense says 
        NO, but the correct answer is YES, since (a) we do not know if the 
        user asked this, as _we_ see it in here as 'key ~ 1' anyway and 
        we must allow the caller to adjust the slot by moving it into the 
        left neighbour page -- an action we cannot do as we do not know, 
        in here, whether there's more pages adjacent to this one we're 
        currently looking at.

	    EXCEPT... the common sense answer 'NO' is CORRECT when we have a 
        single-page db table in our hands; see the remark at the top of this 
        comment section; in that case, we can safely say 'NO' after all.

	    Third scenario: key ~ 3
	    which gives us either:

	   cmp = -1;
	   slot = 1;

	     or

	   cmp = 1;
	   slot = 0;

	     As we check for NEAR instead of just LT or GT, both are okay like 
         that, no adjustment needed.
	     All we need to do is make sure sure we pass along the proper LT/GT 
         'sign' flags for outer level result processing.

      
	    Fourth scenario: key < 3

	    again, we get either:

	   cmp = -1;
	   slot = 1;

	     or

	   cmp = 1;
	   slot = 0;

        but this time around, since we are looking for LT, we'll need to 
        adjust the second result, when that happens by slot++ and sending 
        the appropriate 'sign' flags.
	
	  Fifth scenario: key ~ 5

	    which given us:

	   cmp = -1;
	   slot = 1;

	     hence we point here:

      *           |
      *           V
	  * +-+---+-+---+-+
	  * | | 2 | | 4 | |
	  * +-+---+-+---+-+

	    Should we return this valid slot? Yup, as long as we mention that 
        it's an LT(less than) key; the caller can see that we returned the 
        slot as the upper bound of this page and adjust accordingly when
	    the actual query was 'key > 5' instead of 'key ~ 5' which is how we 
        get to see it.
	*/
	/*
	  Note that we have a 'preference' for LT answers in here; IFF the user'd 
        asked NEAR questions, most of the time that would give him LT answers, 
        i.e. the answers to NEAR ~ LT questions -- mark the word 'most' in 
        there: this is not happening when we're ending up at a page's lower 
        bound.
     */
	if (cmp)
	{
		/*
		 When slot == -1, you're in a special situation: you do NOT know what 
         the comparison with slot[-1] delivers, because there _is_ _no_ slot 
         -1, but you _do_ know what slot[0] delivered: 'cmp' is the
		 value for that one then.
		 */
		if (slot < 0) 
			slot = 0;

		ham_assert(slot <= btree_node_get_count(node) - 1, (0));

		if (flags & HAM_FIND_LT_MATCH)
		{
			if (cmp < 0)
			{
				/* key @ slot is LARGER than the key we search for ... */
				if (slot > 0)
				{
					slot--;
					key_set_flags(key, key_get_flags(key) | KEY_IS_LT);
					cmp = 0;
				}
				else if (flags & HAM_FIND_GT_MATCH)
				{
					ham_assert(slot == 0, (0));
					key_set_flags(key, key_get_flags(key) | KEY_IS_GT);
					cmp = 0;
				}
			}
			else
			{
				/* key @ slot is SMALLER than the key we search for */
				ham_assert(cmp > 0, (0));
				key_set_flags(key, key_get_flags(key) | KEY_IS_LT);
				cmp = 0;
			}
		}
		else if (flags & HAM_FIND_GT_MATCH)
		{
			/*
			 When we get here, we're sure HAM_FIND_LT_MATCH is NOT set...
			 */
			ham_assert(!(flags & HAM_FIND_LT_MATCH), (0));

			if (cmp < 0)
			{
				/* key @ slot is LARGER than the key we search for ... */
				key_set_flags(key, key_get_flags(key) | KEY_IS_GT);
				cmp = 0;
			}
			else
			{
				/* key @ slot is SMALLER than the key we search for */
				ham_assert(cmp > 0, (0));
				if (slot < btree_node_get_count(node) - 1)
				{
					slot++;
					key_set_flags(key, key_get_flags(key) | KEY_IS_GT);
					cmp = 0;
				}
			}
		}
	}

	if (cmp)
        return (-1);

    return (slot);
}

