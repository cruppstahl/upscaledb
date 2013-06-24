/*
 * Copyright (C) 2005-2013 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

/**
 * @brief implementation of btree.h
 *
 */

#include "config.h"

#include <string.h>

#include "btree_index.h"
#include "db.h"
#include "env.h"
#include "error.h"
#include "extkeys.h"
#include "btree_key.h"
#include "mem.h"
#include "page.h"
#include "txn.h"
#include "cursor.h"
#include "btree_node.h"
#include "page_manager.h"

namespace hamsterdb {

/** defines the maximum number of keys per node */
#define MAX_KEYS_PER_NODE         0xFFFFU /* max(ham_u16_t) */

BtreeIndex::BtreeIndex(LocalDatabase *db, ham_u32_t descriptor, ham_u32_t flags)
  : m_db(db), m_keysize(0), m_descriptor_index(descriptor),
    m_flags(flags), m_root_address(0), m_maxkeys(0), m_keydata1(),
    m_keydata2(), m_statistics()
{
}

ham_status_t
BtreeIndex::get_slot(Page *page, ham_key_t *key, ham_s32_t *slot, int *pcmp)
{
  int cmp = -1;
  PBtreeNode *node = PBtreeNode::from_page(page);
  ham_s32_t r = node->get_count() - 1;
  ham_s32_t l = 1;
  ham_s32_t i;
  ham_s32_t last = MAX_KEYS_PER_NODE + 1;

  ham_assert(node->get_count() > 0);

  /* only one element in this node?  */
  if (r == 0) {
    cmp = compare_keys(page, key, 0);
    if (cmp < -1)
      return (ham_status_t)cmp;
    *slot = cmp < 0 ? -1 : 0;
    goto bail;
  }

  for (;;) {
    /* get the median item; if it's identical with the "last" item,
     * we've found the slot */
    i = (l + r) / 2;

    if (i == last) {
      *slot = i;
      cmp = 1;
      ham_assert(i >= 0);
      ham_assert(i < (int)MAX_KEYS_PER_NODE + 1);
      break;
    }

    /* compare it against the key */
    cmp = compare_keys(page, key, (ham_u16_t)i);
    if (cmp < -1)
      return ((ham_status_t)cmp);

    /* found it? */
    if (cmp == 0) {
      *slot = i;
      break;
    }

    /* if the key is bigger than the item: search "to the left" */
    if (cmp < 0) {
      if (r == 0) {
        ham_assert(i == 0);
        *slot = -1;
        break;
      }
      r = i - 1;
    }
    else {
      last = i;
      l = i + 1;
    }
  }

bail:
  if (pcmp)
    *pcmp = cmp;

  return (0);
}

ham_status_t
BtreeIndex::create(ham_u16_t keysize)
{
  ham_assert(keysize != 0);

  /* prevent overflow - maxkeys only has 16 bit! */
  ham_size_t maxkeys = calc_maxkeys(m_db->get_local_env()->get_pagesize(),
                  keysize);
  if (maxkeys > MAX_KEYS_PER_NODE) {
    ham_trace(("keysize/pagesize ratio too high"));
    return (HAM_INV_KEYSIZE);
  }
  else if (maxkeys == 0) {
    ham_trace(("keysize too large for the current pagesize"));
    return (HAM_INV_KEYSIZE);
  }

  /* allocate a new root page */
  Page *root = 0;
  ham_status_t st = m_db->get_local_env()->get_page_manager()->alloc_page(&root,
                        m_db, Page::kTypeBroot, PageManager::kIgnoreFreelist);
  if (st)
    return (st);

  memset(root->get_raw_payload(), 0, sizeof(PBtreeNode) + sizeof(PPageData));
  root->set_type(Page::kTypeBroot);
  root->set_dirty(true);

  /*
   * calculate the maximum number of keys for this page,
   * and make sure that this number is even
   */
  m_maxkeys = (ham_u16_t)maxkeys;
  m_keysize = keysize;
  m_root_address = root->get_address();

  flush_descriptor();

  return (0);
}

ham_status_t
BtreeIndex::open()
{
  ham_u64_t rootadd;
  ham_u16_t maxkeys;
  ham_u16_t keysize;
  ham_u32_t flags;
  PBtreeHeader *desc = m_db->get_local_env()->get_btree_descriptor(m_descriptor_index);

  /*
   * load root address and maxkeys (first two bytes are the
   * database name)
   */
  maxkeys = desc->get_maxkeys();
  keysize = desc->get_keysize();
  rootadd = desc->get_root_address();
  flags = desc->get_flags();

  ham_assert(maxkeys > 0);
  ham_assert(keysize > 0);
  ham_assert(rootadd > 0);

  m_maxkeys = maxkeys;
  m_root_address = rootadd;
  m_keysize = keysize;
  m_flags = flags;

  return (0);
}

void
BtreeIndex::flush_descriptor()
{
  if (m_db->get_rt_flags() & HAM_READ_ONLY)
    return;

  LocalEnvironment *env = m_db->get_local_env();

  PBtreeHeader *desc = env->get_btree_descriptor(m_descriptor_index);

  desc->set_dbname(m_db->get_name());
  desc->set_maxkeys(m_maxkeys);
  desc->set_keysize(get_keysize());
  desc->set_root_address(get_root_address());
  desc->set_flags(get_flags());

  env->mark_header_page_dirty();
}

ham_status_t
BtreeIndex::free_page_extkeys(Page *page, ham_u32_t flags)
{
  LocalDatabase *db = page->get_db();
  LocalEnvironment *env = db->get_local_env();

  /*
   * if this page has a header, and it's either a B-Tree root page or
   * a B-Tree index page: remove all extended keys from the cache,
   * and/or free their blobs
   */
  if (page->get_data()
      && (!(page->get_flags() & Page::kNpersNoHeader))
      && (page->get_type() == Page::kTypeBroot
        || page->get_type() == Page::kTypeBindex)) {
    ExtKeyCache *c = db->get_extkey_cache();
    PBtreeNode *node = PBtreeNode::from_page(page);

    for (ham_size_t i = 0; i < node->get_count(); i++) {
      PBtreeKey *bte = node->get_key(db, i);
      if (bte->get_flags() & PBtreeKey::kExtended) {
        ham_u64_t blobid = bte->get_extended_rid(db);
        if (env->get_flags() & HAM_IN_MEMORY) {
          /* delete the blobid to prevent that it's freed twice */
          *(ham_u64_t *)(bte->get_key() +
                (db->get_keysize() - sizeof(ham_u64_t))) = 0;
        }
        if (c)
          c->remove(blobid);
      }
    }
  }

  return (HAM_SUCCESS);
}

ham_status_t
BtreeIndex::find_internal(Page *page, ham_key_t *key, Page **pchild,
                ham_s32_t *idxptr)
{
  *pchild = 0;

  PBtreeNode *node = PBtreeNode::from_page(page);

  // make sure that we're not in a leaf page, and that the
  // page is not empty
  ham_assert(node->get_count() > 0);
  ham_assert(node->get_ptr_left() != 0);

  ham_s32_t slot;
  ham_status_t st = get_slot(page, key, &slot);
  if (st)
    return (st);

  if (idxptr)
    *idxptr = slot;

  if (slot == -1)
    return (m_db->get_local_env()->get_page_manager()->fetch_page(pchild,
                            m_db, node->get_ptr_left()));
  else {
    PBtreeKey *bte = node->get_key(m_db, slot);
    ham_assert(bte->get_flags() == 0
                || bte->get_flags() == PBtreeKey::kExtended);
    return (m_db->get_local_env()->get_page_manager()->fetch_page(pchild,
                            m_db, bte->get_ptr()));
  }
}

ham_s32_t
BtreeIndex::find_leaf(Page *page, ham_key_t *key, ham_u32_t flags)
{
  int cmp;
  PBtreeNode *node=PBtreeNode::from_page(page);

  /* ensure the approx flag is NOT set by anyone yet */
  ham_key_set_intflags(key, ham_key_get_intflags(key)
            & ~PBtreeKey::kApproximate);

  if (node->get_count() == 0)
    return (-1);

  ham_s32_t slot;
  ham_status_t st = get_slot(page, key, &slot, &cmp);
  if (st) {
    ham_assert(st < -1);
    return (st);
  }

  /*
   * 'approximate matching'
   *
   * When we get here and cmp != 0 and we're looking for LT/GT/LEQ/GEQ
   * key matches, this is where we need to do our prep work.
   *
   * Yes, due to the flag tweak in a caller when we have (the usual)
   * multi-page DB table B+tree, both LT and GT flags are 'ON' here,
   * but let's not get carried way and assume that is always
   * like that. To elaborate a bit here: it may seem like doing something
   * simple the hard way, but in here, we do NOT know if there are
   * adjacent pages, so 'edge cases' like the scenarios 1, 2, and 5 below
   * should NOT return an error KEY_NOT_FOUND but instead produce a
   * valid slot AND, most important, the accompanying 'sign' (LT/GT) flags
   * for that slot, so that the outer call can analyze our response and
   * shift the key index into the left or right adjacent page, when such
   * is available. We CANNOT see that here, so we always should work with
   * both LT+GT enabled here.
   * And to make matters a wee bit more complex still: the one exception
   * to the above is when we have a single-page table: then we get
   * the actual GT/LT flags in here, as we're SURE there won't be any
   * left or right neighbour pages for us to shift into when the need
   * arrises.
   *
   * Anyway, the purpose of the next section is to see if we have a
   * matching 'approximate' key AND feed the 'sign' (i.e. LT(-1) or
   * GT(+1)) back to the caller, who knows _exactly_ what the
   * user asked for and can thus take the proper action there.
   *
   * Here, we are only concerned about determining which key index we
   * should produce, IFF we should produce a matching key.
   *
   * Assume the following page layout, with two keys (values 2 and 4):
   *
   * index:
   *  [0]   [1]
   * +-+---+-+---+-+
   * | | 2 | | 4 | |
   * +-+---+-+---+-+
   *
   * Various scenarios apply. For the key search (key ~ 1) i.e. (key=1,
   * flags=NEAR), we get this:
   *
   * cmp = -1;
   * slot = -1;
   *
   * hence we point here:
   *
   *  |
   *  V
   * +-+---+-+---+-+
   * | | 2 | | 4 | |
   * +-+---+-+---+-+
   *
   * which is not a valid spot. Should we return a key? YES, since no key
   * is less than '1', but there exists a key '2' which fits as NEAR allows
   * for both LT and GT. Hence, this should be modified to become
   *
   * slot=0
   * sign=GT
   *
   *   | ( slot++ )
   *   V
   * +-+---+-+---+-+
   * | | 2 | | 4 | |
   * +-+---+-+---+-+
   *
   * Second scenario: key <= 1, i.e. (key=1, flags=LEQ)
   * which gives us the same as above:
   *
   *  cmp = -1;
   *  slot = -1;
   *
   * hence we point here:
   *
   *  |
   *  V
   * +-+---+-+---+-+
   * | | 2 | | 4 | |
   * +-+---+-+---+-+
   *
   * Should we return a valid slot by adjusting? Your common sense says
   * NO, but the correct answer is YES, since (a) we do not know if the
   * user asked this, as _we_ see it in here as 'key ~ 1' anyway and
   * we must allow the caller to adjust the slot by moving it into the
   * left neighbour page -- an action we cannot do as we do not know,
   * in here, whether there's more pages adjacent to this one we're
   * currently looking at.
   *
   * EXCEPT... the common sense answer 'NO' is CORRECT when we have a
   * single-page db table in our hands; see the remark at the top of this
   * comment section; in that case, we can safely say 'NO' after all.
   *
   * Third scenario: key ~ 3
   * which gives us either:
   *
   *  cmp = -1;
   *  slot = 1;
   *
   *  or
   *
   * cmp = 1;
   * slot = 0;
   *
   * As we check for NEAR instead of just LT or GT, both are okay like
   * that, no adjustment needed.
   * All we need to do is make sure sure we pass along the proper LT/GT
   * 'sign' flags for outer level result processing.
   *
   * Fourth scenario: key < 3
   *
   * again, we get either:
   *
   *  cmp = -1;
   *  slot = 1;
   *
   * or
   *
   *  cmp = 1;
   *  slot = 0;
   *
   * but this time around, since we are looking for LT, we'll need to
   * adjust the second result, when that happens by slot++ and sending
   * the appropriate 'sign' flags.
   *
   * Fifth scenario: key ~ 5
   *
   * which given us:
   *
   *  cmp = -1;
   *  slot = 1;
   *
   *  hence we point here:
   *
   *       |
   *       V
   * +-+---+-+---+-+
   * | | 2 | | 4 | |
   * +-+---+-+---+-+
   *
   * Should we return this valid slot? Yup, as long as we mention that
   * it's an LT(less than) key; the caller can see that we returned the
   * slot as the upper bound of this page and adjust accordingly when
   * the actual query was 'key > 5' instead of 'key ~ 5' which is how we
   * get to see it.
   *
   *
   * Note that we have a 'preference' for LT answers in here; IFF the user'd
   * asked NEAR questions, most of the time that would give him LT answers,
   * i.e. the answers to NEAR ~ LT questions -- mark the word 'most' in
   * there: this is not happening when we're ending up at a page's lower
   * bound.
   */
  if (cmp) {
    /*
     * When slot == -1, you're in a special situation: you do NOT know what
     * the comparison with slot[-1] delivers, because there _is_ _no_ slot
     * -1, but you _do_ know what slot[0] delivered: 'cmp' is the
     * value for that one then.
     */
    if (slot < 0)
      slot = 0;

    ham_assert(slot <= node->get_count() - 1);

    if (flags & HAM_FIND_LT_MATCH) {
      if (cmp < 0) {
        /* key @ slot is LARGER than the key we search for ... */
        if (slot > 0) {
          slot--;
          ham_key_set_intflags(key, ham_key_get_intflags(key)
                  | PBtreeKey::kLower);
          cmp = 0;
        }
        else if (flags & HAM_FIND_GT_MATCH) {
          ham_assert(slot == 0);
          ham_key_set_intflags(key, ham_key_get_intflags(key)
                  | PBtreeKey::kGreater);
          cmp = 0;
        }
      }
      else {
        /* key @ slot is SMALLER than the key we search for */
        ham_assert(cmp > 0);
        ham_key_set_intflags(key, ham_key_get_intflags(key)
                  | PBtreeKey::kLower);
        cmp = 0;
      }
    }
    else if (flags&HAM_FIND_GT_MATCH) {
      /* When we get here, we're sure HAM_FIND_LT_MATCH is NOT set... */
      ham_assert(!(flags&HAM_FIND_LT_MATCH));

      if (cmp < 0) {
        /* key @ slot is LARGER than the key we search for ... */
        ham_key_set_intflags(key, ham_key_get_intflags(key)
                  | PBtreeKey::kGreater);
        cmp = 0;
      }
      else
      {
        /* key @ slot is SMALLER than the key we search for */
        ham_assert(cmp > 0);
        if (slot < node->get_count() - 1) {
          slot++;
          ham_key_set_intflags(key, ham_key_get_intflags(key)
                  | PBtreeKey::kGreater);
          cmp = 0;
        }
      }
    }
  }

  if (cmp)
    return (-1);

  ham_assert(slot >= -1);
  return (slot);
}

ham_status_t
BtreeIndex::prepare_key_for_compare(int which, PBtreeKey *src,
                ham_key_t *dest)
{
  ByteArray *arena;

  if (!(src->get_flags() & PBtreeKey::kExtended)) {
    dest->size = src->get_size();
    dest->data = src->get_key();
    dest->flags = HAM_KEY_USER_ALLOC;
    dest->_flags = src->get_flags();
    return (0);
  }

  dest->size = src->get_size();
  arena = which ? get_keyarena2() : get_keyarena1();
  arena->resize(dest->size);

  if (!arena->get_ptr()) {
    dest->data = 0;
    return (HAM_OUT_OF_MEMORY);
  }

  memcpy(arena->get_ptr(), src->get_key(), get_keysize());
  dest->data    = arena->get_ptr();
  dest->_flags |= PBtreeKey::kExtended;
  dest->flags  |= HAM_KEY_USER_ALLOC;

  return (0);
}

int
BtreeIndex::compare_keys(Page *page, ham_key_t *lhs, ham_u16_t rhs_int)
{
  PBtreeNode *node = PBtreeNode::from_page(page);
  ham_key_t rhs = {0};

  ham_assert(m_db == page->get_db());

  PBtreeKey *r = node->get_key(m_db, rhs_int);

  /* for performance reasons, we follow two branches:
   * if the key is not extended, then immediately compare it.
   * otherwise (if it's extended) use prepare_key_for_compare()
   * to allocate the extended key and compare it.
   */
  if (!(r->get_flags() & PBtreeKey::kExtended)) {
    rhs.size = r->get_size();
    rhs.data = r->get_key();
    rhs.flags = HAM_KEY_USER_ALLOC;
    rhs._flags = r->get_flags();
    return (m_db->compare_keys(lhs, &rhs));
  }

  /* otherwise continue for extended keys */
  ham_status_t st = prepare_key_for_compare(0, r, &rhs);
  if (st)
    return (st);

  return (m_db->compare_keys(lhs, &rhs));
}

ham_status_t
BtreeIndex::read_key(Transaction *txn, PBtreeKey *source, ham_key_t *dest)
{
  ByteArray *arena = (txn == 0 || (txn->get_flags() & HAM_TXN_TEMPORARY))
            ? &m_db->get_key_arena()
            : &txn->get_key_arena();

  /*
   * extended key: copy the whole key, not just the
   * overflow region!
   */
  if (source->get_flags() & PBtreeKey::kExtended) {
    ham_u16_t keysize = source->get_size();
    ham_status_t st = m_db->get_extended_key(source->get_key(),
                  keysize, source->get_flags(), dest);
    if (st) {
      /* if db->get_extended_key() allocated memory: Release it and
       * make sure that there's no leak
       */
      if (!(dest->flags & HAM_KEY_USER_ALLOC)) {
        if (dest->data && arena->get_ptr() != dest->data)
           Memory::release(dest->data);
        dest->data = 0;
      }
      return (st);
    }

    ham_assert(dest->data != 0);

    if (!(dest->flags&HAM_KEY_USER_ALLOC)) {
      if (keysize)
        arena->assign(dest->data, dest->size);
      else
        dest->data = 0;
    }
  }
  /* code path below is for a non-extended key */
  else {
    ham_u16_t keysize = source->get_size();

    if (keysize) {
      if (dest->flags & HAM_KEY_USER_ALLOC)
        memcpy(dest->data, source->get_key(), keysize);
      else {
        arena->resize(keysize);
        dest->data = arena->get_ptr();
        memcpy(dest->data, source->get_key(), keysize);
      }
    }
    else {
      if (!(dest->flags & HAM_KEY_USER_ALLOC))
        dest->data = 0;
    }

    dest->size = keysize;
  }

  /*
   * recno databases: recno is stored in db-endian!
   */
  if (m_db->get_rt_flags() & HAM_RECORD_NUMBER) {
    ham_u64_t recno;
    ham_assert(dest->data != 0);
    ham_assert(dest->size == sizeof(ham_u64_t));
    recno = *(ham_u64_t *)dest->data;
    recno = ham_db2h64(recno);
    memcpy(dest->data, &recno, sizeof(ham_u64_t));
  }

  return (HAM_SUCCESS);
}

ham_status_t
BtreeIndex::read_record(Transaction *txn, ham_u64_t *ridptr,
                ham_record_t *record, ham_u32_t flags)
{
  bool noblob = false;
  ham_size_t blobsize;

  ByteArray *arena = (txn == 0 || (txn->get_flags() & HAM_TXN_TEMPORARY))
            ? &m_db->get_record_arena()
            : &txn->get_record_arena();

  /* if this key has duplicates: fetch the duplicate entry */
  if (record->_intflags & PBtreeKey::kDuplicates) {
    PDupeEntry entry;
    ham_status_t st = m_db->get_local_env()->get_duplicate_manager()->get(
                                record->_rid, 0, &entry);
    if (st)
      return st;
    record->_intflags = dupe_entry_get_flags(&entry);
    record->_rid = dupe_entry_get_rid(&entry);
    /* ridptr must not point to entry._rid because it's on the stack! */
    ridptr = &record->_rid;
  }

  /*
   * if the record size is small enough there's
   * no blob available, but the data is stored compressed in the record's
   * offset.
   */
  if (record->_intflags & PBtreeKey::kBlobSizeTiny) {
    /* the highest byte of the record id is the size of the blob */
    char *p = (char *)ridptr;
    blobsize = p[sizeof(ham_u64_t) - 1];
    noblob = true;
  }
  else if (record->_intflags & PBtreeKey::kBlobSizeSmall) {
    /* record size is sizeof(ham_u64_t) */
    blobsize = sizeof(ham_u64_t);
    noblob = true;
  }
  else if (record->_intflags & PBtreeKey::kBlobSizeEmpty) {
    /* record size is 0 */
    blobsize = 0;
    noblob = true;
  }
  else {
    /* set to a dummy value, so the third if-branch is executed */
    blobsize = 0xffffffff;
  }

  if (noblob && blobsize == 0) {
    record->size = 0;
    record->data = 0;
  }
  else if (noblob && blobsize > 0) {
    if (flags & HAM_PARTIAL) {
      ham_trace(("flag HAM_PARTIAL is not allowed if record->size <= 8"));
      return (HAM_INV_PARAMETER);
    }

    if (!(record->flags & HAM_RECORD_USER_ALLOC)
        && (flags & HAM_DIRECT_ACCESS)) {
      record->data = ridptr;
      record->size = blobsize;
    }
    else {
      if (!(record->flags & HAM_RECORD_USER_ALLOC)) {
        arena->resize(blobsize);
        record->data = arena->get_ptr();
      }
      memcpy(record->data, ridptr, blobsize);
      record->size = blobsize;
    }
  }
  else if (!noblob && blobsize != 0) {
    ByteArray *arena = (txn == 0 || (txn->get_flags() & HAM_TXN_TEMPORARY))
                            ? &m_db->get_record_arena()
                            : &txn->get_record_arena();

    return (m_db->get_local_env()->get_blob_manager()->read(m_db,
                            record->_rid, record, flags, arena));
  }

  return (HAM_SUCCESS);
}

ham_status_t
BtreeIndex::copy_key(const PBtreeKey *source, ham_key_t *dest)
{
  /* extended key: copy the whole key */
  if (source->get_flags() & PBtreeKey::kExtended) {
    ham_status_t st = m_db->get_extended_key((ham_u8_t *)source->get_key(),
          source->get_size(), source->get_flags(), dest);
    if (st)
      return st;
    /* dest->size is set by db->get_extended_key() */
    ham_assert(dest->size == source->get_size());
    ham_assert(dest->data != 0);
  }
  else if (source->get_size()) {
    if (!(dest->flags & HAM_KEY_USER_ALLOC)) {
      if (!dest->data || dest->size < source->get_size()) {
        Memory::release(dest->data);
        dest->data = Memory::allocate<ham_u8_t>(source->get_size());
        if (!dest->data)
          return (HAM_OUT_OF_MEMORY);
      }
    }

    memcpy(dest->data, source->get_key(), source->get_size());
    dest->size = source->get_size();
  }
  else {
    /* key.size is 0 */
    if (!(dest->flags & HAM_KEY_USER_ALLOC)) {
      Memory::release(dest->data);
      dest->data = 0;
    }
    dest->size = 0;
    dest->data = 0;
  }

  dest->flags = 0;

  return (HAM_SUCCESS);
}

ham_size_t
BtreeIndex::calc_maxkeys(ham_size_t pagesize, ham_u16_t keysize)
{
  /* adjust page size and key size by adding the overhead */
  pagesize -= PBtreeNode::get_entry_offset();
  pagesize -= Page::sizeof_persistent_header;
  keysize += (ham_u16_t)PBtreeKey::kSizeofOverhead;

  /* and return an even number */
  ham_size_t max = pagesize / keysize;
  return (max & 1 ? max - 1 : max);
}

//
// visitor object for estimating / counting the number of keys
///
class CalcKeysVisitor : public BtreeVisitor {
  public:
    CalcKeysVisitor(LocalDatabase *db, ham_u32_t flags)
      : m_db(db), m_flags(flags), m_count(0) {
    }

    virtual ham_status_t item(PBtreeNode *node, PBtreeKey *key) {
      ham_size_t dupcount = 1;
      ham_status_t st;

      if (m_flags & HAM_SKIP_DUPLICATES
          || (m_db->get_rt_flags() & HAM_ENABLE_DUPLICATES) == 0) {
        m_count += node->get_count();
        return (BtreeVisitor::kSkipPage);
      }

      if (key->get_flags() & PBtreeKey::kDuplicates) {
        st = m_db->get_local_env()->get_duplicate_manager()->get_count(
                                key->get_ptr(), &dupcount, 0);
        if (st)
          return (st);
        m_count += dupcount;
      }
      else {
        m_count++;
      }
      return (0);
    }

    ham_u64_t get_key_count() const {
      return (m_count);
    }

  private:
    LocalDatabase *m_db;
    ham_u32_t m_flags;
    ham_u64_t m_count;
};

ham_status_t
BtreeIndex::get_key_count(ham_u32_t flags, ham_u64_t *pkeycount)
{
  *pkeycount = 0;

  CalcKeysVisitor visitor(m_db, flags);
  ham_status_t st = enumerate(&visitor);
  if (st)
    return (st);

  *pkeycount = visitor.get_key_count();

  return (0);
}

//
// visitor object to free all allocated blobs
///
class FreeBlobsVisitor : public BtreeVisitor {
  public:
    FreeBlobsVisitor(LocalDatabase *db)
      : m_db(db) {
    }

    // also look at internal nodes, not just the leafs
    virtual bool visit_internal_nodes() const {
      return (true);
    }

    virtual ham_status_t item(PBtreeNode *node, PBtreeKey *key) {
      ham_status_t st = 0;

      if (key->get_flags() & PBtreeKey::kExtended) {
        ham_u64_t blobid = key->get_extended_rid(m_db);
        /* delete the extended key */
        st = m_db->remove_extkey(blobid);
        if (st)
          return (st);
      }

      if (key->get_flags() & (PBtreeKey::kBlobSizeTiny
                | PBtreeKey::kBlobSizeSmall
                | PBtreeKey::kBlobSizeEmpty))
        return (0);

      /* if we're in the leaf page, delete the blob */
      if (node->is_leaf())
        st = key->erase_record(m_db, 0, 0, true);

      return (st);
    }

  private:
    LocalDatabase *m_db;
};

ham_status_t
BtreeIndex::free_all_blobs()
{
  FreeBlobsVisitor visitor(m_db);
  return (enumerate(&visitor));
}

} // namespace hamsterdb
