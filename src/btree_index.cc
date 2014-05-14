/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "config.h"

#include <string.h>

#include "db.h"
#include "env.h"
#include "error.h"
#include "mem.h"
#include "page.h"
#include "txn.h"
#include "cursor.h"
#include "page_manager.h"
#include "btree_index.h"
#include "btree_index_factory.h"
#include "btree_node_proxy.h"

namespace hamsterdb {

ham_u64_t BtreeIndex::ms_btree_smo_split = 0;
ham_u64_t BtreeIndex::ms_btree_smo_merge = 0;
ham_u64_t BtreeIndex::ms_btree_smo_shift = 0;

BtreeIndex::BtreeIndex(LocalDatabase *db, ham_u32_t descriptor, ham_u32_t flags,
                ham_u32_t key_type, ham_u32_t key_size)
  : m_db(db), m_key_size(0), m_key_type(key_type), m_rec_size(0),
    m_descriptor_index(descriptor), m_flags(flags), m_root_address(0)
{
  m_leaf_traits = BtreeIndexFactory::create(db, flags, key_type,
                  key_size, true);
  m_internal_traits = BtreeIndexFactory::create(db, flags, key_type,
                  key_size, false);
}

void
BtreeIndex::create(ham_u16_t key_type, ham_u32_t key_size, ham_u32_t rec_size)
{
  ham_assert(key_size != 0);

  /* allocate a new root page */
  Page *root = m_db->get_local_env()->get_page_manager()->alloc_page(m_db,
                    Page::kTypeBroot, PageManager::kClearWithZero);

  // initialize the new page
  PBtreeNode *node = PBtreeNode::from_page(root);
  node->set_flags(PBtreeNode::kLeafNode);

  m_key_size = key_size;
  m_key_type = key_type;
  m_rec_size = rec_size;
  m_root_address = root->get_address();

  flush_descriptor();
}

void
BtreeIndex::open()
{
  ham_u64_t rootadd;
  ham_u16_t key_size;
  ham_u16_t key_type;
  ham_u32_t flags;
  ham_u32_t rec_size;
  PBtreeHeader *desc = m_db->get_local_env()->get_btree_descriptor(m_descriptor_index);

  key_size = desc->get_key_size();
  key_type = desc->get_key_type();
  rec_size = desc->get_record_size();
  rootadd = desc->get_root_address();
  flags = desc->get_flags();

  ham_assert(key_size > 0);
  ham_assert(rootadd > 0);

  m_root_address = rootadd;
  m_key_size = key_size;
  m_key_type = key_type;
  m_flags = flags;
  m_rec_size = rec_size;
}

void
BtreeIndex::flush_descriptor()
{
  if (m_db->get_rt_flags() & HAM_READ_ONLY)
    return;

  LocalEnvironment *env = m_db->get_local_env();

  PBtreeHeader *desc = env->get_btree_descriptor(m_descriptor_index);

  desc->set_dbname(m_db->get_name());
  desc->set_key_size(get_key_size());
  desc->set_rec_size(get_record_size());
  desc->set_key_type(get_key_type());
  desc->set_root_address(get_root_address());
  desc->set_flags(get_flags());

  env->mark_header_page_dirty();
}

Page *
BtreeIndex::find_child(Page *page, ham_key_t *key, ham_s32_t *idxptr)
{
  BtreeNodeProxy *node = get_node_from_page(page);

  // make sure that we're not in a leaf page, and that the
  // page is not empty
  ham_assert(node->get_ptr_down() != 0);

  ham_u64_t record_id;
  int slot = node->find_child(key, &record_id);

  if (idxptr)
    *idxptr = slot;

#ifdef HAM_DEBUG
  if (slot >= 0) {
    ham_u32_t flags = node->test_get_flags(slot);
    flags &= ~(BtreeKey::kInitialized
                    | BtreeKey::kExtendedKey
                    | BtreeKey::kCompressed);
    ham_assert(flags == 0);
  }
#endif

  return (m_db->get_local_env()->get_page_manager()->fetch_page(m_db,
                    record_id));
}

ham_s32_t
BtreeIndex::find_leaf(Page *page, ham_key_t *key, ham_u32_t flags)
{
  /* ensure the approx flag is NOT set by anyone yet */
  ham_key_set_intflags(key, ham_key_get_intflags(key)
            & ~BtreeKey::kApproximate);

  BtreeNodeProxy *node = get_node_from_page(page);
  if (node->get_count() == 0)
    return (-1);

  int cmp;
  int slot = node->find_child(key, 0, &cmp);

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

    ham_assert(slot <= (int)node->get_count() - 1);

    if (flags & HAM_FIND_LT_MATCH) {
      if (cmp < 0) {
        /* key @ slot is LARGER than the key we search for ... */
        if (slot > 0) {
          slot--;
          ham_key_set_intflags(key, ham_key_get_intflags(key)
                  | BtreeKey::kLower);
          cmp = 0;
        }
        else if (flags & HAM_FIND_GT_MATCH) {
          ham_assert(slot == 0);
          ham_key_set_intflags(key, ham_key_get_intflags(key)
                  | BtreeKey::kGreater);
          cmp = 0;
        }
      }
      else {
        /* key @ slot is SMALLER than the key we search for */
        ham_assert(cmp > 0);
        ham_key_set_intflags(key, ham_key_get_intflags(key)
                  | BtreeKey::kLower);
        cmp = 0;
      }
    }
    else if (flags & HAM_FIND_GT_MATCH) {
      /* When we get here, we're sure HAM_FIND_LT_MATCH is NOT set... */
      ham_assert(!(flags & HAM_FIND_LT_MATCH));

      if (cmp < 0) {
        /* key @ slot is LARGER than the key we search for ... */
        ham_key_set_intflags(key, ham_key_get_intflags(key)
                  | BtreeKey::kGreater);
        cmp = 0;
      }
      else {
        /* key @ slot is SMALLER than the key we search for */
        ham_assert(cmp > 0);
        if (slot < (int)node->get_count() - 1) {
          slot++;
          ham_key_set_intflags(key, ham_key_get_intflags(key)
                  | BtreeKey::kGreater);
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

//
// visitor object for estimating / counting the number of keys
///
class CalcKeysVisitor : public BtreeVisitor {
  public:
    CalcKeysVisitor(LocalDatabase *db, bool distinct)
      : m_db(db), m_distinct(distinct), m_count(0) {
    }

    virtual bool operator()(BtreeNodeProxy *node, const void *key_data,
                  ham_u8_t key_flags, ham_u32_t key_size, 
                  ham_u64_t record_id) {
      ham_u32_t count = node->get_count();

      if (m_distinct
          || (m_db->get_rt_flags() & HAM_ENABLE_DUPLICATE_KEYS) == 0) {
        m_count += count;
        return (false);
      }

      for (ham_u32_t i = 0; i < count; i++)
        m_count += node->get_record_count(i);
      return (false);
    }

    ham_u64_t get_result() const {
      return (m_count);
    }

  private:
    LocalDatabase *m_db;
    bool m_distinct;
    ham_u64_t m_count;
};

ham_u64_t
BtreeIndex::count(bool distinct)
{
  CalcKeysVisitor visitor(m_db, distinct);
  enumerate(visitor);
  return (visitor.get_result());
}

//
// visitor object to free all allocated blobs
///
class FreeBlobsVisitor : public BtreeVisitor {
  public:
    virtual bool operator()(BtreeNodeProxy *node, const void *key_data,
                  ham_u8_t key_flags, ham_u32_t key_size, 
                  ham_u64_t record_id) {
      node->remove_all_entries();
      // no need to continue enumerating the current page
      return (false);
    }
};

void
BtreeIndex::release()
{
  FreeBlobsVisitor visitor;
  enumerate(visitor, true);
}

} // namespace hamsterdb
