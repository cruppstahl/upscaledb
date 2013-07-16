/*
 * Copyright (C) 2005-2013 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 *
 */

#include "config.h"

#include <string.h>

#include "blob_manager.h"
#include "btree.h"
#include "db_local.h"
#include "env_local.h"
#include "error.h"
#include "extkeys.h"
#include "btree_key.h"
#include "mem.h"
#include "page.h"

namespace hamsterdb {

ham_size_t PBtreeKey::kSizeofOverhead = OFFSETOF(PBtreeKey, m_key);

ham_u64_t
PBtreeKey::get_extended_rid(LocalDatabase *db)
{
  ham_u64_t rid;
  memcpy(&rid, get_key() + (db->get_keysize() - sizeof(ham_u64_t)),
          sizeof(rid));
  return (ham_db2h_offset(rid));
}

void
PBtreeKey::set_extended_rid(LocalDatabase *db, ham_u64_t rid)
{
  rid = ham_h2db_offset(rid);
  memcpy(get_key() + (db->get_keysize() - sizeof(ham_u64_t)),
          &rid, sizeof(rid));
}

ham_status_t
PBtreeKey::set_record(LocalDatabase *db, Transaction *txn, ham_record_t *record,
            ham_size_t position, ham_u32_t flags, ham_size_t *new_position)
{
  ham_status_t st;
  LocalEnvironment *env = db->get_local_env();
  ham_u64_t rid = 0;
  ham_u64_t ptr = get_ptr();
  ham_u8_t oldflags = get_flags();

  set_flags(oldflags & ~(kBlobSizeSmall
                    | kBlobSizeTiny
                    | kBlobSizeEmpty));

  /* no existing key, just create a new key (but not a duplicate)? */
  if (!ptr && !(oldflags & (kBlobSizeSmall
                    | kBlobSizeTiny
                    | kBlobSizeEmpty))) {
    if (record->size <= sizeof(ham_u64_t)) {
      if (record->data)
        memcpy(&rid, record->data, record->size);
      if (record->size == 0)
        set_flags(get_flags() | kBlobSizeEmpty);
      else if (record->size < sizeof(ham_u64_t)) {
        char *p = (char *)&rid;
        p[sizeof(ham_u64_t) - 1] = (char)record->size;
        set_flags(get_flags() | kBlobSizeTiny);
      }
      else
        set_flags(get_flags() | kBlobSizeSmall);
      set_ptr(rid);
    }
    else {
      st = env->get_blob_manager()->allocate(db, record, flags, &rid);
      if (st)
        return (st);
      set_ptr(rid);
    }
  }
  else if (!(oldflags & kDuplicates)
      && record->size > sizeof(ham_u64_t)
      && !(flags & (HAM_DUPLICATE | HAM_DUPLICATE_INSERT_BEFORE
                    | HAM_DUPLICATE_INSERT_AFTER | HAM_DUPLICATE_INSERT_FIRST
                    | HAM_DUPLICATE_INSERT_LAST))) {
    /*
     * an existing key, which is overwritten with a big record
     * Note that the case where old record is EMPTY (!ptr) or
     * SMALL (size = 8, but content = 00000000 --> !ptr) are caught here
     * and in the next branch, as they should.
     */
    if (oldflags & (kBlobSizeSmall | kBlobSizeTiny | kBlobSizeEmpty)) {
      rid = 0;
      st = env->get_blob_manager()->allocate(db, record, flags, &rid);
      if (st)
        return (st);
      if (rid)
        set_ptr(rid);
    }
    else {
      st = env->get_blob_manager()->overwrite(db, ptr, record, flags, &rid);
      if (st)
        return (st);
      set_ptr(rid);
    }
  }
  else if (!(oldflags & kDuplicates)
          && record->size <= sizeof(ham_u64_t)
          && !(flags & (HAM_DUPLICATE
                  | HAM_DUPLICATE_INSERT_BEFORE
                  | HAM_DUPLICATE_INSERT_AFTER
                  | HAM_DUPLICATE_INSERT_FIRST
                  | HAM_DUPLICATE_INSERT_LAST))) {
    /* an existing key which is overwritten with a small record */
    if (!(oldflags & (kBlobSizeSmall
                    | kBlobSizeTiny
                    | kBlobSizeEmpty))) {
      st = env->get_blob_manager()->free(db, ptr, 0);
      if (st)
        return (st);
    }
    if (record->data)
      memcpy(&rid, record->data, record->size);
    if (record->size == 0)
      set_flags(get_flags() | kBlobSizeEmpty);
    else if (record->size < sizeof(ham_u64_t)) {
      char *p = (char *)&rid;
      p[sizeof(ham_u64_t) - 1] = (char)record->size;
      set_flags(get_flags() | kBlobSizeTiny);
    }
    else
      set_flags(get_flags() | kBlobSizeSmall);
    set_ptr(rid);
  }
  else {
    /*
     * a duplicate of an existing key - always insert it at the end of
     * the duplicate list (unless the DUPLICATE flags say otherwise OR
     * when we have a duplicate-record comparison function for
     * ordered insertion of duplicate records)
     *
     * create a duplicate list, if it does not yet exist
     */
    PDupeEntry entries[2];
    int i = 0;
    ham_assert((flags & (HAM_DUPLICATE | HAM_DUPLICATE_INSERT_BEFORE
                    | HAM_DUPLICATE_INSERT_AFTER | HAM_DUPLICATE_INSERT_FIRST
                    | HAM_DUPLICATE_INSERT_LAST | HAM_OVERWRITE)));
    memset(entries, 0, sizeof(entries));
    if (!(oldflags & kDuplicates)) {
      ham_assert((flags & (HAM_DUPLICATE | HAM_DUPLICATE_INSERT_BEFORE
                      | HAM_DUPLICATE_INSERT_AFTER | HAM_DUPLICATE_INSERT_FIRST
                      | HAM_DUPLICATE_INSERT_LAST)));
      dupe_entry_set_flags(&entries[i],
                oldflags & (kBlobSizeSmall
                    | kBlobSizeTiny | kBlobSizeEmpty));
      dupe_entry_set_rid(&entries[i], ptr);
      i++;
    }
    if (record->size <= sizeof(ham_u64_t)) {
      if (record->data)
        memcpy(&rid, record->data, record->size);
      if (record->size == 0)
        dupe_entry_set_flags(&entries[i], kBlobSizeEmpty);
      else if (record->size < sizeof(ham_u64_t)) {
        char *p = (char *)&rid;
        p[sizeof(ham_u64_t) - 1] = (char)record->size;
        dupe_entry_set_flags(&entries[i], kBlobSizeTiny);
      }
      else
        dupe_entry_set_flags(&entries[i], kBlobSizeSmall);
      dupe_entry_set_rid(&entries[i], rid);
    }
    else {
      st = env->get_blob_manager()->allocate(db, record, flags, &rid);
      if (st)
        return (st);
      dupe_entry_set_flags(&entries[i], 0);
      dupe_entry_set_rid(&entries[i], rid);
    }
    i++;

    rid = 0;
    st = env->get_duplicate_manager()->insert(db, txn,
            (i == 2 ? 0 : ptr), record, position,
            flags, &entries[0], i, &rid, new_position);
    if (st) {
      /* don't leak memory through the blob allocation above */
      if (record->size > sizeof(ham_u64_t)) {
        (void)env->get_blob_manager()->free(db,
                dupe_entry_get_rid(&entries[i-1]), 0);
      }
      return st;
    }

    set_flags(get_flags() | kDuplicates);
    if (rid)
      set_ptr(rid);
  }

  return (0);
}

ham_status_t
PBtreeKey::erase_record(LocalDatabase *db, Transaction *txn, ham_size_t dupe_id,
        bool erase_all_duplicates)
{
  ham_status_t st;
  ham_u64_t rid;

  /* if the record is > 8 bytes then it needs to be freed explicitly */
  if (!(get_flags() & (kBlobSizeSmall | kBlobSizeTiny | kBlobSizeEmpty))) {
    if (get_flags() & kDuplicates) {
      /* delete one (or all) duplicates */
      st = db->get_local_env()->get_duplicate_manager()->erase(db, txn,
                        get_ptr(), dupe_id, erase_all_duplicates, &rid);
      if (st)
        return (st);
      if (erase_all_duplicates) {
        set_flags(get_flags() & ~kDuplicates);
        set_ptr(0);
      }
      else {
        set_ptr(rid);
        if (!rid) /* rid == 0: the last duplicate was deleted */
          set_flags(0);
      }
    }
    else {
      /* delete the blob */
      st = db->get_local_env()->get_blob_manager()->free(db, get_ptr(), 0);
      if (st)
        return (st);
      set_ptr(0);
    }
  }
  /* otherwise just reset the blob flags of the key and set the record
   * pointer to 0 */
  else {
    set_flags(get_flags() & ~(kBlobSizeSmall | kBlobSizeTiny
                    | kBlobSizeEmpty | kDuplicates));
    set_ptr(0);
  }

  return (0);
}

} // namespace hamsterdb
