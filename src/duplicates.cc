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

#include "blob_manager_disk.h"
#include "db_local.h"
#include "device.h"
#include "env.h"
#include "error.h"
#include "txn.h"
#include "duplicates.h"

namespace hamsterdb {

ham_status_t
DuplicateManager::get_table(PDupeTable **table_ref, Page **page,
                ham_u64_t table_id)
{
  ham_status_t st;
  PBlobHeader hdr;
  Page *hdrpage = 0;
  PDupeTable *table;

  *page = 0;
  *table_ref = 0;

  if (m_env->get_flags() & HAM_IN_MEMORY) {
    ham_u8_t *p = (ham_u8_t *)U64_TO_PTR(table_id);
    *table_ref = (PDupeTable *)(p + sizeof(hdr));
    return (0);
  }

  /* load the blob header */
  DiskBlobManager *dbm = (DiskBlobManager *)m_env->get_blob_manager();
  st = dbm->read_chunk(0, &hdrpage, table_id, 0,
                  (ham_u8_t *)&hdr, sizeof(hdr));
  if (st)
    return (st);

  /*
   * if the whole table is in a page (and not split between several
   * pages), just return a pointer directly in the page
   */
  if (hdrpage->get_address() + m_env->get_usable_page_size()
        >= table_id + hdr.get_size()) {
    ham_u8_t *p = hdrpage->get_raw_payload();
    /* yes, table is in the page */
    *page = hdrpage;
    *table_ref = (PDupeTable *)&p[table_id - hdrpage->get_address()
                    + sizeof(hdr)];
    return (0);
  }

  /* otherwise allocate memory for the table */
  table = Memory::allocate<PDupeTable>((ham_u32_t)hdr.get_size());
  if (!table)
    return (HAM_OUT_OF_MEMORY);

  /* then read the rest of the blob */
  st = dbm->read_chunk(hdrpage, 0, table_id + sizeof(hdr),
                    0, (ham_u8_t *)table, (ham_u32_t)hdr.get_size());
  if (st)
    return (st);

  *table_ref = table;
  return (0);
}

ham_status_t
DuplicateManager::insert(LocalDatabase *db, Transaction *txn,
                ham_u64_t table_id, ham_record_t *record, ham_u32_t position,
                ham_u32_t flags, PDupeEntry *entries, ham_u32_t num_entries,
                ham_u64_t *rid, ham_u32_t *new_position)
{
  ham_status_t st = 0;
  PDupeTable *table = 0;
  bool alloc_table = false;
  bool resize = false;
  Page *page = 0;

  /*
   * create a new duplicate table if none existed, and insert
   * the first entry
   */
  if (!table_id) {
    ham_assert(num_entries == 2);
    /* allocates space for 8 (!) entries */
    table = Memory::allocate<PDupeTable>(sizeof(PDupeTable)
            + 7 * sizeof(PDupeEntry));
    if (!table)
      return (HAM_OUT_OF_MEMORY);
    dupe_table_set_capacity(table, 8);
    dupe_table_set_count(table, 1);
    memcpy(dupe_table_get_entry(table, 0), &entries[0], sizeof(entries[0]));

    /* skip the first entry */
    entries++;
    num_entries--;
    alloc_table = true;
  }
  else {
    /* otherwise load the existing table */
    st = get_table(&table, &page, table_id);
    if (st)
      return (st);
    if (!page && !(m_env->get_flags() & HAM_IN_MEMORY))
      alloc_table = true;
  }

  ham_assert(num_entries == 1);

  /* resize the table, if necessary */
  if (!(flags & HAM_OVERWRITE)
        && dupe_table_get_count(table) + 1 >= dupe_table_get_capacity(table)) {
    PDupeTable *old = table;
    ham_u32_t new_cap = dupe_table_get_capacity(table);

    if (new_cap < 3 * 8)
      new_cap += 8;
    else
      new_cap += new_cap / 3;

    table = Memory::allocate<PDupeTable>(
                sizeof(PDupeTable) + (new_cap - 1) * sizeof(PDupeEntry));
    if (!table)
      return (HAM_OUT_OF_MEMORY);
    dupe_table_set_capacity(table, new_cap);
    dupe_table_set_count(table, dupe_table_get_count(old));
    memcpy(dupe_table_get_entry(table, 0), dupe_table_get_entry(old, 0),
                   dupe_table_get_count(old) * sizeof(PDupeEntry));
    if (alloc_table)
      Memory::release(old);

    alloc_table = true;
    resize = true;
  }

  /* insert sorted, unsorted or overwrite the entry at the requested position */
  if (flags & HAM_OVERWRITE) {
    PDupeEntry *e = dupe_table_get_entry(table, position);

    if (!(dupe_entry_get_flags(e) & (BtreeKey::kBlobSizeSmall
                                | BtreeKey::kBlobSizeTiny
                                | BtreeKey::kBlobSizeEmpty))) {
      (void)m_env->get_blob_manager()->free(db, dupe_entry_get_rid(e), 0);
    }

    memcpy(dupe_table_get_entry(table, position),
                    &entries[0], sizeof(entries[0]));
  }
  else {
    if (flags & HAM_DUPLICATE_INSERT_BEFORE) {
      /* do nothing, insert at the current position */
    }
    else if (flags & HAM_DUPLICATE_INSERT_AFTER) {
      position++;
      if (position > dupe_table_get_count(table))
        position = dupe_table_get_count(table);
    }
    else if (flags & HAM_DUPLICATE_INSERT_FIRST) {
      position = 0;
    }
    else if (flags & HAM_DUPLICATE_INSERT_LAST) {
      position = dupe_table_get_count(table);
    }
    else {
      position = dupe_table_get_count(table);
    }

    if (position != dupe_table_get_count(table)) {
      memmove(dupe_table_get_entry(table, position + 1),
          dupe_table_get_entry(table, position),
          sizeof(entries[0]) * (dupe_table_get_count(table) - position));
    }

    memcpy(dupe_table_get_entry(table, position),
          &entries[0], sizeof(entries[0]));

    dupe_table_set_count(table, dupe_table_get_count(table) + 1);
  }

  /* write the table back to disk and return the blobid of the table */
  if ((table_id && !page) || resize) {
    ham_record_t rec = {0};
    rec.data = (ham_u8_t *)table;
    rec.size = sizeof(PDupeTable) + (dupe_table_get_capacity(table) - 1)
                    * sizeof(PDupeEntry);
    st = m_env->get_blob_manager()->overwrite(db, table_id, &rec, 0, rid);
  }
  else if (!table_id) {
    ham_record_t rec = {0};
    rec.data = (ham_u8_t *)table;
    rec.size = sizeof(PDupeTable) + (dupe_table_get_capacity(table) - 1)
                    * sizeof(PDupeEntry);
    st = m_env->get_blob_manager()->allocate(db, &rec, 0, rid);
  }
  else if (table_id && page) {
    page->set_dirty(true);
  }
  else {
    ham_assert(!"shouldn't be here");
  }

  if (alloc_table)
    Memory::release(table);

  if (new_position)
    *new_position = position;

  return (st);
}

ham_status_t
DuplicateManager::erase(LocalDatabase *db, ham_u64_t table_id,
            ham_u32_t position, bool erase_all_duplicates,
            ham_u64_t *new_table_id)
{
  ham_record_t rec = {0};
  ham_u64_t rid;

  if (new_table_id)
    *new_table_id = table_id;

  ByteArray *arena = &db->get_record_arena();

  ham_status_t st = m_env->get_blob_manager()->read(db, table_id, &rec,
          0, arena);
  if (st)
    return (st);

  PDupeTable *table = (PDupeTable *)rec.data;

  /*
   * if erase_all_duplicates is set *OR* if the last duplicate is deleted:
   * free the whole duplicate table
   */
  if (erase_all_duplicates
          || (position == 0 && dupe_table_get_count(table) == 1)) {
    for (ham_u32_t i = 0; i < dupe_table_get_count(table); i++) {
      PDupeEntry *e = dupe_table_get_entry(table, i);
      if (!(dupe_entry_get_flags(e) & (BtreeKey::kBlobSizeSmall
                                    | BtreeKey::kBlobSizeTiny
                                    | BtreeKey::kBlobSizeEmpty))) {
        st = m_env->get_blob_manager()->free(db, dupe_entry_get_rid(e), 0);
        if (st) {
          Memory::release(table);
          return (st);
        }
      }
    }
    /* now free the table itself */
    st = m_env->get_blob_manager()->free(db, table_id, 0);
    if (st)
      return (st);

    if (new_table_id)
      *new_table_id = 0;

    return (0);
  }
  else {
    ham_record_t rec = {0};
    PDupeEntry *e = dupe_table_get_entry(table, position);
    if (!(dupe_entry_get_flags(e) & (BtreeKey::kBlobSizeSmall
                                  | BtreeKey::kBlobSizeTiny
                                  | BtreeKey::kBlobSizeEmpty))) {
      st = m_env->get_blob_manager()->free(db, dupe_entry_get_rid(e), 0);
      if (st) {
        Memory::release(table);
        return (st);
      }
    }
    memmove(e, e + 1, ((dupe_table_get_count(table) - position) - 1)
                * sizeof(PDupeEntry));
    dupe_table_set_count(table, dupe_table_get_count(table) - 1);

    rec.data = (ham_u8_t *)table;
    rec.size = sizeof(PDupeTable) + (dupe_table_get_capacity(table) - 1)
                    * sizeof(PDupeEntry);
    st = m_env->get_blob_manager()->overwrite(db, table_id, &rec, 0, &rid);
    if (st) {
      Memory::release(table);
      return (st);
    }
    if (new_table_id)
      *new_table_id = rid;
  }

  /* return 0 as a rid if the table is empty */
  if (dupe_table_get_count(table) == 0)
    if (new_table_id)
      *new_table_id = 0;

  return (0);
}

ham_status_t
DuplicateManager::get_count(ham_u64_t table_id, ham_u32_t *count,
                PDupeEntry *entry)
{
  PDupeTable *table;
  Page *page = 0;

  ham_status_t st = get_table(&table, &page, table_id);
  if (st)
    return (st);

  *count = dupe_table_get_count(table);
  if (entry)
    memcpy(entry, dupe_table_get_entry(table, (*count) - 1), sizeof(*entry));

  if (!(m_env->get_flags() & HAM_IN_MEMORY)) {
    if (!page)
      Memory::release(table);
  }

  return (0);
}

ham_status_t
DuplicateManager::get(ham_u64_t table_id, ham_u32_t position,
                PDupeEntry *entry)
{
  ham_status_t st;
  PDupeTable *table;
  Page *page = 0;

  st = get_table(&table, &page, table_id);
  if (st)
    return (st);

  if (position >= dupe_table_get_count(table)) {
    if (!(m_env->get_flags() & HAM_IN_MEMORY))
      if (!page)
        Memory::release(table);
    return (HAM_KEY_NOT_FOUND);
  }
  memcpy(entry, dupe_table_get_entry(table, position), sizeof(*entry));

  if (!(m_env->get_flags() & HAM_IN_MEMORY))
    if (!page)
      Memory::release(table);

  return (0);
}

ham_status_t
DuplicateManager::get_table(ham_u64_t table_id, PDupeTable **ptable,
                bool *needs_free)
{
  Page *page = 0;

  ham_status_t st = get_table(ptable, &page, table_id);
  if (st)
    return (st);

  if (!(m_env->get_flags() & HAM_IN_MEMORY))
    if (!page)
      *needs_free = true;

  return (0);
}

} // namespace hamsterdb
