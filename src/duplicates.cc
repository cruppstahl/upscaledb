/*
 * Copyright (C) 2005-2012 Christoph Rupp (chris@crupp.de).
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

#include "blob.h"
#include "db.h"
#include "device.h"
#include "env.h"
#include "error.h"
#include "freelist.h"
#include "duplicates.h"

namespace ham {

ham_status_t
DuplicateManager::get_table(dupe_table_t **table_ref, Page **page,
                ham_u64_t table_id)
{
  ham_status_t st;
  blob_t hdr;
  Page *hdrpage = 0;
  dupe_table_t *table;

  *page = 0;
  *table_ref = 0;

  if (m_env->get_flags() & HAM_IN_MEMORY_DB) {
    ham_u8_t *p = (ham_u8_t *)U64_TO_PTR(table_id);
    *table_ref = (dupe_table_t *)(p + sizeof(hdr));
    return (0);
  }

  /* load the blob header */
  st = m_env->get_blob_manager()->read_chunk(0, &hdrpage, table_id, 0,
                  (ham_u8_t *)&hdr, sizeof(hdr));
  if (st)
    return (st);

  /*
   * if the whole table is in a page (and not split between several
   * pages), just return a pointer directly in the page
   */
  if (hdrpage->get_self() + m_env->get_usable_pagesize()
        >= table_id + blob_get_size(&hdr)) {
    ham_u8_t *p = hdrpage->get_raw_payload();
    /* yes, table is in the page */
    *page = hdrpage;
    *table_ref = (dupe_table_t *)&p[table_id - hdrpage->get_self()
                    + sizeof(hdr)];
    return (0);
  }

  /* otherwise allocate memory for the table */
  table = (dupe_table_t *)m_env->get_allocator()->alloc(
              (ham_size_t)blob_get_size(&hdr));
  if (!table)
    return (HAM_OUT_OF_MEMORY);

  /* then read the rest of the blob */
  st = m_env->get_blob_manager()->read_chunk(hdrpage, 0, table_id + sizeof(hdr),
                    0, (ham_u8_t *)table, (ham_size_t)blob_get_size(&hdr));
  if (st)
    return (st);

  *table_ref = table;
  return (0);
}

ham_size_t
DuplicateManager::get_sorted_position(Database *db, Transaction *txn,
                dupe_table_t *table, ham_record_t *record, ham_u32_t flags)
{
  ham_duplicate_compare_func_t foo = db->get_duplicate_compare_func();
  ham_size_t l, r, m;
  int cmp;
  dupe_entry_t *e;
  ham_record_t item_record;
  ham_u16_t dam;
  ham_status_t st = 0;

  /*
   * Use a slightly adapted form of binary search: as we already have our
   * initial position (as was stored in the cursor), we take that as our
   * first 'median' value and go from there.
   */
  l = 0;
  r = dupe_table_get_count(table) - 1; /* get_count() is 1 too many! */

  /*
   * Maybe Wrong Idea: sequential access/insert doesn't mean the RECORD
   * values are sequential too! They MAY be, but don't have to!
   *
   * For now, we assume they are also sequential when you're storing records
   * in duplicate-key tables (probably a secondary index table for another
   * table, this one).
   */
  dam = db->get_data_access_mode();
  if (dam & HAM_DAM_SEQUENTIAL_INSERT) {
    /* assume the insertion point sits at the end of the dupe table */
    m = r;
  }
  else {
    m = (l + r) / 2;
  }
  ham_assert(m <= r);

  while (l <= r) {
    ham_assert(m < dupe_table_get_count(table));

    e = dupe_table_get_entry(table, m);

    memset(&item_record, 0, sizeof(item_record));
    item_record._rid = dupe_entry_get_rid(e);
    item_record._intflags = dupe_entry_get_flags(e) & (KEY_BLOB_SIZE_SMALL
                                                     | KEY_BLOB_SIZE_TINY
                                                     | KEY_BLOB_SIZE_EMPTY);
    st = btree_read_record(db, txn, &item_record,
              (ham_u64_t *)&dupe_entry_get_ridptr(e), flags);
    if (st)
      return (st);

    cmp = foo((ham_db_t *)db, (ham_u8_t *)record->data, record->size,
                      (ham_u8_t *)item_record.data, item_record.size);
    /* item is lower than the left-most item of our range */
    if (m == l) {
      if (cmp < 0)
        break;
    }
    if (l == r) {
      if (cmp >= 0) {
        /* write GEQ record value in NEXT slot */
        m++;
      }
      else /* if (cmp < 0) */ {
        ham_assert(m == r);
      }
      break;
    }
    else if (cmp == 0) {
      /* write equal record value in NEXT slot */
      m++;
      break;
    }
    else if (cmp < 0) {
      if (m == 0) /* new item will be smallest item in the list */
          break;
      r = m - 1;
    }
    else {
      /* write GE record value in NEXT slot, when we have nothing
       * left to search */
      m++;
      l = m;
    }
    m = (l + r) / 2;
  }

  /* now 'm' points at the insertion point in the table */
  return (m);
}

ham_status_t
DuplicateManager::insert(Database *db, Transaction *txn, ham_offset_t table_id,
                ham_record_t *record, ham_size_t position, ham_u32_t flags,
                dupe_entry_t *entries, ham_size_t num_entries,
                ham_offset_t *rid, ham_size_t *new_position)
{
  ham_status_t st = 0;
  dupe_table_t *table = 0;
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
    table = (dupe_table_t *)m_env->get_allocator()->calloc(
                    sizeof(dupe_table_t) + 7 * sizeof(dupe_entry_t));
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
    if (!page && !(m_env->get_flags() & HAM_IN_MEMORY_DB))
      alloc_table = true;
  }

  ham_assert(num_entries == 1);

  /* resize the table, if necessary */
  if (!(flags & HAM_OVERWRITE)
        && dupe_table_get_count(table) + 1 >= dupe_table_get_capacity(table)) {
    dupe_table_t *old = table;
    ham_size_t new_cap = dupe_table_get_capacity(table);

    if (new_cap < 3 * 8)
      new_cap += 8;
    else
      new_cap += new_cap / 3;

    table = (dupe_table_t *)m_env->get_allocator()->calloc(
                sizeof(dupe_table_t) + (new_cap - 1) * sizeof(dupe_entry_t));
    if (!table)
      return (HAM_OUT_OF_MEMORY);
    dupe_table_set_capacity(table, new_cap);
    dupe_table_set_count(table, dupe_table_get_count(old));
    memcpy(dupe_table_get_entry(table, 0), dupe_table_get_entry(old, 0),
                   dupe_table_get_count(old) * sizeof(dupe_entry_t));
    if (alloc_table)
      m_env->get_allocator()->free(old);

    alloc_table = true;
    resize = true;
  }

  /* insert sorted, unsorted or overwrite the entry at the requested position */
  if (flags&HAM_OVERWRITE) {
    dupe_entry_t *e=dupe_table_get_entry(table, position);

    if (!(dupe_entry_get_flags(e)&(KEY_BLOB_SIZE_SMALL
                                |KEY_BLOB_SIZE_TINY
                                |KEY_BLOB_SIZE_EMPTY))) {
      (void)m_env->get_blob_manager()->free(db, dupe_entry_get_rid(e), 0);
    }

    memcpy(dupe_table_get_entry(table, position),
                    &entries[0], sizeof(entries[0]));
  }
  else {
    if (db->get_rt_flags() & HAM_SORT_DUPLICATES) {
      position = get_sorted_position(db, txn, table, record, flags);
    }
    else if (flags & HAM_DUPLICATE_INSERT_BEFORE) {
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
    rec.size = sizeof(dupe_table_t) + (dupe_table_get_capacity(table) - 1)
                    * sizeof(dupe_entry_t);
    st = m_env->get_blob_manager()->overwrite(db, table_id, &rec, 0, rid);
  }
  else if (!table_id) {
    ham_record_t rec = {0};
    rec.data = (ham_u8_t *)table;
    rec.size = sizeof(dupe_table_t) + (dupe_table_get_capacity(table) - 1)
                    * sizeof(dupe_entry_t);
    st = m_env->get_blob_manager()->allocate(db, &rec, 0, rid);
  }
  else if (table_id && page) {
    page->set_dirty(true);
  }
  else {
    ham_assert(!"shouldn't be here");
  }

  if (alloc_table)
    m_env->get_allocator()->free(table);

  if (new_position)
    *new_position = position;

  return (st);
}

ham_status_t
DuplicateManager::erase(Database *db, Transaction *txn, ham_offset_t table_id,
            ham_size_t position, ham_u32_t flags, ham_offset_t *new_table_id)
{
  ham_status_t st;
  ham_record_t rec = {0};
  dupe_table_t *table;
  ham_offset_t rid;

  if (new_table_id)
    *new_table_id = table_id;

  st = m_env->get_blob_manager()->read(db, txn, table_id, &rec, 0);
  if (st)
    return (st);

  table = (dupe_table_t *)rec.data;

  /*
   * if HAM_ERASE_ALL_DUPLICATES is set *OR* if the last duplicate is deleted:
   * free the whole duplicate table
   */
  if (flags & HAM_ERASE_ALL_DUPLICATES
          || (position == 0 && dupe_table_get_count(table) == 1)) {
    for (ham_size_t i = 0; i < dupe_table_get_count(table); i++) {
      dupe_entry_t *e = dupe_table_get_entry(table, i);
      if (!(dupe_entry_get_flags(e) & (KEY_BLOB_SIZE_SMALL
                                    | KEY_BLOB_SIZE_TINY
                                    | KEY_BLOB_SIZE_EMPTY))) {
        st = m_env->get_blob_manager()->free(db, dupe_entry_get_rid(e), 0);
        if (st) {
          m_env->get_allocator()->free(table);
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
    dupe_entry_t *e = dupe_table_get_entry(table, position);
    if (!(dupe_entry_get_flags(e) & (KEY_BLOB_SIZE_SMALL
                                  | KEY_BLOB_SIZE_TINY
                                  | KEY_BLOB_SIZE_EMPTY))) {
      st = m_env->get_blob_manager()->free(db, dupe_entry_get_rid(e), 0);
      if (st) {
        m_env->get_allocator()->free(table);
        return (st);
      }
    }
    memmove(e, e + 1, ((dupe_table_get_count(table) - position) - 1)
                * sizeof(dupe_entry_t));
    dupe_table_set_count(table, dupe_table_get_count(table) - 1);

    rec.data = (ham_u8_t *)table;
    rec.size = sizeof(dupe_table_t) + (dupe_table_get_capacity(table) - 1)
                    * sizeof(dupe_entry_t);
    st = m_env->get_blob_manager()->overwrite(db, table_id, &rec, 0, &rid);
    if (st) {
      m_env->get_allocator()->free(table);
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
DuplicateManager::get_count(ham_offset_t table_id, ham_size_t *count,
                dupe_entry_t *entry)
{
  dupe_table_t *table;
  Page *page = 0;

  ham_status_t st = get_table(&table, &page, table_id);
  if (st)
    return (st);

  *count = dupe_table_get_count(table);
  if (entry)
    memcpy(entry, dupe_table_get_entry(table, (*count) - 1), sizeof(*entry));

  if (!(m_env->get_flags() & HAM_IN_MEMORY_DB))
    if (!page)
      m_env->get_allocator()->free(table);

  return (0);
}

ham_status_t
DuplicateManager::get(ham_offset_t table_id, ham_size_t position,
                dupe_entry_t *entry)
{
  ham_status_t st;
  dupe_table_t *table;
  Page *page = 0;

  st = get_table(&table, &page, table_id);
  if (st)
    return (st);

  if (position >= dupe_table_get_count(table)) {
    if (!(m_env->get_flags() & HAM_IN_MEMORY_DB))
      if (!page)
        m_env->get_allocator()->free(table);
    return (HAM_KEY_NOT_FOUND);
  }
  memcpy(entry, dupe_table_get_entry(table, position), sizeof(*entry));

  if (!(m_env->get_flags() & HAM_IN_MEMORY_DB))
    if (!page)
      m_env->get_allocator()->free(table);

  return (0);
}

ham_status_t
DuplicateManager::get_table(ham_offset_t table_id, dupe_table_t **ptable,
                bool *needs_free)
{
  Page *page = 0;

  ham_status_t st = get_table(ptable, &page, table_id);
  if (st)
    return (st);

  if (!(m_env->get_flags() & HAM_IN_MEMORY_DB))
    if (!page)
      *needs_free = true;

  return (0);
}

} // namespace ham
