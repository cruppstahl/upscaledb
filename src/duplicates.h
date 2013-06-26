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
 * @brief functions for reading/writing/allocating duplicate keys
 *
 */

#ifndef HAM_DUPLICATES_H__
#define HAM_DUPLICATES_H__

#include <ham/hamsterdb_int.h>
#include "endianswap.h"

namespace hamsterdb {

class LocalDatabase;
class Transaction;

#include "packstart.h"

/**
 * a structure for a duplicate - used in a duplicate table
 */
typedef HAM_PACK_0 struct HAM_PACK_1 PDupeEntry
{
    /** reserved, for padding */
    ham_u8_t _padding[7];

    /**
     * the flags - same as @ref KEY_BLOB_SIZE_SMALL,
     *             @ref KEY_BLOB_SIZE_TINY and @ref KEY_BLOB_SIZE_EMPTY
     */
    ham_u8_t _flags;

    /** the record id (unless it's TINY, SMALL or NULL) */
    ham_u64_t _rid;

} HAM_PACK_2 PDupeEntry;

#include "packstop.h"

/* get the flags of a duplicate entry */
#define dupe_entry_get_flags(e)         (e)->_flags

/* set the flags of a duplicate entry */
#define dupe_entry_set_flags(e, f)      (e)->_flags=(f)

/*
 * get the record id of a duplicate entry
 *
 * !!!
 * if TINY or SMALL is set, the rid is actually a char*-pointer;
 * in this case, we must not use endian-conversion!
 */
#define dupe_entry_get_rid(e)                                                 \
         (((dupe_entry_get_flags(e)&PBtreeKey::KEY_BLOB_SIZE_TINY)             \
          || (dupe_entry_get_flags(e)&PBtreeKey::KEY_BLOB_SIZE_SMALL))         \
           ? (e)->_rid                                                        \
           : ham_db2h_offset((e)->_rid))

/* same as above, but without endian conversion */
#define dupe_entry_get_ridptr(e)        (e)->_rid

/*
 * set the record id of a duplicate entry
 *
 * !!! same problems as with get_rid():
 * if TINY or SMALL is set, the rid is actually a char*-pointer;
 * in this case we must not use endian-conversion!
 */
#define dupe_entry_set_rid(e, r)                                              \
         (e)->_rid=(((dupe_entry_get_flags(e)&PBtreeKey::KEY_BLOB_SIZE_TINY)   \
          || (dupe_entry_get_flags(e)&PBtreeKey::KEY_BLOB_SIZE_SMALL))         \
           ? (r)                                                              \
           : ham_h2db_offset(r))

#include "packstart.h"

/**
 * a structure for duplicates (PDupeTable)
 */
typedef HAM_PACK_0 struct HAM_PACK_1 PDupeTable
{
    /** the number of duplicates (used entries in this table) */
    ham_u32_t _count;

    /** the capacity of entries in this table */
    ham_u32_t _capacity;

    /** a dynamic array of duplicate entries */
    PDupeEntry _entries[1];

} HAM_PACK_2 PDupeTable;

#include "packstop.h"

/** get the number of duplicates */
#define dupe_table_get_count(t)         (ham_db2h32((t)->_count))

/** set the number of duplicates */
#define dupe_table_set_count(t, c)      (t)->_count=ham_h2db32(c)

/** get the maximum number of duplicates */
#define dupe_table_get_capacity(t)      (ham_db2h32((t)->_capacity))

/** set the maximum number of duplicates */
#define dupe_table_set_capacity(t, c)   (t)->_capacity=ham_h2db32(c)

/** get a pointer to a duplicate entry @a i */
#define dupe_table_get_entry(t, i)      (&(t)->_entries[i])

#if defined(_MSC_VER) && defined(_CRTDBG_MAP_ALLOC)
#  undef alloc
#  undef free
#  undef realloc
#  undef calloc
#endif

/**
 * The DuplicateManager manages duplicates (not a surprise)
 */
class DuplicateManager
{
  public:
    DuplicateManager(Environment *env)
      : m_env(env) {
    }

    /**
     * create a duplicate table and insert all entries in the duplicate
     * (max. two entries are allowed; first entry will be at the first position,
     * second entry will be set depending on the flags)
     *
     * OR, if the table already exists (i.e. table_id != 0), insert the
     * entry depending on the flags (only one entry is allowed in this case)
     */
    ham_status_t insert(LocalDatabase *db, Transaction *txn, ham_u64_t table_id,
                ham_record_t *record, ham_size_t position, ham_u32_t flags,
                PDupeEntry *entries, ham_size_t num_entries,
                ham_u64_t *rid, ham_size_t *new_position);

    /**
     * delete a duplicate
     *
     * if erase_all_duplicates is true then all duplicates and the dupe
     * table are deleted; otherwise only the single duplicate is erased and
     * the table remains (unless it became empty)
     *
     * sets new_table_id to 0 if the table is empty
     */
    ham_status_t erase(LocalDatabase *db, Transaction *txn, ham_u64_t table_id,
                ham_size_t position, bool erase_all_duplicates,
                ham_u64_t *new_table_id);

    /**
     * get the number of duplicates
     */
    ham_status_t get_count(ham_u64_t table_id, ham_size_t *count,
                PDupeEntry *entry);

    /**
     * get a duplicate
     */
    ham_status_t get(ham_u64_t table_id, ham_size_t position,
                PDupeEntry *entry);

    /**
     * retrieve the whole table of duplicates
     *
     * @warning will return garbage if the key has no dupes!!
     * @warning memory has to be freed by the caller IF needs_free is true!
     */
    ham_status_t get_table(ham_u64_t table_id, PDupeTable **ptable,
                bool *needs_free);

  private:
    /** internal implementation of get_table() */
    ham_status_t get_table(PDupeTable **table_ref, Page **page,
                ham_u64_t table_id);

    /** the Environment which created this BlobManager */
    Environment *m_env;
};

} // namespace hamsterdb

#endif /* HAM_DUPLICATES_H__ */
