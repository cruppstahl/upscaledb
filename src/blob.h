/**
 * Copyright (C) 2005-2007 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See file COPYING.GPL2 and COPYING.GPL3 for License information.
 *
 * functions for reading/writing/allocating blobs (memory chunks of
 * arbitrary size)
 *
 */

#ifndef HAM_BLOB_H__
#define HAM_BLOB_H__

#ifdef __cplusplus
extern "C" {
#endif 

#include <ham/hamsterdb.h>
#include "page.h"
#include "keys.h"

#include "packstart.h"


/**
 * a blob structure (blob_t)
 *
 * every blob has a blob_t header; it holds flags and some other 
 * administrative information
 */
typedef HAM_PACK_0 struct HAM_PACK_1 
{
    /**
     * the blob ID - which is the absolute address/offset of this 
     * blob_t structure in the file
     */
    ham_offset_t _blobid;

    /**
     * the allocated size of the blob; this is the size, which is used
     * by the blob and it's header and maybe additional padding
     */
    ham_u64_t _allocated_size;

    /**
     * the size of the blob - this can hold additional padding, i.e. because
     * of encryption alignment (<= allocated_size)
     */
    ham_u64_t _real_size;

    /**
     * the size of the blob, as it's seen by the user (<= real_size)
     */
    ham_u64_t _user_size;

    /**
     * additional flags
     */
    ham_u32_t _flags;

} HAM_PACK_2 blob_t;

#include "packstop.h"

/**
 * get the blob ID (blob start address) of a blob_t
 */
#define blob_get_self(b)               (ham_db2h_offset((b)->_blobid))

/**
 * set the blob ID (blob start address) of a blob_t
 */
#define blob_set_self(b, s)            (b)->_blobid=ham_h2db_offset(s)

/**
 * get the allocated size of a blob_t
 */
#define blob_get_alloc_size(b)         (ham_db2h64((b)->_allocated_size))

/**
 * set the allocated size of a blob_t
 */
#define blob_set_alloc_size(b, s)      (b)->_allocated_size=ham_h2db64(s)

/**
 * get the real size of a blob_t
 */
#define blob_get_real_size(b)          (ham_db2h64((b)->_real_size))

/**
 * set the real size of a blob_t
 */
#define blob_set_real_size(b, s)      (b)->_real_size=ham_h2db64(s)

/**
 * get the user size of a blob_t
 */
#define blob_get_user_size(b)          (ham_db2h64((b)->_user_size))

/**
 * get the user size of a blob_t
 */
#define blob_set_user_size(b, s)       (b)->_user_size=ham_h2db64(s)

/**
 * get flags of a blob_t
 */
#define blob_get_flags(b)              (ham_db2h32((b)->_flags))

/**
 * set flags of a blob_t
 */
#define blob_set_flags(b, f)           (b)->_flags=ham_h2db32(f)


/**
 * a structure for a duplicate - used in a duplicate table
 */
typedef HAM_PACK_0 struct HAM_PACK_1 
{
    /*
     * reserved, for padding
     */
    ham_u8_t _padding;

    /*
     * the flags - same as KEY_TINY, KEY_SMALL, KEY_NULL
     */
    ham_u8_t _flags;

    /*
     * the record id (unless it's TINY, SMALL or NULL)
     */
    ham_offset_t _rid;

} dupe_entry_t;

/*
 * get the flags of a duplicate entry
 */
#define dupe_entry_get_flags(e)         (e)->_flags

/*
 * set the flags of a duplicate entry
 */
#define dupe_entry_set_flags(e, f)      (e)->_flags=f

/*
 * get the record id of a duplicate entry
 * 
 * !!!
 * if TINY or SMALL is set, the rid is actually a char*-pointer;
 * in this case, we must not use endian-conversion!
 */
#define dupe_entry_get_rid(e)                                                 \
         (((dupe_entry_get_flags(e)&KEY_BLOB_SIZE_TINY)                       \
          || (dupe_entry_get_flags(e)&KEY_BLOB_SIZE_SMALL))                   \
           ? (e)->_rid                                                        \
           : ham_db2h_offset((e)->_rid))

/*
 * set the record id of a duplicate entry
 *
 * !!! same problems as with get_rid():
 * if TINY or SMALL is set, the rid is actually a char*-pointer;
 * in this case, we must not use endian-conversion!
 */
#define dupe_entry_set_rid(e, r)                                              \
         (e)->_rid=(((dupe_entry_get_flags(e)&KEY_BLOB_SIZE_TINY)             \
                    || (dupe_entry_get_flags(e)&KEY_BLOB_SIZE_SMALL))         \
                     ? r                                                      \
                       : ham_h2db_offset(r))

/**
 * a structure for duplicates (dupe_table_t)
 */
typedef HAM_PACK_0 struct HAM_PACK_1 
{
    /*
     * the number of duplicates (used entries in this table)
     */
    ham_u32_t _count;

    /*
     * the capacity of entries in this table
     */
    ham_u32_t _capacity;

    /*
     * a dynamic array of duplicate entries
     */
    dupe_entry_t _entries[1];

} dupe_table_t;


/*
 * get the number of duplicates
 */
#define dupe_table_get_count(t)         (ham_db2h32((t)->_count))

/*
 * set the number of duplicates
 */
#define dupe_table_set_count(t, c)      (t)->_count=ham_h2db32(c)

/*
 * get the maximum number of duplicates
 */
#define dupe_table_get_capacity(t)      (ham_db2h32((t)->_capacity))

/*
 * set the maximum number of duplicates
 */
#define dupe_table_set_capacity(t, c)   (t)->_capacity=ham_h2db32(c)

/*
 * get a pointer to a duplicate entry #i
 */
#define dupe_table_get_entry(t, i)      (&(t)->_entries[i])

/**
 * allocate/create a blob
 *
 * returns the blob-id (the start address of the blob header) in @a blobid
 */
extern ham_status_t
blob_allocate(ham_db_t *db, ham_u8_t *data, 
        ham_size_t size, ham_u32_t flags, ham_offset_t *blobid);

/**
 * read a blob
 *
 * stores the data in @a record
 */
extern ham_status_t
blob_read(ham_db_t *db, ham_offset_t blobid, 
        ham_record_t *record, ham_u32_t flags);

/**
 * overwrite an existing blob
 *
 * will return an error if the blob does not exist
 * returns the blob-id (the start address of the blob header) in @a blobid
 */
extern ham_status_t
blob_overwrite(ham_db_t *db, ham_offset_t old_blobid, 
        ham_u8_t *data, ham_size_t size, ham_u32_t flags, 
        ham_offset_t *new_blobid);

/**
 * delete an existing blob
 */
extern ham_status_t
blob_free(ham_db_t *db, ham_offset_t blobid, ham_u32_t flags);

#define BLOB_FREE_ALL_DUPES   1

/**
 * delete the head element of a linked blob list
 */
extern ham_status_t
blob_free_dupe(ham_db_t *db, ham_offset_t blobid, ham_u32_t flags, 
        ham_offset_t *newhead);

/**
 * create a duplicate table and insert all entries in the duplicate
 * (max. two entries are allowed; first entry will be at the first position,
 * second entry will be set depending on the flags)
 *
 * OR, if the table already exists (i.e. table_id != 0), insert the 
 * entry depending on the flags (only one entry is allowed in this case)
 */
extern ham_status_t
blob_duplicate_insert(ham_db_t *db, ham_offset_t table_id, 
        ham_size_t position, ham_u32_t flags, 
        dupe_entry_t *entries, ham_size_t num_entries, 
        ham_offset_t *rid);

/*
 * get the number of duplicates
 */
extern ham_status_t
blob_duplicate_get_count(ham_db_t *db, ham_offset_t table_id,
        ham_size_t *count);

/*
 * get a duplicate
 */
extern ham_status_t 
blob_duplicate_get(ham_db_t *db, ham_offset_t table_id,
        ham_size_t position, dupe_entry_t *entry);


#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_BLOB_H__ */
