/*
 * Copyright (C) 2005-2010 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

/**
 * @brief journal entries for insert, erase, begin, commit, abort...
 *
 */

#ifndef HAM_JOURNAL_ENTRIES_H__
#define HAM_JOURNAL_ENTRIES_H__

#ifdef __cplusplus
extern "C" {
#endif 


#include "packstart.h"

/**
 * A journal entry for all txn related operations (begin, commit, abort)
 * 
 * This structure can be followed by one of the structures below
 * (journal_entry_insert_t or journal_entry_erase_t); the field 'followup_size'
 * is the structure size of this follow-up structure.
 * 
 */
typedef HAM_PACK_0 struct HAM_PACK_1 journal_entry_t
{
    /** the lsn of this entry */
    ham_u64_t _lsn;

    /** the size of the follow-up entry in bytes (may be padded) */
    ham_u64_t _followup_size;

    /** the transaction id */
    ham_u64_t _txn_id;

    /** the flags of this entry; the lowest 8 bits are the
     * type of this entry, see below */
    ham_u32_t _flags;

    /** a reserved value */
    ham_u32_t _reserved;

} HAM_PACK_2 journal_entry_t;

#include "packstop.h"

/* get the lsn */
#define journal_entry_get_lsn(j)                    (j)->_lsn

/* set the lsn */
#define journal_entry_set_lsn(j, lsn)               (j)->_lsn=lsn

/* get the transaction ID */
#define journal_entry_get_txn_id(j)                 (j)->_txn_id

/* set the transaction ID */
#define journal_entry_set_txn_id(j, id)             (j)->_txn_id=id

/* get the flags of this entry */
#define journal_entry_get_flags(j)                  (j)->_flags

/* set the flags of this entry */
#define journal_entry_set_flags(j, f)               (j)->_flags=f

/* get the type of this entry */
#define journal_entry_get_type(j)                   ((j)->_flags&0xf)

/* set the type of this entry */
#define journal_entry_set_type(j, t)                (j)->_flags|=(t)

/* get the follow-up size */
#define journal_entry_get_followup_size(j)          (j)->_followup_size

/* set the follow-up size */
#define journal_entry_set_followup_size(j, s)       (j)->_followup_size=s


#include "packstart.h"

/**
 * a journal entry for insert 
 */
typedef HAM_PACK_0 struct HAM_PACK_1 journal_entry_insert_t
{
    /** key size */
    ham_u16_t _key_size;

    /** record size */
    ham_u32_t _record_size;

    /** record partial size */
    ham_u32_t _record_partial_size;

    /** record partial offset */
    ham_u32_t _record_partial_offset;

    /** flags of ham_insert(), ham_cursor_insert() */
    ham_u32_t _insert_flags;

    /** data follows here - first 'key_size' bytes for the key, then 
     * 'record_size' bytes for the record (and maybe some padding) */
    ham_u8_t _data[1];

} HAM_PACK_2 journal_entry_insert_t;

#include "packstop.h"

/* insert: get the key size */
#define journal_entry_insert_get_key_size(j)        (j)->_key_size

/* insert: set the key size */
#define journal_entry_insert_set_key_size(j, s)     (j)->_key_size=(s)

/* insert: get the record size */
#define journal_entry_insert_get_record_size(j)     (j)->_record_size

/* insert: set the record size */
#define journal_entry_insert_set_record_size(j, s)  (j)->_record_size=(s)

/* insert: get the record partial size */
#define journal_entry_insert_get_record_partial_size(j)                     \
                                (j)->_record_partial_size

/* insert: set the record partial size */
#define journal_entry_insert_set_record_partial_size(j, s)                  \
                                (j)->_record_partial_size=(s)

/* insert: get the record partial offset */
#define journal_entry_insert_get_record_partial_offset(j)                   \
                                (j)->_record_partial_offset

/* insert: set the record partial offset */
#define journal_entry_insert_set_record_partial_offset(j, o)                \
                                (j)->_record_partial_offset=(o)

/* insert: get the flags */
#define journal_entry_insert_get_flags(j)           (j)->_insert_flags

/* insert: set the flags */
#define journal_entry_insert_set_flags(j, f)        (j)->_insert_flags=(f)

/* insert: get a pointer to the key data */
#define journal_entry_insert_get_key_data(j)        (&((j)->_data[0]))

/* insert: get a pointer to the record data */
#define journal_entry_insert_get_record_data(j)    (&(j)->_data[(j)->_key_size])


#include "packstart.h"

/**
 * a journal entry for erase 
 */
typedef HAM_PACK_0 struct HAM_PACK_1 journal_entry_erase_t
{
    /** key size */
    ham_u16_t _key_size;

    /** flags of ham_erase(), ham_cursor_erase() */
    ham_u32_t _erase_flags;

    /** which duplicate to erase */
    ham_u32_t _duplicate;

    /** the key data */
    ham_u8_t _data[1];

} HAM_PACK_2 journal_entry_erase_t;

#include "packstop.h"

/* erase: get the key size */
#define journal_entry_erase_get_key_size(j)        (j)->_key_size

/* erase: set the key size */
#define journal_entry_erase_set_key_size(j, s)     (j)->_key_size=(s)

/* erase: get the flags */
#define journal_entry_erase_get_flags(j)           (j)->_erase_flags

/* erase: set the flags */
#define journal_entry_erase_set_flags(j, f)        (j)->_erase_flags=(f)

/* erase: get the dupe index */
#define journal_entry_erase_get_dupe(j)            (j)->_duplicate

/* erase: set the dupe index */
#define journal_entry_erase_set_dupe(j, d)         (j)->_duplicate=(d)

/* erase: get a pointer to the key data */
#define journal_entry_erase_get_key_data(j)        (&(j)->_data[0])


#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_JOURNAL_ENTRIES_H__ */
