/**
 * Copyright (C) 2005, 2006 Christoph Rupp (chris@crupp.de)
 * see file LICENSE for licence information
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
#include "txn.h"

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
 * write a blob
 *
 * returns the blob-id (the start address of the blob header) in @a blobid
 */
extern ham_status_t
blob_allocate(ham_db_t *db, ham_txn_t *txn, ham_u8_t *data, 
        ham_size_t size, ham_u32_t flags, ham_offset_t *blobid);

/**
 * read a blob
 *
 * stores the data in @a record
 */
extern ham_status_t
blob_read(ham_db_t *db, ham_txn_t *txn, ham_offset_t blobid, 
        ham_record_t *record, ham_u32_t flags);

/**
 * replace an existing blob
 *
 * will return an error if the blob does not exist
 * returns the blob-id (the start address of the blob header) in @a blobid
 */
extern ham_status_t
blob_replace(ham_db_t *db, ham_txn_t *txn, ham_offset_t old_blobid, 
        ham_u8_t *data, ham_size_t size, ham_u32_t flags, 
        ham_offset_t *new_blobid);

/**
 * delete an existing blob
 */
extern ham_status_t
blob_free(ham_db_t *db, ham_txn_t *txn, ham_offset_t blobid, ham_u32_t flags);


#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_BLOB_H__ */
