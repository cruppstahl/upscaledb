/**
 * Copyright (C) 2005, 2006 Christoph Rupp (chris@crupp.de)
 * see file COPYING for licence information
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
 * if a blob is bigger than a page, it is split into several pages 
 * ("blob_part_t").  information about these overflow pages 
 * (page ID and page length) are stored in the '_parts'-array.
 *
 * if the blob is too big, and the number of blob_part_t-pages is too high for
 * the blob_t::_parts array (because the whole blob_t structure would
 * not fit in a single page), the blob_t::_parts_overflow member points
 * to a page with another blob_t structure and more blob_part_t elements.
 */
typedef HAM_PACK_0 struct HAM_PACK_1 
{
    /**
     * the blob ID
     */
    ham_offset_t _blobid;

    /**
     * the total size of the blob
     */
    ham_u64_t _total_size;

    /**
     * additional flags
     */
    ham_u32_t _flags;

    /**
     * pointer to an overflow page with a blob_t header
     * and more blob_part_t elements
     */
    ham_offset_t _parts_overflow;

    /**
     * number of elements in _parts
     */
    ham_u16_t _parts_size;

    /**
     * array of blob_part_t descriptors
     */
    struct blob_part_t {
        /**
         * ID of the page
         */
        ham_offset_t _page;

        /**
         * size of data in the page; w/o the header information!
         */
        ham_u32_t _size;

    } _parts[1];

    /**
     * the data itself follows here...
     */

} HAM_PACK_2 blob_t;

#include "packstop.h"

/**
 * get a blob_t from a ham_page_t
 */
extern blob_t *
ham_page_get_blob(ham_page_t *page, ham_offset_t blobid);

/**
 * get the blob ID (blob start address) of a blob_t
 */
#define blob_get_self(b)               (ham_db2h_offset((b)->_blobid))

/**
 * set the blob ID (blob start address) of a blob_t
 */
#define blob_set_self(b, s)            (b)->_blobid=ham_h2db_offset(s)

/**
 * get the total size of a blob_t
 */
#define blob_get_total_size(b)         (ham_db2h64((b)->_total_size))

/**
 * get the total size of a blob_t
 */
#define blob_set_total_size(b, s)      (b)->_total_size=ham_h2db64(s)

/**
 * get flags of a blob_t
 */
#define blob_get_flags(b)              (ham_db2h32((b)->_flags))

/**
 * set flags of a blob_t
 */
#define blob_set_flags(b, f)           (b)->_flags=ham_h2db32(f)

/**
 * get the overflow pointer to more parts
 */
#define blob_get_parts_overflow(b)     (ham_db2h_offset((b)->_parts_overflow))

/**
 * set the overflow pointer to more parts
 */
#define blob_set_parts_overflow(b, o)  (b)->_parts_overflow=ham_h2db_offset(o)

/**
 * get number of elements in _parts
 */
#define blob_get_parts_size(b)         (ham_db2h16((b)->_parts_size))

/**
 * set number of elements in _parts
 */
#define blob_set_parts_size(b, s)      (b)->_parts_size=ham_h2db16(s)

/**
 * get the offset of part #i
 */
#define blob_get_part_offset(b, i)     (ham_db2h_offset((b)->_parts[i]._page))

/**
 * set the offset of part #i
 */
#define blob_set_part_offset(b, i, o)  (b)->_parts[i]._page=ham_h2db_offset(o)

/**
 * get the size of part #i
 */
#define blob_get_part_size(b, i)       (ham_db2h32((b)->_parts[i]._size))

/**
 * set the size of part #i
 */
#define blob_set_part_size(b, i, s)    (b)->_parts[i]._size=ham_h2db32(s)

/**
 * get a pointer to the data of this blob_t; the data follows 
 * immediately after the blob_t structure
 */
#define blob_get_data(b)    ((ham_u8_t *)(b)->_parts[blob_get_parts_size(b)])

/**
 * write a blob
 *
 * returns the blob-id (the start address of the blob header) in 'blobid'
 */
extern ham_status_t
blob_allocate(ham_db_t *db, ham_txn_t *txn, ham_u8_t *data, 
        ham_size_t datasize, ham_u32_t flags, ham_offset_t *blobid);

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
 * returns the blob-id (the start address of the blob header) in 'blobid'
 */
extern ham_status_t
blob_replace(ham_db_t *db, ham_txn_t *txn, ham_offset_t old_blobid, 
        ham_u8_t *data, ham_size_t datasize, ham_u32_t flags, 
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
