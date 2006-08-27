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
 * every blob has a blob_t header; it holds flags and some other 
 * administrative information. and it has a "toc" - table of contents - 
 * with pointers to the chunks in the file; all those chunks 
 * together are the data of the blob. 
 *
 * since (theoretically) it's possible that a blob is very, very large, and 
 * therefore the toc of a blob_t does not fit in a single page, every 
 * blob_t structure has an overflow pointer to another blob_t header. 
 */
typedef HAM_PACK_0 struct HAM_PACK_1 
{
    /**
     * the blob ID - which is the absolute address/offset of this 
     * blob_t structure in the file
     */
    ham_offset_t _blobid;

    /**
     * the total size of the blob
     */
    ham_u64_t _total_size;

    /**
     * the real size of the blob - if a blob needs padding because it's 
     * encrypted, the real_size holds the size before the padding
     */
    ham_u64_t _real_size;

    /**
     * additional flags
     */
    ham_u32_t _flags;

    /**
     * pointer to an overflow page with a blob_t header
     * and more blob_chunk_t elements
     */
    ham_offset_t _overflow;

    /**
     * number of used elements in _toc
     */
    ham_u16_t _toc_usedsize;

    /**
     * maximum capacity of _toc
     */
    ham_u16_t _toc_maxsize;

    /**
     * array of blob_chunk_t descriptors
     */
    struct blob_chunk_t {
        /**
         * absolute address of the chunk in the file
         */
        ham_offset_t _offset;

        /**
         * size of this chunk
         */
        ham_u32_t _size;

        /**
         * flags of this chunk
         */
        ham_u32_t _flags;

    } _toc[1];

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
 * get the total size of a blob_t
 */
#define blob_get_total_size(b)         (ham_db2h64((b)->_total_size))

/**
 * get the total size of a blob_t
 */
#define blob_set_total_size(b, s)      (b)->_total_size=ham_h2db64(s)

/**
 * get the real size of a blob_t
 */
#define blob_get_real_size(b)          (ham_db2h64((b)->_real_size))

/**
 * get the real size of a blob_t
 */
#define blob_set_real_size(b, s)       (b)->_real_size=ham_h2db64(s)

/**
 * get flags of a blob_t
 */
#define blob_get_flags(b)              (ham_db2h32((b)->_flags))

/**
 * set flags of a blob_t
 */
#define blob_set_flags(b, f)           (b)->_flags=ham_h2db32(f)

/**
 * get the overflow pointer to the next blob_t header
 */
#define blob_get_overflow(b)           (ham_db2h_offset((b)->_overflow))

/**
 * set the overflow pointer 
 */
#define blob_set_overflow(b, o)        (b)->_overflow=ham_h2db_offset(o)

/**
 * get number of used elements in the toc
 */
#define blob_get_toc_usedsize(b)       (ham_db2h16((b)->_toc_usedsize))

/**
 * set number of used elements in the toc
 */
#define blob_set_toc_usedsize(b, s)    (b)->_toc_usedsize=ham_h2db16(s)

/**
 * get maximum capacity of the toc
 */
#define blob_get_toc_maxsize(b)        (ham_db2h16((b)->_toc_maxsize))

/**
 * set maximum capacity of the toc
 */
#define blob_set_toc_maxsize(b, s)     (b)->_toc_maxsize=ham_h2db16(s)

/**
 * get the offset of chunk #i
 */
#define blob_get_chunk_offset(b, i)    (ham_db2h_offset((b)->_toc[i]._offset))

/**
 * set the offset of chunk #i
 */
#define blob_set_chunk_offset(b, i, o) (b)->_toc[i]._offset=ham_h2db_offset(o)

/**
 * get the size of chunk #i
 */
#define blob_get_chunk_size(b, i)      (ham_db2h32((b)->_toc[i]._size))

/**
 * set the size of chunk #i
 */
#define blob_set_chunk_size(b, i, s)   (b)->_toc[i]._size=ham_h2db32(s)

/**
 * get the flags of chunk #i
 */
#define blob_get_chunk_flags(b, i)     (ham_db2h32((b)->_toc[i]._flags))

/**
 * set the flags of chunk #i
 */
#define blob_set_chunk_flags(b, i, f)  (b)->_toc[i]._flags=ham_h2db32(f)

/**
 * the following chunk flags are available
 *
 * the chunk is directly adjacent to the blob_t header
 */
#define BLOB_CHUNK_NEXT_TO_HEADER       1

/*
 * the chunk spans a full page
 */
#define BLOB_CHUNK_SPANS_PAGE           2

/**
 * write a blob
 *
 * returns the blob-id (the start address of the blob header) in 'blobid'
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
 * returns the blob-id (the start address of the blob header) in 'blobid'
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
