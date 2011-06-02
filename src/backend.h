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
 * @brief a base-"class" for a generic database backend
 *
 */

#ifndef HAM_BACKEND_H__
#define HAM_BACKEND_H__

#include "internal_fwd_decl.h"


#ifdef __cplusplus
extern "C" {
#endif 


/**
* @defgroup ham_cb_status hamsterdb Backend Node/Page Enumerator Status Codes
* @{
*/

/** continue with the traversal */
#define CB_CONTINUE            0 
/** do not not descend another level (or descend from page to key traversal) */
#define CB_DO_NOT_DESCEND    1 
/** stop the traversal entirely */
#define CB_STOP                2 

/**
 * @}
 */


/**
 * a callback function for enumerating the index nodes/pages using the 
 * @ref ham_backend_t::_fun_enumerate callback/method.
 *
 * @param event one of the @ref ham_cb_event state codes
 *
 * @param param1
 * @param param2
 * @param context
 *
 * @return one of the @ref ham_cb_status values or a @ref ham_status_codes 
 *         error code when an error occurred.
 */
typedef ham_status_t (*ham_enumerate_cb_t)(int event, void *param1, 
                    void *param2, void *context);

/**
* @defgroup ham_cb_event hamsterdb Backend Node/Page Enumerator State Codes
* @{
*/

/** descend one level; param1 is an integer value with the new level */
#define ENUM_EVENT_DESCEND      1

/** start of a new page; param1 points to the page */
#define ENUM_EVENT_PAGE_START   2

/** end of a new page; param1 points to the page */
#define ENUM_EVENT_PAGE_STOP    3

/** an item in the page; param1 points to the key; param2 is the index 
 * of the key in the page */
#define ENUM_EVENT_ITEM         4

/**
* @}
*/

/**
 * the backend structure - these functions and members are "inherited"
 * by every other backend (i.e. btree, hashdb etc). 
 */
#define BACKEND_DECLARATIONS(clss)                                      \
    /**                                                                 \
     * create and initialize a new backend                              \
     *                                                                  \
     * @remark this function is called after the @a ham_db_t structure  \
     * and the file were created                                        \
     *                                                                  \
     * the @a flags are stored in the database; only transfer           \
     * the persistent flags!                                            \
     */                                                                 \
    ham_status_t (*_fun_create)(clss *be, ham_u16_t keysize,            \
            ham_u32_t flags);                                           \
                                                                        \
    /**                                                                 \
     * open and initialize a backend                                    \
     *                                                                  \
     * @remark this function is called after the ham_db_structure       \
     * was allocated and the file was opened                            \
     */                                                                 \
    ham_status_t (*_fun_open)(clss *be, ham_u32_t flags);               \
                                                                        \
    /**                                                                 \
     * close the backend                                                \
     *                                                                  \
     * @remark this function is called before the file is closed        \
     */                                                                 \
    ham_status_t (*_fun_close)(clss *be);                               \
                                                                        \
    /**                                                                 \
     * flush the backend                                                \
     *                                                                  \
     * @remark this function is called during ham_flush                 \
     */                                                                 \
    ham_status_t (*_fun_flush)(clss *be);                               \
                                                                        \
    /**                                                                 \
     * find a key in the index                                          \
     */                                                                 \
    ham_status_t (*_fun_find)(clss *be, ham_key_t *key,                 \
            ham_record_t *record, ham_u32_t flags);                     \
                                                                        \
    /**                                                                 \
     * insert (or update) a key in the index                            \
     *                                                                  \
     * the backend is responsible for inserting or updating the         \
     * record. (see blob.h for blob management functions)               \
     */                                                                 \
    ham_status_t (*_fun_insert)(clss *be,                               \
            ham_key_t *key, ham_record_t *record, ham_u32_t flags);     \
                                                                        \
    /**                                                                 \
     * erase a key in the index                                         \
     */                                                                 \
    ham_status_t (*_fun_erase)(clss *be, ham_key_t *key,                \
            ham_u32_t flags);                                           \
                                                                        \
    /**                                                                 \
     * iterate the whole tree and enumerate every item                  \
     */                                                                 \
    ham_status_t (*_fun_enumerate)(clss *be,                            \
            ham_enumerate_cb_t cb, void *context);                      \
                                                                        \
    /**                                                                 \
     * verify the whole tree                                            \
     */                                                                 \
    ham_status_t (*_fun_check_integrity)(clss *be);                     \
                                                                        \
    /**                                                                 \
     * free all allocated resources                                     \
     *                                                                  \
     * @remark this function is called after _fun_close()               \
     */                                                                 \
    ham_status_t (*_fun_delete)(clss *be);                              \
                                                                        \
    /**                                                                 \
     * estimate the number of keys per page, given the keysize          \
     */                                                                 \
    ham_status_t (*_fun_calc_keycount_per_page)(clss *be,               \
                  ham_size_t *keycount, ham_u16_t keysize);             \
                                                                        \
    /**                                                                 \
     * Create a new cursor instance.                                    \
     */                                                                 \
    ham_status_t (*_fun_cursor_create)(clss *be,                        \
                ham_db_t *db, ham_txn_t *txn,                           \
                ham_u32_t flags, ham_cursor_t **cu);                    \
                                                                        \
    /**                                                                 \
     * Close (and free) all cursors related to this database table.     \
     */                                                                 \
    ham_status_t (*_fun_close_cursors)(clss *be, ham_u32_t flags);      \
                                                                        \
    /**                                                                 \
     * uncouple all cursors from a page                                 \
     *                                                                  \
     * @remark this is called whenever the page is deleted or           \
     * becoming invalid                                                 \
     */                                                                 \
    ham_status_t (*_fun_uncouple_all_cursors)(clss *be,                 \
                ham_page_t *page, ham_size_t start);                    \
                                                                        \
    /**                                                                 \
     * Remove all extended keys for the given @a page from the          \
     * extended key cache.                                              \
     */                                                                 \
    ham_status_t (*_fun_free_page_extkeys)(clss *be,                    \
                ham_page_t *page, ham_u32_t flags);                     \
                                                                        \
    /**                                                                 \
     * pointer to the database object                                   \
     */                                                                 \
    ham_db_t *_db;                                                      \
                                                                        \
    /**                                                                 \
     * the last used record number                                      \
     */                                                                 \
    ham_offset_t _recno;                                                \
                                                                        \
    /**                                                                 \
     * the keysize of this backend index                                \
     */                                                                 \
    ham_u16_t _keysize;                                                 \
                                                                        \
    /**                                                                 \
     * flag if this backend has to be written to disk                   \
     */                                                                 \
    unsigned _dirty: 1;                                                 \
                                                                        \
    /**                                                                 \
     * flag if this backend has been fully initialized                  \
     */                                                                 \
    unsigned _is_active: 1;                                             \
                                                                        \
    /**                                                                 \
     * the persistent flags of this backend index                       \
     */                                                                 \
    ham_u32_t _flags


#include "packstart.h"

/**
* A generic backend structure, which has the same memory layout as 
* all other backends.
*
* @remark We're pre-declaring struct ham_backend_t and the typedef 
* to avoid syntax errors in @ref BACKEND_DECLARATIONS .
*
* @remark Since this structure is not persistent, we don't really
* need packing; however, with Microsoft Visual C++ 8, the
* offset of ham_backend_t::_flags (the last member) is not the same
* as the offset of ham_btree_t::_flags, unless packing is enabled.
*/
HAM_PACK_0 struct HAM_PACK_1 ham_backend_t
{
    BACKEND_DECLARATIONS(ham_backend_t);
} HAM_PACK_2;

#include "packstop.h"

/**
 * convenience macro to get the database pointer of a ham_backend_t-structure
 */
#define be_get_db(be)                        (be)->_db

/**
 * get the keysize
 */
#define be_get_keysize(be)                  (be)->_keysize

/**
 * set the keysize
 */
#define be_set_keysize(be, ks)              (be)->_keysize=(ks)

/**
 * get the flags
 */
#define be_get_flags(be)                    (be)->_flags

/**
 * set the flags
 */
#define be_set_flags(be, f)                 (be)->_flags=(f)

/**
 * get the last used record number
 */
#define be_get_recno(be)                    (be)->_recno

/**
 * set the last used record number
 */
#define be_set_recno(be, rn)                (be)->_recno=(rn)

/**
 * get the dirty-flag
 */
#define be_is_dirty(be)                     (be)->_dirty

/**
 * set the dirty-flag
 */
#define be_set_dirty(be, d)                 (be)->_dirty=!!(d)

/**
 * get the active-flag
 */
#define be_is_active(be)                    (be)->_is_active

/**
 * set the active-flag
 */
#define be_set_active(be, d)                (be)->_is_active=!!(d)


#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_BACKEND_H__ */
