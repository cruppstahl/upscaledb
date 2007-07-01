/**
 * Copyright (C) 2005-2007 Christoph Rupp (chris@crupp.de).
 * All rights reserved. See file LICENSE for licence and copyright
 * information.
 *
 * a base-"class" for a backend
 *
 */

#ifndef HAM_BACKEND_H__
#define HAM_BACKEND_H__

#ifdef __cplusplus
extern "C" {
#endif 

#include <ham/hamsterdb.h>
#include <ham/hamsterdb_int.h>

/**
 * a callback function for enumeration 
 */
typedef void (*ham_enumerate_cb_t)(int event, void *param1, void *param2, 
        void *context);

/** descend one level; param1 is an integer value with the new level */
#define ENUM_EVENT_DESCEND      1

/** start of a new page; param1 points to the page */
#define ENUM_EVENT_PAGE_START   2

/** an item in the page; param1 points to the key; param2 is the index 
 * of the key in the page */
#define ENUM_EVENT_ITEM         3

/**
 * the backend structure - these functions and members are "inherited"
 * by every other backend (i.e. btree, hashdb etc). 
 */
#define BACKEND_DECLARATIONS(clss)                                      \
    /**                                                                 \
     * create and initialize a new backend                              \
     *                                                                  \
     * @remark this function is called after the ham_db_t structure     \ 
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
     *                                                                  \
     * @remark this function returns the record of the key              \
     * in @a rid                                                        \
     */                                                                 \
    ham_status_t (*_fun_erase)(clss *be, ham_key_t *key,                \
            ham_offset_t *rid, ham_u32_t *intflags, ham_u32_t flags);   \
                                                                        \
    /**                                                                 \
     * iterate the whole tree and enumerate every item                  \ 
     */                                                                 \
    ham_status_t (*_fun_enumerate)(clss *be,                            \
            ham_enumerate_cb_t cb, void *context);                      \
                                                                        \
    /**                                                                 \
     * verify the whole tree                                            \
     *                                                                  \
     * @remark this function is available in DEBUG-mode only            \
     */                                                                 \
    ham_status_t (*_fun_check_integrity)(clss *be);                     \
                                                                        \
    /**                                                                 \
     * free all allocated resources                                     \
     *                                                                  \
     * @remark this function is called after _fun_close()               \
     */                                                                 \
    void (*_fun_delete)(clss *be);                                      \
                                                                        \
    /**                                                                 \
     * pointer to the database object                                   \
     */                                                                 \
    ham_db_t *_db;                                                      \
                                                                        \
    /**                                                                 \
     * the keysize of this backend index                                \
     */                                                                 \
    ham_u16_t _keysize;                                                 \
                                                                        \
    /**                                                                 \
     * the persistent flags of this backend index                       \
     */                                                                 \
    ham_u32_t _flags;


/**
 * a generic backend structure, which has the same memory layout as 
 * all other backends
 *
 * @remark we're pre-declaring struct ham_backend_t and the typedef 
 * to avoid syntax errors in BACKEND_DECLARATIONS
 */
struct ham_backend_t;
typedef struct ham_backend_t ham_backend_t;

struct ham_backend_t
{
    BACKEND_DECLARATIONS(ham_backend_t)
};

/*
 * get the keysize
 */
#define be_get_keysize(be)                  (be)->_keysize

/*
 * set the keysize
 */
#define be_set_keysize(be, ks)              (be)->_keysize=ks

/*
 * get the flags
 */
#define be_get_flags(be)                    (be)->_flags

/*
 * set the flags
 */
#define be_set_flags(be, f)                 (be)->_flags=f


#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_BACKEND_H__ */
