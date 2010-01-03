/**
 * Copyright (C) 2005-2008 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 *
 *
 * memory management routines
 *
 */

#ifndef HAM_MEM_H__
#define HAM_MEM_H__

#include <ham/types.h>


#ifdef __cplusplus
extern "C" {
#endif 

/**
 * typedefs for allocator function pointers
 */
struct mem_allocator_t;
typedef struct mem_allocator_t mem_allocator_t;

typedef void *(*alloc_func_t)(mem_allocator_t *self, const char *file, 
                   int line, ham_size_t size);
typedef void  (*free_func_t) (mem_allocator_t *self, const char *file, 
                   int line, void *ptr);
typedef void *(*realloc_func_t) (mem_allocator_t *self, const char *file, 
                   int line, void *ptr, ham_size_t size);
typedef void  (*close_func_t)(mem_allocator_t *self);

/**
 * an allocator "class"
 */
struct mem_allocator_t
{
    alloc_func_t alloc;
    free_func_t  free;
    realloc_func_t realloc;
    close_func_t close;
    void *priv;
};

/**
 * a factory for creating the standard allocator (based on libc malloc 
 * and free)
 */
extern mem_allocator_t *
ham_default_allocator_new(void);

/** 
 * allocate memory 
 * returns 0 if memory can not be allocated; the default implementation
 * uses malloc()
 */
#define ham_mem_alloc(db, size) db_get_allocator(db)->alloc(                  \
                                    db_get_allocator(db), __FILE__, __LINE__, \
                                        size)

/**
 * same as above, but with an mem_allocator_t pointer
 */
#define allocator_alloc(a, size)  (a)->alloc(a, __FILE__, __LINE__, size)

/** 
 * free memory 
 * the default implementation uses free()
 */
#define ham_mem_free(db, ptr)   db_get_allocator(db)->free(                   \
                                    db_get_allocator(db), __FILE__, __LINE__, \
                                        ptr)

/**
 * same as above, but with an mem_allocator_t pointer
 */
#define allocator_free(a, ptr)  (a)->free(a, __FILE__, __LINE__, ptr)

/** 
 * re-allocate memory 
 * returns 0 if memory can not be allocated; the default implementation
 * uses realloc()
 */
#define ham_mem_realloc(db, p, size) db_get_allocator(db)->realloc(           \
                                    db_get_allocator(db), __FILE__, __LINE__, \
                                        p, size)

/**
 * same as above, but with an mem_allocator_t pointer
 */
#define allocator_realloc(a, p, size)  (a)->realloc(a, __FILE__, __LINE__,    \
                                        p, size)

/**
 * a calloc function
 */
struct ham_db_t;
extern void *ham_mem_calloc(struct ham_db_t *db, ham_size_t size);

struct ham_env_t;
extern void *ham_mem_calloc_env(struct ham_env_t *env, ham_size_t size);


#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_MEM_H__ */
