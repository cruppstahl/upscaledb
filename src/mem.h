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
 *
 * memory management routines
 *
 */

#ifndef HAM_MEM_H__
#define HAM_MEM_H__

#ifdef __cplusplus
extern "C" {
#endif 

#include <ham/types.h>

/**
 * typedefs for allocator function pointers
 */
struct mem_allocator_t;
typedef struct mem_allocator_t mem_allocator_t;

typedef void *(*alloc_func_t)(mem_allocator_t *self, const char *file, 
                   int line, ham_size_t size);
typedef void  (*free_func_t) (mem_allocator_t *self, const char *file, 
                   int line, void *ptr);
typedef void  (*close_func_t)(mem_allocator_t *self);

/**
 * an allocator "class"
 */
struct mem_allocator_t
{
    alloc_func_t alloc;
    free_func_t  free;
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
 * a calloc function
 */
struct ham_db_t;
extern void *ham_mem_calloc(struct ham_db_t *db, ham_size_t size);


#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_MEM_H__ */
