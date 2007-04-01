/**
 * Copyright (C) 2005-2007 Christoph Rupp (chris@crupp.de).
 * All rights reserved. See file LICENSE for licence and copyright
 * information.
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
 * free memory 
 * the default implementation uses free()
 */
#define ham_mem_free(db, ptr)   db_get_allocator(db)->free(                   \
                                    db_get_allocator(db), __FILE__, __LINE__, \
                                        ptr)

/**
 * frees memory, then sets the pointer to NULL
 */
#define ham_mem_free_null(d, p) do { db_get_allocator(db)->free(              \
                                    db_get_allocator(db), __FILE__, __LINE__, \
                                        ptr); ptr=0; } while (0)


#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_MEM_H__ */
