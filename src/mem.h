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
 * @brief memory management routines
 *
 */

#ifndef HAM_MEM_H__
#define HAM_MEM_H__

#include "internal_fwd_decl.h"

#include <string.h>


#ifdef __cplusplus
extern "C" {
#endif 


/**
 * typedefs for allocator function pointers
 */
typedef void *(*alloc_func_t)(mem_allocator_t *self, const char *file, 
                   int line, ham_size_t size);
typedef void  (*free_func_t) (mem_allocator_t *self, const char *file, 
                   int line, const void *ptr);
typedef void *(*realloc_func_t) (mem_allocator_t *self, const char *file, 
                   int line, const void *ptr, ham_size_t size);
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
#define ham_default_allocator_new()                                           \
                                _ham_default_allocator_new(__FILE__, __LINE__)
extern mem_allocator_t *
_ham_default_allocator_new(const char *fname, const int lineno);


#if defined(_MSC_VER) && defined(_CRTDBG_MAP_ALLOC)

#include "db.h"
#include "env.h"

#pragma push_macro("alloc")
#pragma push_macro("free")
#pragma push_macro("realloc")
#pragma push_macro("close")
#undef alloc
#undef free
#undef realloc
#undef close

/** 
 * allocate memory 
 * returns 0 if memory can not be allocated; the default implementation
 * uses malloc()
 */
#define allocator_alloc(a, size)   _allocator_alloc(a, __FILE__, __LINE__, size)

static __inline void *
_allocator_alloc(mem_allocator_t *a, const char *fname, 
                    const int lineno, ham_size_t size)
{
    return (a->alloc(a, fname, lineno, size));
}

/** 
 * free memory 
 * the default implementation uses free()
 */
#define allocator_free(a, ptr)      _allocator_free(a, __FILE__, __LINE__, ptr)

static __inline void 
_allocator_free(mem_allocator_t *a, const char *fname, 
                    const int lineno, const void *ptr)
{
    if (ptr)
        a->free(a, fname, lineno, ptr);
}

/** 
 * re-allocate memory 
 * returns 0 if memory can not be allocated; the default implementation
 * uses realloc()
 */
#define allocator_realloc(a, p, size)                                         \
                              _allocator_realloc(a, __FILE__, __LINE__, p, size)

static __inline void *
_allocator_realloc(mem_allocator_t *a, const char *fname, 
                    const int lineno, const void *ptr, ham_size_t size)
{
    return (a->realloc(a, fname, lineno, ptr, size));
}

/**
 * a calloc function
 */
#define allocator_calloc(a, size) _allocator_calloc(a, __FILE__, __LINE__, size)

static __inline void *
_allocator_calloc(mem_allocator_t *a, const char *fname, 
                const int lineno, ham_size_t size)
{
    void *p = a->alloc(a, fname, lineno, size);

    if (p)
        memset(p, 0, size);
    return (p);
}

#pragma pop_macro("alloc")
#pragma pop_macro("free")
#pragma pop_macro("realloc")
#pragma pop_macro("close")

#else /* defined(_MSC_VER) && defined(_CRTDBG_MAP_ALLOC) */

/** 
 * allocate memory 
 * returns 0 if memory can not be allocated; the default implementation
 * uses malloc()
 */
#define allocator_alloc(a, size)              (a)->alloc(a, "-", __LINE__, size)

/** 
 * free memory 
 * the default implementation uses free()
 */
#define allocator_free(a, ptr)                (a)->free(a, "-", __LINE__, ptr)

/** 
 * re-allocate memory 
 * returns 0 if memory can not be allocated; the default implementation
 * uses realloc()
 */
#define allocator_realloc(a, p, size)    (a)->realloc(a, "-", __LINE__, p, size)

/**
 * a calloc function
 */
#define allocator_calloc(a, size)         _allocator_calloc(a, __LINE__, size)

static __inline void *
_allocator_calloc(mem_allocator_t *a, const int lineno, ham_size_t size)
{
    void *p = a->alloc(a, "-", lineno, size);

    if (p)
        memset(p, 0, size);
    return (p);
}

#endif /* defined(_MSC_VER) && defined(_CRTDBG_MAP_ALLOC) */

#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_MEM_H__ */
