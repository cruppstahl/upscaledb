/**
 * Copyright (C) 2005, 2006 Christoph Rupp (chris@crupp.de)
 * see file LICENSE for license and copyright information
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
 * allocate memory 
 * returns 0 if memory can not be allocated; the default implementation
 * uses malloc()
 */
#define ham_mem_alloc(size) _ham_mem_malloc(__FILE__, __LINE__, size)

/** 
 * free memory 
 * the default implementation uses free()
 */
#define ham_mem_free(ptr)  _ham_mem_free(__FILE__, __LINE__, ptr)

/**
 * the implementation of ham_mem_malloc() and ham_mem_free()
 */
extern void *_ham_mem_malloc(const char *file, int line, ham_u32_t size);
extern void  _ham_mem_free(const char *file, int line, void *ptr);

#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_MEM_H__ */
