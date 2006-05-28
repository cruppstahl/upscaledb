/**
 * Copyright (C) 2005, 2006 Christoph Rupp (chris@crupp.de)
 * see file LICENSE for license and copyright information
 *
 * this file has error logging routines and a strerror() replacement
 *
 */

#ifndef HAM_ERROR_H__
#define HAM_ERROR_H__

#ifdef __cplusplus
extern "C" {
#endif 

#include <ham/types.h>

/** 
 * in debug mode we write trace()-messages to stderr, and assert() 
 * is enabled.
 *
 * not every preprocessor supports ellipsis as macro-arguments -
 * a separate version for crippled preprocessors will be provided
 * when necessary
 */
#ifdef HAM_DEBUG
#   define ham_trace(f, ...) _ham_log(__FILE__, __LINE__, f, __VA_ARGS__) 
#   define ham_assert(e, f, ...) ((e) ?      \
                                  (void)0 : \
                        _ham_verify(__FILE__, __LINE__, #e, f, __VA_ARGS__))
#else /* HAM_RELEASE */
#   define ham_trace(...)  (void)0
#   define ham_assert(...) (void)0
#endif /* HAM_DEBUG */

/**
 * log() and verify() are available in every build
 */
#define ham_log(f, ...)       _ham_log(__FILE__, __LINE__, f, __VA_ARGS__)
#define ham_verify(e, f, ...) _ham_verify(__FILE__, __LINE__,#e,f,__VA_ARGS__)

/**
 * function prototypes
 */
extern void _ham_log(const char *file, int line, const char *format, ...);
extern void _ham_verify(const char *file, int line, const char *msg, 
        const char *format, ...);


#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_ERROR_H__ */
