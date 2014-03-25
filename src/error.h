/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef HAM_ERROR_H__
#define HAM_ERROR_H__

#include <ham/hamsterdb.h>

namespace hamsterdb {

//
// A generic exception for storing a status code
//
struct Exception
{
  Exception(ham_status_t st)
    : code(st) {
  }

  ham_status_t code;
};

// the global error handler function
extern ham_errhandler_fun g_handler;

// the default error handler
void HAM_CALLCONV default_errhandler(int level, const char *message);

// function prototypes, required for asserts etc
extern void dbg_lock(void);

extern void dbg_unlock(void);

extern void dbg_prepare(int level, const char *file, int line,
    const char *function, const char *expr);

extern void dbg_log(const char *format, ...);

#define CLANG_ANALYZER_NORETURN
#if __clang__
#  if __has_feature(attribute_analyzer_noreturn)
#    undef CLANG_ANALYZER_NORETURN
#    define CLANG_ANALYZER_NORETURN __attribute__((analyzer_noreturn))
#  endif
#endif

// causes the actual abort()
extern void dbg_verify_failed(const char *format, ...) CLANG_ANALYZER_NORETURN;

// a hook for unittests; will be triggered when an assert fails
extern void (*ham_test_abort)();

// if your compiler does not support __FUNCTION__, you can define it here:
//  #define __FUNCTION__ 0

/*
 * in debug mode we write trace()-messages to stderr, and assert()
 * is enabled.
 *
 * not every preprocessor supports ellipsis as macro-arguments -
 * therefore we have to use brackets, so preprocessors treat multiple
 * arguments like a single argument. and we need to lock the output,
 * otherwise we are not thread-safe. this is super-ugly.
 */
#ifdef HAM_DEBUG
#   define ham_assert(e) if (!(e)) {                                          \
                hamsterdb::dbg_lock();                                        \
                hamsterdb::dbg_prepare(HAM_DEBUG_LEVEL_FATAL, __FILE__,       \
                    __LINE__, __FUNCTION__, #e);                              \
                hamsterdb::dbg_verify_failed(0);                              \
                hamsterdb::dbg_unlock();                                      \
              }
#else /* !HAM_DEBUG */
#   define ham_assert(e)      (void)0
#endif /* HAM_DEBUG */

// ham_log() and ham_verify() are available in every build
#define ham_trace(f)     do {                                                 \
                hamsterdb::dbg_lock();                                        \
                hamsterdb::dbg_prepare(HAM_DEBUG_LEVEL_DEBUG, __FILE__,       \
                    __LINE__, __FUNCTION__, 0);                               \
                hamsterdb::dbg_log f;                                         \
                hamsterdb::dbg_unlock();                                      \
              } while (0)

#define ham_log(f)       do {                                                 \
                hamsterdb::dbg_lock();                                        \
                hamsterdb::dbg_prepare(HAM_DEBUG_LEVEL_NORMAL, __FILE__,      \
                    __LINE__, __FUNCTION__, 0);                               \
                hamsterdb::dbg_log f;                                         \
                hamsterdb::dbg_unlock();                                      \
              } while (0)

#define ham_verify(e)      if (!(e)) {                                        \
                hamsterdb::dbg_lock();                                        \
                hamsterdb::dbg_prepare(HAM_DEBUG_LEVEL_FATAL, __FILE__,       \
                    __LINE__, __FUNCTION__, #e);                              \
                hamsterdb::dbg_verify_failed(0);                              \
                hamsterdb::dbg_unlock();                                      \
              }

} // namespace hamsterdb

#endif /* HAM_ERROR_H__ */
