/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
 * All Rights Reserved.
 *
 * NOTICE: All information contained herein is, and remains the property
 * of Christoph Rupp and his suppliers, if any. The intellectual and
 * technical concepts contained herein are proprietary to Christoph Rupp
 * and his suppliers and may be covered by Patents, patents in process,
 * and are protected by trade secret or copyright law. Dissemination of
 * this information or reproduction of this material is strictly forbidden
 * unless prior written permission is obtained from Christoph Rupp.
 */

/*
 * Global variables; used for tests and metrics
 *
 * @exception_safe: nothrow
 * @thread_safe: no
 */
 
#ifndef HAM_GLOBALS_H
#define HAM_GLOBALS_H

#include "0root/root.h"

#include "ham/types.h"

// Always verify that a file of level N does not include headers > N!
#include "1base/error.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

struct Globals {
  // for counting extended keys
  static uint64_t ms_extended_keys;

  // for counting extended duplicate tables
  static uint64_t ms_extended_duptables;

  // Move every key > threshold to a blob. For testing purposes.
  // Value is not persisted.
  static uint32_t ms_extended_threshold;

  // Create duplicate table if amount of duplicates > threshold. For testing
  // purposes.
  // Value is not persisted.
  static uint32_t ms_duplicate_threshold;

  // linear search threshold for the PAX layout
  static int ms_linear_threshold;

  // used in error.h/error.cc
  static int ms_error_level;

  // used in error.h/error.cc
  static const char *ms_error_file;

  // used in error.h/error.cc
  static int ms_error_line;

  // used in error.h/error.cc
  static const char *ms_error_expr;

  // used in error.h/error.cc
  static const char *ms_error_function;

  // used in error.h/error.cc
  static ham_errhandler_fun ms_error_handler;

  // PRO: Tracking key bytes before compression
  static uint64_t ms_bytes_before_compression;

  // PRO: Tracking key bytes after compression
  static uint64_t ms_bytes_after_compression;
};

} // namespace hamsterdb

#endif /* HAM_GLOBALS_H */
