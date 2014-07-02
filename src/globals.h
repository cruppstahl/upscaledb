/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
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
 * global variables; used for tests and metrics
 *
 */
 
#ifndef HAM_GLOBALS_H__
#define HAM_GLOBALS_H__

#include <ham/hamsterdb.h>

#include "config.h"

namespace hamsterdb {

struct Globals {
  // for counting extended keys
  static ham_u64_t ms_extended_keys;

  // for counting extended duplicate tables
  static ham_u64_t ms_extended_duptables;

  // Move every key > threshold to a blob. For testing purposes.
  // TODO currently gets assigned at runtime
  static ham_u32_t ms_extended_threshold;

  // Create duplicate table if amount of duplicates > threshold. For testing
  // purposes.
  // TODO currently gets assigned at runtime
  static ham_u32_t ms_duplicate_threshold;

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
  static ham_u64_t ms_bytes_before_compression;

  // PRO: Tracking key bytes after compression
  static ham_u64_t ms_bytes_after_compression;

  // PRO: enable/disable SIMD
  static bool ms_is_simd_enabled;
};

} // namespace hamsterdb

#endif /* HAM_GLOBALS_H__ */
