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
