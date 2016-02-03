/*
 * Copyright (C) 2005-2016 Christoph Rupp (chris@crupp.de).
 * All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * See the file COPYING for License information.
 */

/*
 * Global variables; used for tests and metrics
 *
 * @exception_safe: nothrow
 * @thread_safe: no
 */
 
#ifndef UPS_GLOBALS_H
#define UPS_GLOBALS_H

#include "0root/root.h"

#include "ups/types.h"

// Always verify that a file of level N does not include headers > N!
#include "1base/error.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

struct Globals {
  // for counting extended keys
  static uint64_t ms_extended_keys;

  // for counting extended duplicate tables
  static uint64_t ms_extended_duptables;

  // Move every key > threshold to a blob. For testing purposes.
  // TODO currently gets assigned at runtime
  static uint32_t ms_extended_threshold;

  // Create duplicate table if amount of duplicates > threshold. For testing
  // purposes.
  // TODO currently gets assigned at runtime
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
  static ups_error_handler_fun ms_error_handler;

  // Tracking key bytes before compression
  static uint64_t ms_bytes_before_compression;

  // Tracking key bytes after compression
  static uint64_t ms_bytes_after_compression;

  // enable/disable SIMD
  static bool ms_is_simd_enabled;
};

} // namespace upscaledb

#endif /* UPS_GLOBALS_H */
