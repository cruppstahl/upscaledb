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

#include "globals.h"

namespace hamsterdb {

ham_u64_t Globals::ms_extended_keys;

ham_u64_t Globals::ms_extended_duptables;

ham_u64_t Globals::ms_bytes_before_compression;

ham_u64_t Globals::ms_bytes_after_compression;

ham_u32_t Globals::ms_extended_threshold;

ham_u32_t Globals::ms_duplicate_threshold;

int Globals::ms_linear_threshold;

int Globals::ms_error_level;

const char *Globals::ms_error_file;

int Globals::ms_error_line;

const char *Globals::ms_error_expr;

const char *Globals::ms_error_function;

// the default error handler
void HAM_CALLCONV default_errhandler(int level, const char *message);

ham_errhandler_fun Globals::ms_error_handler = default_errhandler;

bool Globals::ms_is_simd_enabled = true;

} // namespace hamsterdb

