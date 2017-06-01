/*
 * Copyright (C) 2005-2017 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * See the file COPYING for License information.
 */

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!
#include "1globals/globals.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

uint64_t Globals::ms_extended_keys;

uint64_t Globals::ms_extended_duptables;

uint32_t Globals::ms_extended_threshold;

uint32_t Globals::ms_duplicate_threshold;

int Globals::ms_linear_threshold;

int Globals::ms_error_level;

const char *Globals::ms_error_file;

int Globals::ms_error_line;

const char *Globals::ms_error_expr;

const char *Globals::ms_error_function;

// the default error handler
void UPS_CALLCONV default_errhandler(int level, const char *message);

ups_error_handler_fun Globals::ms_error_handler = default_errhandler;

uint64_t Globals::ms_bytes_before_compression;

uint64_t Globals::ms_bytes_after_compression;

bool Globals::ms_is_simd_enabled = true;

uint64_t Globals::ms_btree_smo_split;

uint64_t Globals::ms_btree_smo_merge;

uint64_t Globals::ms_btree_smo_shift;

int Globals::ms_flush_threshold = 10;

} // namespace upscaledb

