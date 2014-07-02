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

#include "globals.h"

namespace hamsterdb {

ham_u64_t Globals::ms_extended_keys;

ham_u64_t Globals::ms_extended_duptables;

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

ham_u64_t Globals::ms_bytes_before_compression;

ham_u64_t Globals::ms_bytes_after_compression;

bool Globals::ms_is_simd_enabled = true;

} // namespace hamsterdb

