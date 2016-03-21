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

#include "0root/root.h"

#include "ups/upscaledb_uqi.h"

// Always verify that a file of level N does not include headers > N!
#include "1base/error.h"
#include "4env/env.h"
#include "4uqi/plugins.h"
#include "4uqi/result.h"
#include "4uqi/scanvisitor.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

using namespace upscaledb;

class Cursor;

UPS_EXPORT uint32_t UPS_CALLCONV
uqi_result_get_row_count(uqi_result_t *result)
{
  return ((Result *)result)->row_count;
}

UPS_EXPORT uint32_t UPS_CALLCONV
uqi_result_get_key_type(uqi_result_t *result)
{
  return ((Result *)result)->key_type;
}

UPS_EXPORT uint32_t UPS_CALLCONV
uqi_result_get_record_type(uqi_result_t *result)
{
  return ((Result *)result)->record_type;
}

UPS_EXPORT void UPS_CALLCONV
uqi_result_get_key(uqi_result_t *result, uint32_t row, ups_key_t *key)
{
  Result *r = (Result *)result;
  if (likely(row < r->row_count)) {
    r->key(row, key);
  }
  else {
    key->size = 0;
    key->data = 0;
  }
}

UPS_EXPORT void UPS_CALLCONV
uqi_result_get_record(uqi_result_t *result, uint32_t row, ups_record_t *record)
{
  Result *r = (Result *)result;
  if (likely(row < r->row_count)) {
    r->record(row, record);
  }
  else {
    record->size = 0;
    record->data = 0;
  }
}

UPS_EXPORT void *UPS_CALLCONV
uqi_result_get_key_data(uqi_result_t *result, uint32_t *psize)
{
  Result *r = (Result *)result;
  if (psize)
    *psize = r->key_data.size();
  return r->key_data.data();
}

UPS_EXPORT void *UPS_CALLCONV
uqi_result_get_record_data(uqi_result_t *result, uint32_t *psize)
{
  Result *r = (Result *)result;
  if (psize)
    *psize = r->record_data.size();
  return r->record_data.data();
}

UPS_EXPORT void UPS_CALLCONV
uqi_result_close(uqi_result_t *result)
{
  delete ((Result *)result);
}

UPS_EXPORT ups_status_t UPS_CALLCONV
uqi_register_plugin(uqi_plugin_t *descriptor)
{
  if (!descriptor) {
    ups_trace(("parameter 'descriptor' cannot be null"));
    return UPS_INV_PARAMETER;
  }

  return PluginManager::add(descriptor);
}

UPS_EXPORT ups_status_t UPS_CALLCONV
uqi_select(ups_env_t *env, const char *query, uqi_result_t **result)
{
  return uqi_select_range(env, query, 0, 0, result);
}

UPS_EXPORT ups_status_t UPS_CALLCONV
uqi_select_range(ups_env_t *henv, const char *query, ups_cursor_t *begin,
                    const ups_cursor_t *end, uqi_result_t **result)
{
  if (!henv) {
    ups_trace(("parameter 'env' cannot be null"));
    return UPS_INV_PARAMETER;
  }
  if (!query) {
    ups_trace(("parameter 'query' cannot be null"));
    return UPS_INV_PARAMETER;
  }
  if (!result) {
    ups_trace(("parameter 'result' cannot be null"));
    return UPS_INV_PARAMETER;
  }

  Env *env = (Env *)henv;
  ScopedLock lock(env->mutex);

  try {
    return env->select_range(query,
                        (upscaledb::Cursor *)begin,
                        (upscaledb::Cursor *)end,
                        (upscaledb::Result **)result);
  }
  catch (Exception &ex) {
    return ex.code;
  }
}

UPS_EXPORT void UPS_CALLCONV
uqi_result_initialize(uqi_result_t *result, int key_type, int record_type)
{
  Result *r = (Result *)result;
  r->initialize(key_type, record_type);
}

UPS_EXPORT void UPS_CALLCONV
uqi_result_add_row(uqi_result_t *result,
                    const void *key_data, uint32_t key_size,
                    const void *record_data, uint32_t record_size)
{
  Result *r = (Result *)result;
  r->add_row(key_data, key_size, record_data, record_size);
}

UPS_EXPORT void UPS_CALLCONV
uqi_result_move(uqi_result_t *destination, uqi_result_t *source)
{
  Result *d = (Result *)destination;
  Result *s = (Result *)source;
  d->move_from(*s);
}
