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
#include "3btree/btree_visitor.h"
#include "4env/env.h"
#include "4uqi/plugins.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

using namespace upscaledb;

class Cursor;

UPS_EXPORT ups_status_t UPS_CALLCONV
uqi_register_plugin(uqi_plugin_t *descriptor)
{
  if (!descriptor) {
    ups_trace(("parameter 'descriptor' cannot be null"));
    return (UPS_INV_PARAMETER);
  }

  return (PluginManager::add(descriptor));
}

UPS_EXPORT ups_status_t UPS_CALLCONV
uqi_select(ups_env_t *env, const char *query, uqi_result_t *result)
{
  return (uqi_select_range(env, query, 0, 0, result));
}

UPS_EXPORT ups_status_t UPS_CALLCONV
uqi_select_range(ups_env_t *henv, const char *query, ups_cursor_t **begin,
                    const ups_cursor_t *end, uqi_result_t *result)
{
  if (!henv) {
    ups_trace(("parameter 'env' cannot be null"));
    return (UPS_INV_PARAMETER);
  }
  if (!query) {
    ups_trace(("parameter 'query' cannot be null"));
    return (UPS_INV_PARAMETER);
  }
  if (!result) {
    ups_trace(("parameter 'result' cannot be null"));
    return (UPS_INV_PARAMETER);
  }

  Environment *env = (Environment *)henv;
  ScopedLock lock(env->mutex());

  return (env->select_range(query, (upscaledb::Cursor **)begin,
                        (upscaledb::Cursor *)end, result));
}
