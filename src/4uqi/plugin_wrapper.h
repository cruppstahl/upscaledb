/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
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
 * RAII wrapper for plugins
 */

#ifndef UPS_UPSCALEDB_PLUGIN_WRAPPER_H
#define UPS_UPSCALEDB_PLUGIN_WRAPPER_H

#include "0root/root.h"

#include "ups/upscaledb_uqi.h"

// Always verify that a file of level N does not include headers > N!

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

struct DbConfig;
struct SelectStatement;

struct PluginWrapper {
  PluginWrapper(const DbConfig *cfg, SelectStatement *stmt)
    : plugin(stmt->predicate_plg), state(0) {
    if (plugin->init)
      state = plugin->init(stmt->predicate.flags, cfg->key_type,
                            cfg->key_size, cfg->record_type,
                            cfg->record_size, 0);
  }

  // clean up the plugin's state
  ~PluginWrapper() {
    if (plugin->cleanup) {
      plugin->cleanup(state);
      state = 0;
    }
  }

  bool pred(const void *key_data, uint32_t key_size,
                  const void *record_data, uint32_t record_size) {
    return plugin->pred(state, key_data, key_size, record_data, record_size);
  }

  // The predicate plugin
  uqi_plugin_t *plugin;

  // The (optional) plugin's state
  void *state;
};

} // namespace upscaledb

#endif /* UPS_UPSCALEDB_PLUGIN_WRAPPER_H */
