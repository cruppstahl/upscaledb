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

struct PluginWrapperBase
{
  PluginWrapperBase(const DbConfig *cfg, uqi_plugin_t *p, uint32_t init_flags)
    : plugin(p), state(0) {
    if (plugin->init)
      state = plugin->init(init_flags, cfg->key_type, cfg->key_size,
                      cfg->record_type, cfg->record_size, 0);
  }

  // clean up the plugin's state
  ~PluginWrapperBase() {
    if (plugin->cleanup) {
      plugin->cleanup(state);
      state = 0;
    }
  }

  // The predicate plugin
  uqi_plugin_t *plugin;

  // The (optional) plugin's state
  void *state;
};

struct PredicatePluginWrapper : PluginWrapperBase
{
  PredicatePluginWrapper(const DbConfig *cfg, SelectStatement *stmt)
    : PluginWrapperBase(cfg, stmt->predicate_plg, stmt->predicate.flags) {
  }

  bool pred(const void *key_data, uint32_t key_size,
                  const void *record_data, uint32_t record_size) {
    return plugin->pred(state, key_data, key_size, record_data, record_size);
  }
};

struct AggregatePluginWrapper : PluginWrapperBase
{
  AggregatePluginWrapper(const DbConfig *cfg, SelectStatement *stmt)
    : PluginWrapperBase(cfg, stmt->function_plg, stmt->function.flags) {
  }

  // Aggregation function for a single key/record pair
  void agg_single(const void *key_data, uint16_t key_size, 
                  const void *record_data, uint32_t record_size) {
    plugin->agg_single(state, key_data, key_size, record_data, record_size);
  }

  // Aggregation function for a sequence of keys and records
  void agg_many(const void *key_data, const void *record_data, size_t length) {
    plugin->agg_many(state, key_data, record_data, length);
  }

  // Assigns and collects the results of the query
  void assign_result(uqi_result_t *result) {
    plugin->results(state, result);
  }
};

} // namespace upscaledb

#endif /* UPS_UPSCALEDB_PLUGIN_WRAPPER_H */
