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
 * UQI Plugin management functions
 */

#ifndef UPS_UPSCALEDB_PLUGINS_H
#define UPS_UPSCALEDB_PLUGINS_H

#include "0root/root.h"

#include "ups/upscaledb_uqi.h"

// Always verify that a file of level N does not include headers > N!

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

/*
 * The struct PluginManager provides a common namespace for all
 * plugin-related activities
 */
struct PluginManager
{
  /* Closes all handles - used to prevent valgrind errors */
  static void cleanup();

  /* Imports a plugin from an external library */
  static ups_status_t import(const char *library, const char *plugin_name);

  /* Adds a new plugin to the system */
  static ups_status_t add(uqi_plugin_t *plugin);

  /* Returns true if a plugin with this name is registered */
  static bool is_registered(const char *plugin_name);

  /* Returns a plugin descriptor, or NULL */
  static uqi_plugin_t *get(const char *plugin_name);

  /* A helper to generate an "aggregate" plugin */ 
  static uqi_plugin_t aggregate(const char *name,
                            uqi_plugin_init_function init,
                            uqi_plugin_aggregate_single_function agg_single,
                            uqi_plugin_aggregate_many_function agg_many,
                            uqi_plugin_result_function results);

  /* A helper to generate a "predicate" plugin */ 
  static uqi_plugin_t predicate(const char *name,
                            uqi_plugin_init_function init,
                            uqi_plugin_predicate_function pred,
                            uqi_plugin_result_function results);
};

/* Typedef for the exported function in a plugin DLL */
typedef uqi_plugin_t *(*uqi_plugin_export_function)(const char *name);

} // namespace upscaledb

#endif /* UPS_UPSCALEDB_PLUGINS_H */
