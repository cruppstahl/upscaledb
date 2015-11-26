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
 * UQI Plugin management functions
 *
 * @thread_safe: yes
 * @exception_safe: nothrow
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
