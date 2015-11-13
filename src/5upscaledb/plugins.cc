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

#include "0root/root.h"

#include <string>
#include <map>
#include <dlfcn.h>

#include "1base/error.h"
#include "1base/mutex.h"
#include "5upscaledb/plugins.h"

// Always verify that a file of level N does not include headers > N!

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

typedef std::map<std::string, uqi_plugin_t> PluginMap;
static Mutex mutex;
static PluginMap plugins;

ups_status_t
PluginManager::import(const char *library, const char *plugin_name)
{
  // clear reported errors
  dlerror();

  // the |dl| handle is leaked deliberately
  void *dl = ::dlopen(library, RTLD_NOW);
  if (!dl) {
    ups_log(("Failed to open library %s: %s", library, dlerror()));
    return (UPS_PLUGIN_NOT_FOUND);
  }

  uqi_plugin_export_function foo;
  foo = (uqi_plugin_export_function)::dlsym(dl,"plugin_descriptor");
  if (!foo) {
    ups_log(("Failed to load exported symbol from library %s: %s",
                library, dlerror()));
    return (UPS_PLUGIN_NOT_FOUND);
  }

  uqi_plugin_t *plugin = foo(plugin_name);
  if (!plugin) {
    ups_log(("Failed to load plugin %s from library %s", plugin_name, library));
    return (UPS_PLUGIN_NOT_FOUND);
  }

  return (add(plugin));
}

ups_status_t
PluginManager::add(uqi_plugin_t *plugin)
{
  ScopedLock lock(mutex);
  plugins.insert(PluginMap::value_type(plugin->name, *plugin));
  return (0);
}

bool
PluginManager::is_registered(const char *plugin_name)
{
  return (get(plugin_name) != 0);
}

uqi_plugin_t *
PluginManager::get(const char *plugin_name)
{
  ScopedLock lock(mutex);
  PluginMap::iterator it = plugins.find(plugin_name);
  if (it == plugins.end())
    return (0);
  return (&it->second);
}

} // namespace upscaledb
