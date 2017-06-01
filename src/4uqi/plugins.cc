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

#include <string>
#include <map>
#include <vector>
#ifdef WIN32
#  include <windows.h>
#else
#  include <dlfcn.h>
#endif

#include "1base/error.h"
#include "1base/mutex.h"
#include "4uqi/plugins.h"

// Always verify that a file of level N does not include headers > N!

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

typedef std::map<std::string, uqi_plugin_t> PluginMap;

#ifdef WIN32
typedef HINSTANCE dll_instance_t;
#else
typedef void *dll_instance_t; 
#endif

static std::vector<dll_instance_t> handles;
static Mutex handle_mutex;
static Mutex mutex;
static PluginMap plugins;

void
PluginManager::cleanup()
{
  ScopedLock lock(handle_mutex);
  for (std::vector<dll_instance_t>::iterator it = handles.begin();
        it != handles.end(); it++) {
#ifdef WIN32
    ::FreeLibrary(*it);
#else
    ::dlclose(*it);
#endif
  }

  handles.clear();
}

ups_status_t
PluginManager::import(const char *library, const char *plugin_name)
{
#ifdef WIN32
#if defined(__MINGW32__)
  dll_instance_t dl = ::LoadLibrary(library);
#else
  wchar_t wlibrary[MAX_PATH];
  ::mbstowcs(wlibrary, library, ::strlen(library) + 1);
  dll_instance_t dl = ::LoadLibrary(wlibrary);
#endif
  if (!dl) {
    ups_log(("Failed to open library %s: %u", library, ::GetLastError()));
    return UPS_PLUGIN_NOT_FOUND;
  }
#else
  // clear reported errors
  ::dlerror();
  dll_instance_t dl = ::dlopen(library, RTLD_NOW);
  if (!dl) {
    ups_log(("Failed to open library %s: %s", library, ::dlerror()));
    return UPS_PLUGIN_NOT_FOUND;
  }
#endif

  // store the handle, otherwise we cannot clean it up later on
  {
    ScopedLock lock(handle_mutex);
    handles.push_back(dl);
  }

  uqi_plugin_export_function foo;
#ifdef WIN32
  foo = (uqi_plugin_export_function)::GetProcAddress(dl, "plugin_descriptor");
  if (!foo) {
    ups_log(("Failed to load exported symbol from library %s: %u",
      library, ::GetLastError()));
    return UPS_PLUGIN_NOT_FOUND;
  }
#else
  foo = (uqi_plugin_export_function)::dlsym(dl, "plugin_descriptor");
  if (!foo) {
    ups_log(("Failed to load exported symbol from library %s: %s",
      library, ::dlerror()));
    return UPS_PLUGIN_NOT_FOUND;
  }
#endif

  uqi_plugin_t *plugin = foo(plugin_name);
  if (!plugin) {
    ups_log(("Failed to load plugin %s from library %s", plugin_name, library));
    return UPS_PLUGIN_NOT_FOUND;
  }

  return add(plugin);
}

ups_status_t
PluginManager::add(uqi_plugin_t *plugin)
{
  if (plugin->plugin_version != 0) {
    ups_log(("Failed to load plugin %s: invalid version (%d != %d)",
            plugin->name, 0, plugin->plugin_version));
    return UPS_PLUGIN_NOT_FOUND;
  }

  switch (plugin->type) {
    case UQI_PLUGIN_PREDICATE:
      if (!plugin->pred) {
        ups_log(("Failed to load predicate plugin %s: 'pred' function pointer "
                "must not be null", plugin->name));
        return UPS_PLUGIN_NOT_FOUND;
      }
      break;
    case UQI_PLUGIN_AGGREGATE:
      if (!plugin->agg_single) {
        ups_log(("Failed to load aggregate plugin %s: 'agg_single' function "
                "pointer must not be null", plugin->name));
        return UPS_PLUGIN_NOT_FOUND;
      }
      if (!plugin->agg_many) {
        ups_log(("Failed to load aggregate plugin %s: 'agg_many' function "
                "pointer must not be null", plugin->name));
        return UPS_PLUGIN_NOT_FOUND;
      }
      break;
    default:
      ups_log(("Failed to load plugin %s: unknown type %d",
              plugin->name, plugin->type));
      return UPS_PLUGIN_NOT_FOUND;
  }

  ScopedLock lock(mutex);
  plugins.insert(PluginMap::value_type(plugin->name, *plugin));
  return 0;
}

bool
PluginManager::is_registered(const char *plugin_name)
{
  return get(plugin_name) != 0;
}

uqi_plugin_t *
PluginManager::get(const char *plugin_name)
{
  ScopedLock lock(mutex);
  PluginMap::iterator it = plugins.find(plugin_name);
  if (it == plugins.end())
    return 0;
  return &it->second;
}

uqi_plugin_t
PluginManager::aggregate(const char *name,
                            uqi_plugin_init_function init,
                            uqi_plugin_aggregate_single_function agg_single,
                            uqi_plugin_aggregate_many_function agg_many,
                            uqi_plugin_result_function results)
{
  uqi_plugin_t plugin = {0};
  plugin.name = name;
  plugin.type = UQI_PLUGIN_AGGREGATE;
  plugin.init = init;
  plugin.agg_single = agg_single;
  plugin.agg_many = agg_many;
  plugin.results = results;
  return plugin;
}

uqi_plugin_t
PluginManager::predicate(const char *name,
                            uqi_plugin_init_function init,
                            uqi_plugin_predicate_function pred,
                            uqi_plugin_result_function results)
{
  uqi_plugin_t plugin = {0};
  plugin.name = name;
  plugin.type = UQI_PLUGIN_PREDICATE;
  plugin.init = init;
  plugin.pred = pred;
  plugin.results = results;
  return plugin;
}

} // namespace upscaledb
