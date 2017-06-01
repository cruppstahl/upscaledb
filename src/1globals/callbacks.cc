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

#include <boost/algorithm/string.hpp> 
#include <string>
#include <map>

#include "3rdparty/murmurhash3/MurmurHash3.h"

#include "1globals/callbacks.h"
#include "1base/mutex.h"

// Always verify that a file of level N does not include headers > N!

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

typedef std::map<uint32_t, ups_compare_func_t> CallbackMap;
static Mutex mutex;
static CallbackMap callbacks;

uint32_t
CallbackManager::hash(std::string name)
{
  boost::algorithm::to_lower(name);
  uint32_t h = 0;
  MurmurHash3_x86_32(name.data(), (int)name.size(), 0, &h);
  return h;
}

void
CallbackManager::add(const char *zname, ups_compare_func_t func)
{
  uint32_t h = hash(zname);

  ScopedLock lock(mutex);
  callbacks.insert(CallbackMap::value_type(h, func));
}

bool
CallbackManager::is_registered(const char *zname)
{
  return get(zname) != 0;
}

ups_compare_func_t
CallbackManager::get(const char *zname)
{
  uint32_t h = hash(zname);
  return get(h);
}

ups_compare_func_t
CallbackManager::get(uint32_t h)
{
  ScopedLock lock(mutex);
  CallbackMap::iterator it = callbacks.find(h);
  if (it == callbacks.end())
    return 0;
  return it->second;
}

} // namespace upscaledb
