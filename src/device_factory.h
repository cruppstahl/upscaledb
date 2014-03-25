/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef HAM_DEVICE_FACTORY_H__
#define HAM_DEVICE_FACTORY_H__

#include <ham/types.h>
#include "device_disk.h"
#include "device_inmem.h"

namespace hamsterdb {

class DeviceFactory {
  public:
    // creates a new Device instance depending on the flags
    static Device *create(LocalEnvironment *env, ham_u32_t flags) {
      if (flags & HAM_IN_MEMORY)
        return (new InMemoryDevice(env, flags));
      else
        return (new DiskDevice(env, flags));
    }
};

} // namespace hamsterdb

#endif /* HAM_DEVICE_FACTORY_H__ */
