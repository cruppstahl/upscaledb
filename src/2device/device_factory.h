/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
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

/*
 * A factory for Device objects
 *
 * @exception_safe: strong
 * @thread_safe: yes
 */

#ifndef HAM_DEVICE_FACTORY_H
#define HAM_DEVICE_FACTORY_H

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!
#include "2config/env_config.h"
#include "2device/device_disk.h"
#include "2device/device_inmem.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

struct DeviceFactory {
  // creates a new Device instance depending on the flags
  static Device *create(const EnvironmentConfiguration &config) {
    if (config.flags & HAM_IN_MEMORY)
      return (new InMemoryDevice(config));
    else
      return (new DiskDevice(config));
  }
};

} // namespace hamsterdb

#endif /* HAM_DEVICE_FACTORY_H */
