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
 * A factory for Device objects
 *
 * @exception_safe: strong
 * @thread_safe: yes
 */

#ifndef UPS_DEVICE_FACTORY_H
#define UPS_DEVICE_FACTORY_H

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!
#include "2config/env_config.h"
#include "2device/device_disk.h"
#include "2device/device_inmem.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

struct DeviceFactory {
  // creates a new Device instance depending on the flags
  static Device *create(const EnvironmentConfiguration &config) {
    if (config.flags & UPS_IN_MEMORY)
      return (new InMemoryDevice(config));
    else
      return (new DiskDevice(config));
  }
};

} // namespace upscaledb

#endif /* UPS_DEVICE_FACTORY_H */
