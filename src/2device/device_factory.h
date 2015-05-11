/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
 * All Rights Reserved.
 *
 * NOTICE: All information contained herein is, and remains the property
 * of Christoph Rupp and his suppliers, if any. The intellectual and
 * technical concepts contained herein are proprietary to Christoph Rupp
 * and his suppliers and may be covered by Patents, patents in process,
 * and are protected by trade secret or copyright law. Dissemination of
 * this information or reproduction of this material is strictly forbidden
 * unless prior written permission is obtained from Christoph Rupp.
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
