/*
 * Copyright (C) 2005-2013 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
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
