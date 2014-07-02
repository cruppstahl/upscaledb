/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
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

#ifndef HAM_DEVICE_FACTORY_H__
#define HAM_DEVICE_FACTORY_H__

#include <ham/types.h>
#include "device_disk.h"
#include "device_inmem.h"

namespace hamsterdb {

class DeviceFactory {
  public:
    // creates a new Device instance depending on the flags
    static Device *create(LocalEnvironment *env, ham_u32_t flags,
                    ham_u64_t file_size_limit) {
      if (flags & HAM_IN_MEMORY)
        return (new InMemoryDevice(env, flags, file_size_limit));
      else
        return (new DiskDevice(env, flags, file_size_limit));
    }
};

} // namespace hamsterdb

#endif /* HAM_DEVICE_FACTORY_H__ */
