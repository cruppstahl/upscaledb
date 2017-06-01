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

#ifndef UPS_BLOB_MANAGER_FACTORY_H
#define UPS_BLOB_MANAGER_FACTORY_H

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!
#include "3blob_manager/blob_manager_disk.h"
#include "3blob_manager/blob_manager_inmem.h"
#include "4env/env_local.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

struct BlobManagerFactory {
  // creates a new BlobManager instance depending on the flags
  static BlobManager *create(LocalEnv *env, uint32_t flags) {
    if (flags & UPS_IN_MEMORY)
      return (new InMemoryBlobManager(&env->config, env->page_manager.get(),
                              env->device.get()));
    else
      return (new DiskBlobManager(&env->config, env->page_manager.get(),
                              env->device.get()));
  }
};

} // namespace upscaledb

#endif /* UPS_BLOB_MANAGER_FACTORY_H */
