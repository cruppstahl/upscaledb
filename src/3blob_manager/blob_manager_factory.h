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

#ifndef HAM_BLOB_MANAGER_FACTORY_H
#define HAM_BLOB_MANAGER_FACTORY_H

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!
#include "3blob_manager/blob_manager_disk.h"
#include "3blob_manager/blob_manager_inmem.h"
#include "4env/env_local.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

struct BlobManagerFactory {
  // creates a new BlobManager instance depending on the flags
  static BlobManager *create(LocalEnvironment *env, uint32_t flags) {
    if (flags & HAM_IN_MEMORY)
      return (new InMemoryBlobManager(&env->config(), env->page_manager(),
                              env->device()));
    else
      return (new DiskBlobManager(&env->config(), env->page_manager(),
                              env->device()));
  }
};

} // namespace hamsterdb

#endif /* HAM_BLOB_MANAGER_FACTORY_H */
