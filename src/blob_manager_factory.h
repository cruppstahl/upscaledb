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
 

#ifndef HAM_BLOB_MANAGER_FACTORY_H__
#define HAM_BLOB_MANAGER_FACTORY_H__

#include <ham/types.h>
#include "blob_manager_disk.h"
#include "blob_manager_inmem.h"

namespace hamsterdb {

class BlobManagerFactory {
  public:
    // creates a new BlobManager instance depending on the flags
    static BlobManager *create(LocalEnvironment *env, ham_u32_t flags) {
      if (flags & HAM_IN_MEMORY)
        return (new InMemoryBlobManager(env));
      else
        return (new DiskBlobManager(env));
    }
};

} // namespace hamsterdb

#endif /* HAM_BLOB_MANAGER_FACTORY_H__ */
