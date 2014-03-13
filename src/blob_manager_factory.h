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
