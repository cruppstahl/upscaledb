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
