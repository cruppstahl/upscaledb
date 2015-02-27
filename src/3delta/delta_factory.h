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
 * A factory to create DeltaUpdate objects.
 *
 * @exception_safe: strong
 * @thread_safe: yes
 */

#ifndef HAM_DELTA_FACTORY_H
#define HAM_DELTA_FACTORY_H

#include "0root/root.h"

#include "ham/types.h"

// Always verify that a file of level N does not include headers > N!
#include "1mem/mem.h"
#include "3delta/delta_update.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

struct DeltaUpdateFactory
{
  // Creates a new DeltaUpdate
  static DeltaUpdate *create_delta_update(LocalDatabase *db, ham_key_t *key) {
    DeltaUpdate *du;
    du = Memory::allocate<DeltaUpdate>(sizeof(*du)
                                            + (key ? key->size : 0));
    du->initialize(db, key);
    return (du);
  }

  // Destroys a DeltaUpdate
  static void destroy_delta_update(DeltaUpdate *du) {
    Memory::release(du);
  }

  // Creates a new DeltaAction
  static DeltaAction *create_delta_action(DeltaUpdate *update,
                    uint64_t txn_id, uint64_t lsn,
                    uint32_t flags, uint32_t original_flags,
                    int referenced_duplicate, ham_record_t *record) {
    DeltaAction *da;
    da = Memory::allocate<DeltaAction>(sizeof(*da)
                                            + (record ? record->size : 0));
    da->initialize(update, txn_id, lsn, flags, original_flags,
            referenced_duplicate, record);
    return (da);
  }

  // Destroys a DeltaAction
  static void destroy_delta_action(DeltaAction *da) {
    Memory::release(da);
  }
};

} // namespace hamsterdb

#endif /* HAM_DELTA_FACTORY_H */
