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
 * @exception_safe: nothrow
 * @thread_safe: no
 */

#ifndef HAM_DELTA_UPDATES_UNSORTED_H
#define HAM_DELTA_UPDATES_UNSORTED_H

#include "0root/root.h"

#include <vector>
#include <algorithm>

// Always verify that a file of level N does not include headers > N!
#include "3btree/btree_index_traits.h"
#include "3delta/delta_update.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

class DeltaAction;

//
// An unsorted vector of DeltaAction objects
//
struct UnsortedDeltaActions
{
  typedef std::vector<DeltaAction *>::iterator Iterator;

  // Inserts a DeltaAction into the sorted vector
  void append(DeltaAction *da) {
    m_vec.push_back(da);
  }

  // Returns a pointer to the first element of the vector
  Iterator begin() {
    return (m_vec.begin());
  }

  // Returns a pointer to the first element AFTER the vector
  Iterator end() {
    return (m_vec.end());
  }

  // The vector
  std::vector<DeltaAction *> m_vec;
};

} // namespace hamsterdb

#endif /* HAM_DELTA_UPDATES_UNSORTED_H */
