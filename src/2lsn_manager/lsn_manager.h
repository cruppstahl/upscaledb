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

/*
 * Manager for the log sequence number (lsn)
 *
 * @exception_safe: nothrow
 * @thread_safe: no
 */
 
#ifndef UPS_LSN_MANAGER_H
#define UPS_LSN_MANAGER_H

#include "0root/root.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

struct LsnManager
{
  // Constructor
  LsnManager()
    : current(1) {
  }

  // Returns the next lsn
  uint64_t next() {
    return current++;
  }

  // the current lsn
  uint64_t current;
};

} // namespace upscaledb

#endif /* UPS_LSN_MANAGER_H */
