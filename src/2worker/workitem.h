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
 * A work item for the worker thread.
 */

#ifndef UPS_WORK_ITEM_H
#define UPS_WORK_ITEM_H

#include "0root/root.h"

#include <ups/types.h>

// Always verify that a file of level N does not include headers > N!
#include "1base/spinlock.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

//
// The WorkItem. Other messages can derive from it and append their own
// payload.
//
struct WorkItem {
  // Message flags
  enum {
    // The message is blocking
    kIsBlocking  = 1,

    // Do NOT delete the message after it was processed
    kDontDelete  = 2,
  };

  WorkItem(int flags_ = 0)
    : flags(flags_) {
  }

  virtual ~WorkItem() {
  }

  int flags;
};

} // namespace upscaledb

#endif // UPS_WORK_ITEM_H

