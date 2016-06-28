/*
 * Copyright (C) 2005-2016 Christoph Rupp (chris@crupp.de).
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

