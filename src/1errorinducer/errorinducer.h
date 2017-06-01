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
 * Facility to simulate errors.
 *
 * The ErrorInducer is a static object. Its state is shared between all threads
 * and all upscaledb Environments!
 */

#ifndef UPS_ERRORINDUCER_H
#define UPS_ERRORINDUCER_H

#include "0root/root.h"

#include <string.h>

#include "ups/upscaledb.h"

// Always verify that a file of level N does not include headers > N!
#include "1base/error.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

// a macro to invoke errors
#define UPS_INDUCE_ERROR(id)                                        \
  while (unlikely(ErrorInducer::is_active())) {                     \
    ups_status_t st = ErrorInducer::induce(id);                     \
    if (st)                                                         \
      throw Exception(st);                                          \
    break;                                                          \
  }

namespace upscaledb {

struct ErrorInducer {
  enum Action {
    // simulates a failure in Changeset::flush
    kChangesetFlush,

    // simulates a hang in hamserver-connect
    kServerConnect,

    // let mmap fail
    kFileMmap,

    kMaxActions
  };

  // Activates or deactivates the error inducer
  static void activate(bool active);

  // Returns true if the error inducer is active
  static bool is_active();

  // Adds a "planned failure" to the error inducer
  static void add(Action action, int loops,
                  ups_status_t error = UPS_INTERNAL_ERROR);

  // Decrements the counter of the specified failure; returns failure when
  // counter is zero
  static ups_status_t induce(Action action);
};

} // namespace upscaledb

#endif // UPS_ERRORINDUCER_H
