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

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!
#include "1errorinducer/errorinducer.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

struct State {
  State()
    : loops(0), error(UPS_INTERNAL_ERROR) {
  }

  int loops;
  ups_status_t error;
};

static State state[ErrorInducer::kMaxActions];
static bool is_active_ = false;

void
ErrorInducer::activate(bool active)
{
  is_active_ = active;
}

bool
ErrorInducer::is_active()
{
  return is_active_;
}

void
ErrorInducer::add(Action action, int loops, ups_status_t error)
{
  state[action].loops = loops;
  state[action].error = error;
}

ups_status_t
ErrorInducer::induce(Action action)
{
  assert(is_active() == true);
  assert(state[action].loops >= 0);

  if (state[action].loops > 0 && --state[action].loops == 0)
    return state[action].error;
  return 0;
}

} // namespace upscaledb
