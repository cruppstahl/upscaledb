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
