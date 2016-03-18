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
 * TODO
 * Add brief documentation about this file
 */

#ifndef UPS_TEMPLATE_H // TODO
#define UPS_TEMPLATE_H // TODO

#include "0root/root.h"

// TODO include c/c++ standard libraries
#include <ups/types.h>

// TODO include 3rd party headers
#include <json/json.h>

// TODO include public upscaledb headers
#include "ups/upscaledb.h"

// TODO include local headers, ordered by tier
// Always verify that a file of level N does not include headers > N!

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

struct Journal;
struct LocalEnv;

// Prefer struct over class
struct LocalEnvTest {
  LocalEnvTest(LocalEnv *env_)
    : env(env_), _journal(0) {
  }

  // Sets a new journal object
  void set_journal(Journal *journal);

  // NO leading underscore: public member. getters/setters are not
  // required
  LocalEnv *env;

  // leading underscore: "private" member
  Journal *_journal;
};

} // namespace upscaledb

#endif // UPS_TEMPLATE_H // TODO
