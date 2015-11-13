/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
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
 * parsed UQI statements
 *
 * @thread_safe: no
 * @exception_safe: nothrow
 */

#ifndef UPS_UPSCALEDB_STATEMENTS_H
#define UPS_UPSCALEDB_STATEMENTS_H

#include "0root/root.h"

#include <map> // for std::pair

#include "ups/upscaledb_uqi.h"

// Always verify that a file of level N does not include headers > N!

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

typedef std::pair<std::string, std::string> FunctionDesc;

struct SelectStatement {
  // constructor
  SelectStatement()
    : dbid(0), distinct(false), limit(0) {
  }

  // constructor - required by the parser
  SelectStatement(const std::string &foo)
    : dbid(0), distinct(false), limit(0) {
  }

  // the database id
  uint16_t dbid;

  // true if this is a distinct query (duplicates are ignored)
  bool distinct;

  // the limit - if 0 then unlimited
  int limit;

  // the actual query function (an aggregation plugin)
  FunctionDesc function;

  // an optional predicate function (for the WHERE clause)
  FunctionDesc predicate;
};

} // namespace upscaledb

#endif /* UPS_UPSCALEDB_STATEMENTS_H */
