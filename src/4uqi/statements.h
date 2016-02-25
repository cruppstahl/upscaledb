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
 * parsed UQI statements
 *
 * @thread_safe: no
 * @exception_safe: nothrow
 */

#ifndef UPS_UPSCALEDB_STATEMENTS_H
#define UPS_UPSCALEDB_STATEMENTS_H

#include "0root/root.h"

#include <string>

#include "ups/upscaledb_uqi.h"

// Always verify that a file of level N does not include headers > N!

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

struct FunctionDesc {
  FunctionDesc()
    : flags(0) {
  }

  uint32_t flags;    // UQI_INPUT_KEY, UQI_INPUT_RECORD
  std::string name;
  std::string library;
};

struct SelectStatement {
  // constructor
  SelectStatement()
    : dbid(0), distinct(false), limit(0), function_plg(0), predicate_plg(0),
      requires_keys(true), requires_records(true) {
  }

  // constructor - required by the parser
  SelectStatement(const std::string &foo)
    : dbid(0), distinct(false), limit(0), function_plg(0), predicate_plg(0),
      requires_keys(true), requires_records(true) {
  }

  // the database id
  uint16_t dbid;

  // true if this is a distinct query (duplicates are ignored)
  bool distinct;

  // the limit - if 0 then unlimited
  int limit;

  // the actual query function (an aggregation plugin)
  FunctionDesc function;

  // the resolved function plugin
  uqi_plugin_t *function_plg;

  // an optional predicate function (for the WHERE clause)
  FunctionDesc predicate;

  // the resolved predicate plugin
  uqi_plugin_t *predicate_plg;

  // internal flag for the Btree scan
  bool requires_keys;

  // internal flag for the Btree scan
  bool requires_records;
};

} // namespace upscaledb

#endif /* UPS_UPSCALEDB_STATEMENTS_H */
