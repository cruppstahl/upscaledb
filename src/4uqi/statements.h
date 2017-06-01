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
 * parsed UQI statements
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

struct FunctionDesc{
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
