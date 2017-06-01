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

#ifndef UPS_UQI_SCAN_VISITOR_H
#define UPS_UQI_SCAN_VISITOR_H

#include "0root/root.h"

#include "ups/upscaledb_uqi.h"

#include "2config/db_config.h"
#include "4uqi/statements.h"

// Always verify that a file of level N does not include headers > N!

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

//
// The ScanVisitor is the callback implementation for the scan call.
// It will either receive single keys or multiple keys in an array.
//
struct ScanVisitor
{
  enum {
    // accepts binary AND numeric input
    kOnlyNumericInput = 0,

    // by default, both streams are required as input
    kRequiresBothStreams = 1,
  };

  // Constructor
  ScanVisitor(SelectStatement *stmt = 0)
    : statement(stmt) {
  }

  // Operates on a single key/value pair
  virtual void operator()(const void *key_data, uint16_t key_size, 
                  const void *record_data, uint32_t record_size) = 0;

  // Operates on an array of keys and/or records
  virtual void operator()(const void *key_array, const void *record_array,
                  size_t key_count) = 0;

  // Assigns the internal result to |result|
  virtual void assign_result(uqi_result_t *result) = 0;

  // The select statement
  SelectStatement *statement;
};

//
// A ScanVisitor accepting only numerical input
//
struct NumericalScanVisitor : public ScanVisitor
{
  enum {
    // accepts numeric input only
    kOnlyNumericInput = 1,
  };

  NumericalScanVisitor(SelectStatement *stmt = 0)
    : ScanVisitor(stmt) {
  }
};

} // namespace upscaledb

#endif /* UPS_UQI_SCAN_VISITOR_H */
