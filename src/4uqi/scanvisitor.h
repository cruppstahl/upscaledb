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
struct ScanVisitor {
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
                  const void *record_data, uint32_t record_size,
                  size_t duplicate_count) = 0;

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
struct NumericalScanVisitor : public ScanVisitor {
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
