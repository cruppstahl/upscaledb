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
 * UQI builtin plugins
 */

#ifndef UPS_UPSCALEDB_SCANVISITORFACTORY_H
#define UPS_UPSCALEDB_SCANVISITORFACTORY_H

#include "0root/root.h"

#include "ups/upscaledb_uqi.h"

// Always verify that a file of level N does not include headers > N!

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

struct ScanVisitor;
struct SelectStatement;
class LocalDatabase;

struct ScanVisitorFactory
{
  /*
   * Creates a ScanVisitor instance for a SelectStatement.
   * Returns 0 in case of an error (i.e. if the plugin was not found)
   */
  static ScanVisitor *from_select(SelectStatement *stmt, LocalDatabase *db);
};

} // namespace upscaledb

#endif /* UPS_UPSCALEDB_SCANVISITORFACTORY_H */
