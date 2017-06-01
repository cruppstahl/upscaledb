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
struct LocalDb;

struct ScanVisitorFactory
{
  /*
   * Creates a ScanVisitor instance for a SelectStatement.
   * Returns 0 in case of an error (i.e. if the plugin was not found)
   */
  static ScanVisitor *from_select(SelectStatement *stmt, LocalDb *db);
};

} // namespace upscaledb

#endif /* UPS_UPSCALEDB_SCANVISITORFACTORY_H */
