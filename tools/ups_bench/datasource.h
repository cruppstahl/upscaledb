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

#ifndef UPS_BENCH_DATASOURCE_H
#define UPS_BENCH_DATASOURCE_H

#include <vector>

//
// abstract base class for a data source - generates test data
//
struct Datasource
{
  // virtual destructor - can be overwritten
  virtual ~Datasource() {
  }

  // resets the input and restarts delivering the same sequence
  // from scratch
  virtual void reset() = 0;

  // returns the next piece of data
  virtual void next(std::vector<uint8_t> &vec) = 0;
};

#endif /* UPS_BENCH_DATASOURCE_H */
