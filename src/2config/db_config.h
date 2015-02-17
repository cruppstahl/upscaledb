/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * The configuration settings of a Database.
 *
 * @exception_safe nothrow
 * @thread_safe no
 */

#ifndef HAM_DB_CONFIG_H
#define HAM_DB_CONFIG_H

#include "0root/root.h"

#include <ham/types.h>

// Always verify that a file of level N does not include headers > N!

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

struct DatabaseConfiguration
{
  // Constructor initializes with default values
  DatabaseConfiguration()
    : db_name(0), flags(0), key_type(HAM_TYPE_BINARY),
      key_size(HAM_KEY_SIZE_UNLIMITED), record_size(HAM_RECORD_SIZE_UNLIMITED),
      key_compressor(0), record_compressor(0) {
  }

  // the database name
  uint16_t db_name;

  // the database flags
  uint32_t flags;

  // the key type
  int key_type;

  // the key size (if specified)
  size_t key_size;

  // the record size (if specified)
  size_t record_size;

  // the algorithm for key compression
  int key_compressor;

  // the algorithm for record compression
  int record_compressor;

};

} // namespace hamsterdb

#endif // HAM_DB_CONFIG_H
