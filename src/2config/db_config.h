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
