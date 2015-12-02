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
 * UQI Result sets
 *
 * @thread_safe: no
 * @exception_safe: nothrow
 */

#ifndef UPS_UPSCALEDB_RESULT_H
#define UPS_UPSCALEDB_RESULT_H

#include "0root/root.h"

#include "ups/upscaledb_uqi.h"

// Always verify that a file of level N does not include headers > N!
#include "1base/dynamic_array.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

/*
 * The struct Result is the actual implementation of uqs_result_t.
 */
struct Result
{
  uint32_t row_count;
  uint32_t key_type;
  uint32_t key_size;
  uint32_t record_type;
  uint32_t record_size;

  ByteArray key_data;
  ByteArray record_data;

  void add_key(const char *str) {
    key_size = UPS_KEY_SIZE_UNLIMITED;
    key_data.copy((const uint8_t *)str, ::strlen(str));
  }

  template<typename T>
  void add_record(T t) {
    record_size = sizeof(t);
    record_data.copy((const uint8_t *)&t, sizeof(t));
  }
};

} // namespace upscaledb

#endif /* UPS_UPSCALEDB_RESULT_H */
