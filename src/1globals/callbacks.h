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
 * Callback management
 *
 * @thread_safe: yes
 * @exception_safe: nothrow
 */

#ifndef UPS_UPSCALEDB_CALLBACKS_H
#define UPS_UPSCALEDB_CALLBACKS_H

#include "0root/root.h"

#include <string>

#include <ups/upscaledb.h>

// Always verify that a file of level N does not include headers > N!

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

/*
 * The struct CallbackManager provides a common namespace for all
 * callback-related activities
 */
struct CallbackManager
{
  /* Calculates the hash of a callback function name */
  static uint32_t hash(std::string name);

  /* Adds a new callback to the system. |name| is case-insensitive.
   * Passing nullptr as |func| is valid and deletes an entry.
   * Adding the same name twice will be silently ignored. */
  static void add(const char *name, ups_compare_func_t func);

  /* Returns true if a callback with this name is registered. |name| is
   * case-insensitive */
  static bool is_registered(const char *name);

  /* Returns a callback function, or NULL. |name| is case-insensitive */
  static ups_compare_func_t get(const char *name);

  /* Returns a callback function, or NULL */
  static ups_compare_func_t get(uint32_t hash);
};

} // namespace upscaledb

#endif /* UPS_UPSCALEDB_CALLBACKS_H */
