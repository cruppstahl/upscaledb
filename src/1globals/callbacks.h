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
