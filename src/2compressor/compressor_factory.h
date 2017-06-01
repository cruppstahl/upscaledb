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
 * A factory for compressor objects.
 *
 * @exception_safe: unknown
 * @thread_safe: unknown
 */

#ifndef UPS_COMPRESSOR_FACTORY_H
#define UPS_COMPRESSOR_FACTORY_H

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!
#include "2compressor/compressor.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

struct CompressorFactory {
  // Returns true if the specified compressor is available, otherwise false
  static bool is_available(int type);

  // Creates a new Compressor instance for the specified |type| (being
  // UPS_COMPRESSOR_ZLIB, UPS_COMPRESSOR_SNAPPY etc)
  static Compressor *create(int type);
};

}; // namespace upscaledb

#endif // UPS_COMPRESSOR_FACTORY_H
