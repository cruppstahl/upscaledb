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

class CompressorFactory {
  public:
    // Returns true if the specified compressor is available, otherwise false
    static bool is_available(int type);

    // Creates a new Compressor instance for the specified |type| (being
    // UPS_COMPRESSOR_ZLIB, UPS_COMPRESSOR_SNAPPY etc)
    static Compressor *create(int type);
};

}; // namespace upscaledb

#endif // UPS_COMPRESSOR_FACTORY_H
