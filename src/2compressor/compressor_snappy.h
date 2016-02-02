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
 * A compressor which uses snappy.
 *
 * @exception_safe: unknown
 * @thread_safe: unknown
 */

#ifndef UPS_COMPRESSOR_SNAPPY_H
#define UPS_COMPRESSOR_SNAPPY_H

#ifdef HAVE_SNAPPY_H

#include "0root/root.h"

#include <snappy.h>

// Always verify that a file of level N does not include headers > N!
#include "2compressor/compressor.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

struct SnappyCompressor {
  uint32_t compressed_length(uint32_t length) {
    return snappy::MaxCompressedLength(length);
  }

  uint32_t compress(const uint8_t *inp, uint32_t inlength,
                          uint8_t *outp, uint32_t outlength) {
    size_t real_outlength = outlength;
    snappy::RawCompress((const char *)inp, inlength,
              (char *)outp, &real_outlength);
    assert(real_outlength <= outlength);
    return real_outlength;
  }

  void decompress(const uint8_t *inp, uint32_t inlength,
                          uint8_t *outp, uint32_t outlength) {
    assert(snappy::IsValidCompressedBuffer((const char *)inp, inlength));
    if (!snappy::RawUncompress((const char *)inp, inlength,
                (char *)outp))
      throw Exception(UPS_INTERNAL_ERROR);
  }
};

}; // namespace upscaledb

#endif // HAVE_SNAPPY_H

#endif // UPS_COMPRESSOR_SNAPPY_H
