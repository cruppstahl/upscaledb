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
 * A compressor which uses zlib.
 *
 * @exception_safe: unknown
 * @thread_safe: unknown
 */

#ifndef UPS_COMPRESSOR_ZLIB_H
#define UPS_COMPRESSOR_ZLIB_H

#ifdef HAVE_ZLIB_H

#include "0root/root.h"

#include <zlib.h>

#include "2compressor/compressor.h"

namespace upscaledb {

struct ZlibCompressor {
  uint32_t compressed_length(uint32_t length) {
    return ::compressBound(length);
  }

  uint32_t compress(const uint8_t *inp, uint32_t inlength,
                            uint8_t *outp, uint32_t outlength) {
    uLongf real_outlength = outlength;
    int zret = ::compress((Bytef *)outp, &real_outlength,
                      (const Bytef *)inp, inlength);
    if (zret != 0)
      throw Exception(UPS_INTERNAL_ERROR);
    return real_outlength;
  }

  void decompress(const uint8_t *inp, uint32_t inlength,
                            uint8_t *outp, uint32_t outlength) {
    uLongf real_outlength = outlength;
    int zret = ::uncompress((Bytef *)outp, &real_outlength,
                          (const Bytef *)inp, inlength);
    if (zret != 0)
      throw Exception(UPS_INTERNAL_ERROR);
  }
};

}; // namespace upscaledb;

#endif // HAVE_ZLIB_H

#endif // UPS_COMPRESSOR_ZLIB_H
