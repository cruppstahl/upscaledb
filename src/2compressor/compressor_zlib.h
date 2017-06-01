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
