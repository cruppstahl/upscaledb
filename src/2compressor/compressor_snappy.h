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
