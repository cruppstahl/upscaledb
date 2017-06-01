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
 * A compressor implementation using liblzf.
 *
 * @exception_safe: unknown
 * @thread_safe: unknown
 */

#ifndef UPS_COMPRESSOR_LZF_H
#define UPS_COMPRESSOR_LZF_H

#include "0root/root.h"

#include "3rdparty/liblzf/lzf.h"

#include "2compressor/compressor.h"

// Always verify that a file of level N does not include headers > N!

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

struct LzfCompressor {
  // Returns the maximum number of bytes that are required for
  // compressing |length| bytes.
  uint32_t compressed_length(uint32_t length) {
    return length < 32 ? 64 : (length + length / 2);
  }

  // Performs the actual compression. |outp| points into |m_arena| and
  // has sufficient size (allocated with |get_compressed_length()|).
  //
  // Returns the length of the compressed data.
  uint32_t compress(const uint8_t *inp, uint32_t inlength,
                          uint8_t *outp, uint32_t outlength) {
    return ::lzf_compress(inp, inlength, outp, outlength);
  }

  // Performs the actual decompression. Derived classes decompress into
  // |m_arena| which has sufficient size for the decompressed data.
  void decompress(const uint8_t *inp, uint32_t inlength,
                          uint8_t *outp, uint32_t outlength) {
    if (!::lzf_decompress(inp, inlength, outp, outlength))
      throw Exception(UPS_INTERNAL_ERROR);
  }
};

}; // namespace upscaledb

#endif // UPS_COMPRESSOR_LZF_H
