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
 * An abstract base class for a compressor.
 *
 * @exception_safe: unknown
 * @thread_safe: unknown
 */

#ifndef UPS_COMPRESSOR_H
#define UPS_COMPRESSOR_H

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!
#include "1base/dynamic_array.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

struct Compressor {
  // Constructor
  Compressor()
    : skip(0) {
  }

  // Virtual destructor - can be overwritten
  virtual ~Compressor() {
  }

  // Compresses |inlength1| bytes of data in |inp1|. If |inp2| is supplied
  // then |inp2| will be compressed immediately after |inp1|.
  // The compressed data can then be retrieved with |get_output_data()|.
  //
  // Returns the length of the compressed data.
  virtual uint32_t compress(const uint8_t *inp1, uint32_t inlength1,
                    const uint8_t *inp2 = 0, uint32_t inlength2 = 0) = 0;

  // Decompresses |inlength| bytes of data in |inp|. |outlength| is the
  // expected size of the decompressed data.
  virtual void decompress(const uint8_t *inp, uint32_t inlength,
                  uint32_t outlength) = 0;

  // Decompresses |inlength| bytes of data in |inp|. |outlength| is the
  // expected size of the decompressed data. Uses the caller's |arena|
  // for storage.
  virtual void decompress(const uint8_t *inp, uint32_t inlength,
                  uint32_t outlength, ByteArray *arena) = 0;

  // Decompresses |inlength| bytes of data in |inp|. |outlength| is the
  // expected size of the decompressed data. Uses the caller's |destination|
  // for storage.
  virtual void decompress(const uint8_t *inp, uint32_t inlength,
                  uint32_t outlength, uint8_t *destination) = 0;

  // Reserves |n| bytes in the output buffer; can be used by the caller
  // to insert flags or sizes
  void reserve(int n) {
    skip = n;
  }

  // The ByteArray which stores the compressed (or decompressed) data
  ByteArray arena;

  // Number of bytes to reserve for the caller
  int skip;
};

template<typename T>
struct CompressorImpl : public Compressor
{
  // Compresses |inlength1| bytes of data in |inp1|. If |inp2| is supplied
  // then |inp2| will be compressed immediately after |inp1|.
  // The compressed data can then be retrieved with |get_output_data()|.
  //
  // Returns the length of the compressed data.
  virtual uint32_t compress(const uint8_t *inp1, uint32_t inlength1,
                  const uint8_t *inp2 = 0, uint32_t inlength2 = 0) {
    uint32_t clen = 0;
    uint32_t arena_size = skip + impl.compressed_length(inlength1);
    if (inp2 != 0)
      arena_size += impl.compressed_length(inlength2);
    arena.resize(arena_size + skip);

    uint8_t *out = arena.data() + skip;

    clen = impl.compress(inp1, inlength1, out, arena.size() - skip);
    if (inp2)
      clen += impl.compress(inp2, inlength2, out + clen,
                        arena.size() - clen - skip);
    return clen;
  }

  // Decompresses |inlength| bytes of data in |inp|. |outlength| is the
  // expected size of the decompressed data.
  void decompress(const uint8_t *inp, uint32_t inlength, uint32_t outlength) {
    arena.resize(outlength);
    impl.decompress(inp, inlength, arena.data(), outlength);
  }

  // Decompresses |inlength| bytes of data in |inp|. |outlength| is the
  // expected size of the decompressed data. Uses the caller's |arena|
  // for storage.
  void decompress(const uint8_t *inp, uint32_t inlength,
                  uint32_t outlength, ByteArray *arena) {
    arena->resize(outlength);
    impl.decompress(inp, inlength, arena->data(), outlength);
  }

  // Decompresses |inlength| bytes of data in |inp|. |outlength| is the
  // expected size of the decompressed data. Uses the caller's |destination|
  // for storage.
  void decompress(const uint8_t *inp, uint32_t inlength,
                  uint32_t outlength, uint8_t *destination) {
    impl.decompress(inp, inlength, destination, outlength);
  }

  // The implementation object
  T impl;
};

}; // namespace upscaledb

#endif // UPS_COMPRESSOR_H
