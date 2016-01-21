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

class Compressor {
  public:
    // Constructor
    Compressor()
      : m_skip(0) {
    }

    // Virtual destructor - can be overwritten
    virtual ~Compressor() {
    }

    // Compresses |inlength1| bytes of data in |inp1|. If |inp2| is supplied
    // then |inp2| will be compressed immediately after |inp1|.
    // The compressed data can then be retrieved with |get_output_data()|.
    //
    // Returns the length of the compressed data.
    uint32_t compress(const uint8_t *inp1, uint32_t inlength1,
                    const uint8_t *inp2 = 0, uint32_t inlength2 = 0) {
      uint32_t clen = 0;
      uint32_t arena_size = m_skip + get_compressed_length(inlength1);
      if (inp2 != 0)
        arena_size += get_compressed_length(inlength2);
      m_arena.resize(arena_size + m_skip);

      uint8_t *out = (uint8_t *)m_arena.get_ptr() + m_skip;

      clen = do_compress(inp1, inlength1, out,
                                  m_arena.get_size() - m_skip);
      if (inp2)
        clen += do_compress(inp2, inlength2, out + clen,
                          m_arena.get_size() - clen - m_skip);
      return (clen);
    }

    // Reserves |n| bytes in the output buffer; can be used by the caller
    // to insert flags or sizes
    void reserve(int n) {
      m_skip = n;
    }

    // Decompresses |inlength| bytes of data in |inp|. |outlength| is the
    // expected size of the decompressed data.
    void decompress(const uint8_t *inp, uint32_t inlength,
                    uint32_t outlength) {
      m_arena.resize(outlength);
      do_decompress(inp, inlength, (uint8_t *)m_arena.get_ptr(), outlength);
    }

    // Decompresses |inlength| bytes of data in |inp|. |outlength| is the
    // expected size of the decompressed data. Uses the caller's |arena|
    // for storage.
    void decompress(const uint8_t *inp, uint32_t inlength,
                    uint32_t outlength, ByteArray *arena) {
      arena->resize(outlength);
      do_decompress(inp, inlength, (uint8_t *)arena->get_ptr(), outlength);
    }

    // Decompresses |inlength| bytes of data in |inp|. |outlength| is the
    // expected size of the decompressed data. Uses the caller's |destination|
    // for storage.
    void decompress(const uint8_t *inp, uint32_t inlength,
                    uint32_t outlength, uint8_t *destination) {
      do_decompress(inp, inlength, destination, outlength);
    }

    // Retrieves the compressed (or decompressed) data, including its size
    const uint8_t *get_output_data() const {
      return ((uint8_t *)m_arena.get_ptr());
    }

    // Same as above, but non-const
    uint8_t *get_output_data() {
      return ((uint8_t *)m_arena.get_ptr());
    }

    // Returns the internal memory arena
    ByteArray *get_arena() {
      return (&m_arena);
    }

  protected:
    // Returns the maximum number of bytes that are required for
    // compressing |length| bytes.
    virtual uint32_t get_compressed_length(uint32_t length) = 0;

    // Performs the actual compression. |outp| points into |m_arena| and
    // has sufficient size (allocated with |get_compressed_length()|).
    //
    // Returns the length of the compressed data.
    // In case of an error: returns length of the uncompressed data + 1
    virtual uint32_t do_compress(const uint8_t *inp, uint32_t inlength,
                            uint8_t *outp, uint32_t outlength) = 0;

    // Performs the actual decompression. |outp| points into |m_arena| and
    // has sufficient size for the decompressed data.
    virtual void do_decompress(const uint8_t *inp, uint32_t inlength,
                            uint8_t *outp, uint32_t outlength) = 0;

  private:
    // The ByteArray which stores the compressed (or decompressed) data
    ByteArray m_arena;

    // Number of bytes to reserve for the caller
    int m_skip;
};

}; // namespace upscaledb

#endif // UPS_COMPRESSOR_H
