/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
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
 * A compressor implementation using liblzf.
 *
 * @exception_safe: unknown
 * @thread_safe: unknown
 */

#ifndef UPS_COMPRESSOR_LZF_H
#define UPS_COMPRESSOR_LZF_H

#ifdef UPS_ENABLE_COMPRESSION

#include "0root/root.h"

#include "3rdparty/liblzf/lzf.h"

#include "2compressor/compressor.h"

// Always verify that a file of level N does not include headers > N!

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

class LzfCompressor : public Compressor {
  public:
    // Constructor
    LzfCompressor() {
    }

  protected:
    // Returns the maximum number of bytes that are required for
    // compressing |length| bytes.
    virtual uint32_t get_compressed_length(uint32_t length) {
      return (length < 32 ? 64 : (length + length / 2));
    }

    // Performs the actual compression. |outp| points into |m_arena| and
    // has sufficient size (allocated with |get_compressed_length()|).
    //
    // Returns the length of the compressed data.
    virtual uint32_t do_compress(const uint8_t *inp, uint32_t inlength,
                            uint8_t *outp, uint32_t outlength) {
      return (::lzf_compress(inp, inlength, outp, outlength));
    }

    // Performs the actual decompression. Derived classes decompress into
    // |m_arena| which has sufficient size for the decompressed data.
    virtual void do_decompress(const uint8_t *inp, uint32_t inlength,
                            uint8_t *outp, uint32_t outlength) {
      if (!::lzf_decompress(inp, inlength, outp, outlength))
        throw Exception(UPS_INTERNAL_ERROR);
    }
};

}; // namespace upscaledb

#endif // UPS_ENABLE_COMPRESSION

#endif // UPS_COMPRESSOR_LZF_H
