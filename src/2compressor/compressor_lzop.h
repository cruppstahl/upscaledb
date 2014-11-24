/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 * All Rights Reserved.
 *
 * NOTICE: All information contained herein is, and remains the property
 * of Christoph Rupp and his suppliers, if any. The intellectual and
 * technical concepts contained herein are proprietary to Christoph Rupp
 * and his suppliers and may be covered by Patents, patents in process,
 * and are protected by trade secret or copyright law. Dissemination of
 * this information or reproduction of this material is strictly forbidden
 * unless prior written permission is obtained from Christoph Rupp.
 *
 * See files COPYING.* for License information.
 */

/*
 * A compressor implementation using liblzo.
 *
 * @exception_safe: unknown
 * @thread_safe: unknown
 */

#ifndef HAM_COMPRESSOR_LZOP_H
#define HAM_COMPRESSOR_LZOP_H

#ifdef HAM_ENABLE_COMPRESSION

#ifdef HAVE_LZO_LZO1X_H

#include "0root/root.h"

#include <lzo/lzoconf.h>
#include <lzo/lzo1x.h>

#include "2compressor/compressor.h"

// Always verify that a file of level N does not include headers > N!

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

class LzopCompressor : public Compressor {
  public:
    // Constructor
    LzopCompressor()
      : m_work(LZO1X_1_MEM_COMPRESS) {
      if (::lzo_init() != LZO_E_OK)
        throw Exception(HAM_INTERNAL_ERROR);
    }

  protected:
    // Returns the maximum number of bytes that are required for
    // compressing |length| bytes.
    virtual uint32_t get_compressed_length(uint32_t length) {
      return (length + length / 16 + 64 + 3);
    }

    // Performs the actual compression. |outp| points into |m_arena| and
    // has sufficient size (allocated with |get_compressed_length()|).
    //
    // Returns the length of the compressed data.
    virtual uint32_t do_compress(const uint8_t *inp, uint32_t inlength,
                            uint8_t *outp, uint32_t outlength) {
      int r = ::lzo1x_1_compress(inp, inlength, outp, (lzo_uint *)&outlength,
                      (uint8_t *)m_work.get_ptr());
      if (r != 0)
        throw Exception(HAM_INTERNAL_ERROR);
      return (outlength);
    }

    // Performs the actual decompression. Derived classes decompress into
    // |m_arena| which has sufficient size for the decompressed data.
    virtual void do_decompress(const uint8_t *inp, uint32_t inlength,
                            uint8_t *outp, uint32_t outlength) {
      int r = ::lzo1x_decompress(inp, inlength,
                      outp, (lzo_uint *)&outlength, 0);
      if (r != 0)
        throw Exception(HAM_INTERNAL_ERROR);
    }

  private:
    ByteArray m_work;
};

}; // namespace hamsterdb

#endif // HAVE_LZO_LZO1X_H

#endif // HAM_ENABLE_COMPRESSION

#endif // HAM_COMPRESSOR_LZOP_H
