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
 *
 */

#ifndef HAM_COMPRESSOR_ZLIB_H__
#define HAM_COMPRESSOR_ZLIB_H__

#ifdef HAM_ENABLE_COMPRESSION

#ifndef WIN32
#  include "../config.h"
#endif

#ifdef HAVE_ZLIB_H

#include <zlib.h>

#include "compressor.h"
#include "error.h"

namespace hamsterdb {

class ZlibCompressor : public Compressor {
  public:
    // Constructor
    ZlibCompressor() {
    }

  protected:
    // Returns the maximum number of bytes that are required for
    // compressing |length| bytes.
    virtual ham_u32_t get_compressed_length(ham_u32_t length) {
      return (::compressBound(length));
    }

    // Performs the actual compression. |outp| points into |m_arena| and
    // has sufficient size (allocated with |get_compressed_length()|).
    //
    // Returns the length of the compressed data.
    virtual ham_u32_t do_compress(const ham_u8_t *inp, ham_u32_t inlength,
                            ham_u8_t *outp, ham_u32_t outlength) {
      uLongf real_outlength = outlength;
      int zret = ::compress((Bytef *)outp, &real_outlength,
                        (const Bytef *)inp, inlength);
      if (zret != 0)
        throw Exception(HAM_INTERNAL_ERROR);
      return (real_outlength);
    }

    // Performs the actual decompression. Derived classes decompress into
    // |m_arena| which has sufficient size for the decompressed data.
    virtual void do_decompress(const ham_u8_t *inp, ham_u32_t inlength,
                            ham_u8_t *outp, ham_u32_t outlength) {
      uLongf real_outlength = outlength;
      int zret = ::uncompress((Bytef *)outp, &real_outlength,
                            (const Bytef *)inp, inlength);
      if (zret != 0)
        throw Exception(HAM_INTERNAL_ERROR);
    }
};

}; // namespace hamsterdb;

#endif // HAVE_ZLIB_H

#endif // HAM_ENABLE_COMPRESSION

#endif // HAM_COMPRESSOR_ZLIB_H__
