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
 * A compressor which uses snappy.
 *
 * @exception_safe: unknown
 * @thread_safe: yes
 */

#ifndef HAM_COMPRESSOR_SNAPPY_H
#define HAM_COMPRESSOR_SNAPPY_H

#ifdef HAM_ENABLE_COMPRESSION

#ifdef HAVE_SNAPPY_H

#include "0root/root.h"

#include <snappy.h>

// Always verify that a file of level N does not include headers > N!
#include "2compressor/compressor.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

struct SnappyCompressor {
  // Returns the maximum number of bytes that are required for
  // compressing |length| bytes.
  uint32_t get_compressed_length(uint32_t length) {
    return (snappy::MaxCompressedLength(length));
  }

  // Performs the actual compression. |outp| points into |m_arena| and
  // has sufficient size (allocated with |get_compressed_length()|).
  //
  // Returns the length of the compressed data.
  uint32_t compress(const uint8_t *inp, uint32_t inlength,
                          uint8_t *outp, uint32_t outlength) {
    size_t real_outlength = outlength;
    snappy::RawCompress((const char *)inp, inlength,
              (char *)outp, &real_outlength);
    ham_assert(real_outlength <= outlength);
    return (real_outlength);
  }

  // Performs the actual decompression. Derived classes decompress into
  // |m_arena| which has sufficient size for the decompressed data.
  void decompress(const uint8_t *inp, uint32_t inlength,
                          uint8_t *outp, uint32_t outlength) {
    ham_assert(snappy::IsValidCompressedBuffer((const char *)inp, inlength));
    if (!snappy::RawUncompress((const char *)inp, inlength,
                (char *)outp))
      throw Exception(HAM_INTERNAL_ERROR);
  }
};

}; // namespace hamsterdb

#endif // HAVE_SNAPPY_H

#endif // HAM_ENABLE_COMPRESSION

#endif // HAM_COMPRESSOR_SNAPPY_H
