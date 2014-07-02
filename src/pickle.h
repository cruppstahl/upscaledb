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
 */

#ifndef HAM_PICKLE_H__
#define HAM_PICKLE_H__

namespace hamsterdb {

#include <ham/types.h>

/*
 * Helper class to serialize/unserialize numbers very compact
 */
struct Pickle {
  /* encodes a uint64 number and stores it in |p|; returns the number of
   * bytes used */
  static size_t encode_u64(ham_u8_t *p, ham_u64_t n) {
    if (n <= 0xf) {
      *p = n;
      return (1);
    }
    if (n <= 0xff) {
      *(p + 1) = (n & 0xf0) >> 4;
      *(p + 0) = n & 0xf;
      return (2);
    }
    if (n <= 0xfff) {
      *(p + 2) = (n & 0xf00) >> 8;
      *(p + 1) = (n & 0xf0) >> 4;
      *(p + 0) = n & 0xf;
      return (3);
    }
    if (n <= 0xffff) {
      *(p + 3) = (n & 0xf000) >> 12;
      *(p + 2) = (n & 0xf00) >> 8;
      *(p + 1) = (n & 0xf0) >> 4;
      *(p + 0) = n & 0xf;
      return (4);
    }
    if (n <= 0xfffff) {
      *(p + 4) = (n & 0xf0000) >> 16;
      *(p + 3) = (n & 0xf000) >> 12;
      *(p + 2) = (n & 0xf00) >> 8;
      *(p + 1) = (n & 0xf0) >> 4;
      *(p + 0) = n & 0xf;
      return (5);
    }
    if (n <= 0xffffff) {
      *(p + 5) = (n & 0xf00000) >> 24;
      *(p + 4) = (n & 0xf0000) >> 16;
      *(p + 3) = (n & 0xf000) >> 12;
      *(p + 2) = (n & 0xf00) >> 8;
      *(p + 1) = (n & 0xf0) >> 4;
      *(p + 0) = n & 0xf;
      return (6);
    }
    if (n <= 0xfffffff) {
      *(p + 6) = (n & 0xf000000) >> 32;
      *(p + 5) = (n & 0xf00000) >> 24;
      *(p + 4) = (n & 0xf0000) >> 16;
      *(p + 3) = (n & 0xf000) >> 12;
      *(p + 2) = (n & 0xf00) >> 8;
      *(p + 1) = (n & 0xf0) >> 4;
      *(p + 0) = n & 0xf;
      return (7);
    }
    *(p + 7) = (n & 0xf0000000) >> 36;
    *(p + 6) = (n & 0xf000000) >> 32;
    *(p + 5) = (n & 0xf00000) >> 24;
    *(p + 4) = (n & 0xf0000) >> 16;
    *(p + 3) = (n & 0xf000) >> 12;
    *(p + 2) = (n & 0xf00) >> 8;
    *(p + 1) = (n & 0xf0) >> 4;
    *(p + 0) = n & 0xf;
    return (8);
  }

  /* decodes and returns a pickled number of |len| bytes */
  static ham_u64_t decode_u64(size_t len, ham_u8_t *p) {
    ham_u64_t ret = 0;

    for (size_t i = 0; i < len - 1; i++) {
      ret += *(p + (len - i - 1));
      ret <<= 4;
    }

    // last assignment is without *= 10
    return (ret + *p);
  }
};

} // namespace hamsterdb

#endif // HAM_PICKLE_H__
