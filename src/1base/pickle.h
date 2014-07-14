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

/*
 * Class for pickling/unpickling data to a buffer
 *
 * @exception_safe: nothrow
 * @thread_safe: yes
 */

#ifndef HAM_PICKLE_H
#define HAM_PICKLE_H

#include "0root/root.h"

#include "ham/types.h"

// Always verify that a file of level N does not include headers > N!

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

struct Pickle {
  /* encodes a uint64 number and stores it in |p|; returns the number of
   * bytes used */
  static size_t encode_u64(uint8_t *p, uint64_t n) {
    if (n <= 0xf) {
      *p = (uint8_t)n;
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
  static uint64_t decode_u64(size_t len, uint8_t *p) {
    uint64_t ret = 0;

    for (size_t i = 0; i < len - 1; i++) {
      ret += *(p + (len - i - 1));
      ret <<= 4;
    }

    // last assignment is without *= 10
    return (ret + *p);
  }
};

} // namespace hamsterdb

#endif // HAM_PICKLE_H
