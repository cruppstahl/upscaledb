/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
  static int encode_u64(ham_u8_t *p, ham_u64_t n) {
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

  /* decodes and returns a pickled number */
  static ham_u64_t decode_u64(int n, ham_u8_t *p) {
    ham_u64_t ret = 0;

    for (int i = 0; i < n - 1; i++) {
      ret += *(p + (n - i - 1));
      ret <<= 4;
    }

    // last assignment is without *= 10
    return (ret + *p);
  }
};

} // namespace hamsterdb

#endif // HAM_PICKLE_H__
