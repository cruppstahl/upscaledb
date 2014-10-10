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

/*
 * Base class for KeyLists
 *
 * @exception_safe: unknown
 * @thread_safe: unknown
 */

#ifndef HAM_BTREE_KEYS_BASE_H
#define HAM_BTREE_KEYS_BASE_H

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

struct BaseKeyList
{
  enum {
      // This KeyList cannot reduce its capacity in order to release storage
      kCanReduceCapacity = 0
  };

  BaseKeyList()
    : m_range_size(0) {
  }

  // Erases the extended part of a key; nothing to do here
  void erase_extended_key(int slot) const {
  }

  // Checks the integrity of this node. Throws an exception if there is a
  // violation.
  void check_integrity(ham_u32_t node_count, bool quick = false) const {
  }

  // Rearranges the list
  void vacuumize(ham_u32_t node_count, bool force) const {
  }

  // Performs a linear search in a given range between |start| and
  // |start + length|
  template<typename T, typename Cmp>
  int linear_search(T *data, size_t start, size_t length, T key,
                  Cmp &comparator, int *pcmp) {
    size_t c = start;
    size_t end = start + length;

#undef COMPARE
#define COMPARE(c)      if (key <= data[c]) {                           \
                          if (key < data[c]) {                          \
                            if (c == 0)                                 \
                              *pcmp = -1; /* key < data[0] */           \
                            else                                        \
                              *pcmp = +1; /* key > data[c - 1] */       \
                            return ((c) - 1);                           \
                          }                                             \
                          *pcmp = 0;                                    \
                          return (c);                                   \
                        }

    while (c + 8 < end) {
      COMPARE(c)
      COMPARE(c + 1)
      COMPARE(c + 2)
      COMPARE(c + 3)
      COMPARE(c + 4)
      COMPARE(c + 5)
      COMPARE(c + 6)
      COMPARE(c + 7)
      c += 8;
    }

    while (c < end) {
      COMPARE(c)
      c++;
    }

    /* the new key is > the last key in the page */
    *pcmp = 1;
    return (start + length - 1);
  }

  // The size of the range (in bytes)
  size_t m_range_size;
};

} // namespace hamsterdb

#endif /* HAM_BTREE_KEYS_BASE_H */
