/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
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
 * @exception_safe: unknown
 * @thread_safe: unknown
 */

#ifndef HAM_BTREE_INDEX_TRAITS_H
#define HAM_BTREE_INDEX_TRAITS_H

#include "0root/root.h"

#include <string>

// Always verify that a file of level N does not include headers > N!

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

class Page;
class LocalDatabase;
class BtreeNodeProxy;

//
// Abstract base class, overwritten by a templated version
//
class BtreeIndexTraits
{
  public:
    BtreeIndexTraits(LocalDatabase *db)
      : m_db(db) {
    }
      
    // virtual destructor
    virtual ~BtreeIndexTraits() { }

    // Compares two keys
    // Returns -1, 0, +1 or higher positive values are the result of a
    // successful key comparison (0 if both keys match, -1 when
    // LHS < RHS key, +1 when LHS > RHS key).
    virtual int compare_keys(ham_key_t *lhs, ham_key_t *rhs) const = 0;

    // Implementation of get_node_from_page()
    virtual BtreeNodeProxy *get_node_from_page_impl(Page *page) const = 0;

    // Returns the class name (for testing)
    virtual std::string test_get_classname() const = 0;

  protected:
    LocalDatabase *m_db;
};

} // namespace hamsterdb

#endif /* HAM_BTREE_INDEX_TRAITS_H */
