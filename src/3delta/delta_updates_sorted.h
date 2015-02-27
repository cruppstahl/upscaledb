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
 * @exception_safe: nothrow
 * @thread_safe: no
 */

#ifndef HAM_DELTA_UPDATES_SORTED_H
#define HAM_DELTA_UPDATES_SORTED_H

#include "0root/root.h"

#include <vector>
#include <algorithm>

// Always verify that a file of level N does not include headers > N!
#include "3delta/delta_update.h"
#include "3btree/btree_index_traits.h"
#include "3btree/btree_cursor.h"
#include "4db/db_local.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

struct CompareHelper
{
  CompareHelper(LocalDatabase *db)
    : m_db(db) {
  }

  bool operator()(DeltaUpdate *lhs, ham_key_t *rhs) const {
    return (m_db->btree_index()->compare_keys(lhs->key(), rhs) < 0);
  }

  LocalDatabase *m_db;
};

//
// A sorted vector of DeltaUpdate objects
//
struct SortedDeltaUpdates
{
  typedef std::vector<DeltaUpdate *>::iterator Iterator;
  typedef std::vector<DeltaUpdate *>::reverse_iterator ReverseIterator;

  // Returns the number of stored DeltaUpdates
  size_t size() const {
    return (m_vec.size());
  }

  // Inserts a DeltaUpdate into the sorted vector; returns an Iterator
  // to the insorted element
  Iterator insert(DeltaUpdate *du, LocalDatabase *db) {
    ham_assert(du->next() == 0);
    ham_assert(du->previous() == 0);

    if (m_vec.empty()) {
      m_vec.push_back(du);
      return (m_vec.begin());
    }
    else {
      CompareHelper ch(db);
      Iterator it = std::lower_bound(m_vec.begin(), m_vec.end(), du->key(), ch);
      size_t index = it - begin();
      it = m_vec.insert(it, du);
      if (index > 0) {
        DeltaUpdate *other = at(index - 1);
        du->set_previous(other);
        other->set_next(du);
      }
      if (index < m_vec.size() - 1) {
        DeltaUpdate *other = at(index + 1);
        du->set_next(other);
        other->set_previous(du);
      }
      ham_assert(check_integrity());
      return (it);
    }
  }

  // Performs a lookup on the specific key
  Iterator find(ham_key_t *key, LocalDatabase *db, uint32_t flags = 0) {
    // ham_assert(check_integrity()); - removed to increase performance
    CompareHelper ch(db);
    Iterator it = std::lower_bound(m_vec.begin(), m_vec.end(), key, ch);
    if (it != m_vec.end()) {
      // TODO as soon as key->_flags |= BtreeKey::kApproximate is removed
      // then compare_keys() only needs to be called if exact_match == true!
      int cmp = db->btree_index()->compare_keys((*it)->key(), key);
      if ((flags == 0 || isset(flags, HAM_FIND_EXACT_MATCH)) && cmp == 0)
        return (it);
      if (notset(flags, HAM_FIND_EXACT_MATCH) && cmp == 0)
        return (it); // caller has to adjust the iterator

      if (isset(flags, HAM_FIND_LT_MATCH)) {
        if (cmp > 0 && it != m_vec.begin())
          it--;
      }

      if (issetany(flags, HAM_FIND_LT_MATCH | HAM_FIND_GT_MATCH)) {
        key->_flags |= BtreeKey::kApproximate;
        return (it);
      }
    }
    if (it == m_vec.end() && isset(flags, HAM_FIND_LT_MATCH) && m_vec.size()) {
      key->_flags |= BtreeKey::kApproximate;
      return (m_vec.end() - 1);
    }
    return (m_vec.end());
  }

  // Performs a lower bound lookup on the specific key
  Iterator find_lower_bound(ham_key_t *key, LocalDatabase *db) {
    ham_assert(check_integrity());
    CompareHelper ch(db);
    return (std::lower_bound(m_vec.begin(), m_vec.end(), key, ch));
  }

  // Returns the DeltaUpdate at the |position|
  DeltaUpdate *at(int index) {
    return (m_vec[index]);
  }

  // Returns an iterator to a specific DeltaUpdate
  // This is relatively slow, but the vectors are usually very small
  Iterator get(DeltaUpdate *update) {
    Iterator it = std::find(m_vec.begin(), m_vec.end(), update);
    ham_assert(it != m_vec.end());
    return (it);
  }

  // Returns the numeric index of an update
  // This is relatively slow, but the vectors are usually very small
  int index_of(DeltaUpdate *update) {
    return (get(update) - m_vec.begin());
  }

  // Returns a pointer to the first element of the vector
  Iterator begin() {
    return (m_vec.begin());
  }

  // Returns a pointer to the first element AFTER the vector
  Iterator end() {
    return (m_vec.end());
  }

  // Returns a reverse iterator to the last element of the vector
  ReverseIterator rbegin() {
    return (m_vec.rbegin());
  }

  // Returns a reverse iterator to the end condition of a loop
  ReverseIterator rend() {
    return (m_vec.rend());
  }

  // Append |other_deltas| to this collection
  void append(SortedDeltaUpdates &other) {
    if (!m_vec.empty()) {
      DeltaUpdate *last = *(end() - 1);
      DeltaUpdate *first = *other.m_vec.begin();
      last->set_next(first);
      first->set_previous(last);
    }

    m_vec.insert(m_vec.end(), other.m_vec.begin(), other.m_vec.end());
    ham_assert(check_integrity());
  }

  // Split deltas; move all DeltaUpdates at |pivot| (and following)
  // to |other_deltas|
  void split(int pivot, SortedDeltaUpdates &other) {
    if (m_vec.size() >= (size_t)pivot) {
      DeltaUpdate *last = *(other.m_vec.end() - 1);
      DeltaUpdate *first = *m_vec.begin();
      last->set_next(first);
      first->set_previous(last);
      other.m_vec.insert(other.m_vec.end(), m_vec.begin() + pivot, m_vec.end());
      ham_assert(check_integrity());
    }
  }

  // Verifies the integrity
  bool check_integrity() {
    if (m_vec.empty())
      return (true);

    DeltaUpdate *du = *begin();
    ham_assert(du->previous() == 0);

    int i = 0;
    for (Iterator it = begin(); it != end(); it++, i++) {
      du = *it;
      if (it - begin() > 0)
        ham_assert(du->previous() == *(it - 1));
      if (it < end() - 1)
        ham_assert(du->next() == *(it + 1));
      ham_assert(du == at(i));
    }

    du = *(end() - 1);
    ham_assert(du->next() == 0);

    return (true);
  }

  // The vector
  std::vector<DeltaUpdate *> m_vec;
};

} // namespace hamsterdb

#endif /* HAM_DELTA_UPDATES_SORTED_H */
