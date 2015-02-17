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

#include "0root/root.h"

#include "ham/hamsterdb_ola.h"

// Always verify that a file of level N does not include headers > N!
#include "1base/error.h"
#include "3btree/btree_visitor.h"
#include "4db/db.h"
#include "4db/db_local.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

using namespace hamsterdb;

ham_status_t HAM_CALLCONV
hola_count(ham_db_t *hdb, ham_txn_t *htxn, hola_result_t *result)
{
  if (!hdb) {
    ham_trace(("parameter 'db' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }
  if (!result) {
    ham_trace(("parameter 'result' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  Database *db = (Database *)hdb;
  Transaction *txn = (Transaction *)htxn;

  result->type = HAM_TYPE_UINT64;
  result->u.result_u64 = 0;

  ScopedLock lock(db->get_env()->mutex());
  return (db->set_error(db->count(txn, false, &result->u.result_u64)));
}

//
// A ScanVisitor for hola_count_if
//
template<typename PodType>
struct CountIfScanVisitor : public ScanVisitor {
  CountIfScanVisitor(hola_bool_predicate_t *pred)
    : m_count(0), m_pred(pred) {
  }

  // Operates on a single key
  virtual void operator()(const void *key_data, uint16_t key_size, 
                  size_t duplicate_count) {
    if (m_pred->predicate_func(key_data, key_size, m_pred->context))
      m_count++;
  }

  // Operates on an array of keys
  virtual void operator()(const void *key_array, size_t key_count) {
    const PodType *p = (const PodType *)key_array;
    const PodType *end = &p[key_count];
    for (; p < end; p++) {
      if (m_pred->predicate_func(p, sizeof(PodType), m_pred->context))
        m_count++;
    }
  }

  // Assigns the result to |result|
  virtual void assign_result(hola_result_t *result) {
    memcpy(&result->u.result_u64, &m_count, sizeof(uint64_t));
  }

  // The counter
  uint64_t m_count;

  // The user's predicate
  hola_bool_predicate_t *m_pred;
};

//
// A ScanVisitor for hola_count_if on binary keys
//
struct CountIfScanVisitorBinary : public ScanVisitor {
  CountIfScanVisitorBinary(size_t key_size, hola_bool_predicate_t *pred)
    : m_count(0), m_key_size(key_size), m_pred(pred) {
  }

  // Operates on a single key
  virtual void operator()(const void *key_data, uint16_t key_size, 
                  size_t duplicate_count) {
    if (m_pred->predicate_func(key_data, key_size, m_pred->context))
      m_count++;
  }

  // Operates on an array of keys
  virtual void operator()(const void *key_array, size_t key_count) {
    assert(m_key_size != HAM_KEY_SIZE_UNLIMITED);
    const uint8_t *p = (const uint8_t *)key_array;
    const uint8_t *end = &p[key_count * m_key_size];
    for (; p < end; p += m_key_size) {
      if (m_pred->predicate_func(p, m_key_size, m_pred->context))
        m_count++;
    }
  }

  // Assigns the result to |result|
  virtual void assign_result(hola_result_t *result) {
    memcpy(&result->u.result_u64, &m_count, sizeof(uint64_t));
  }

  // The counter
  uint64_t m_count;

  // The key size
  uint16_t m_key_size;

  // The user's predicate
  hola_bool_predicate_t *m_pred;
};

ham_status_t HAM_CALLCONV
hola_count_if(ham_db_t *hdb, ham_txn_t *txn, hola_bool_predicate_t *pred,
                hola_result_t *result)
{
  if (!hdb) {
    ham_trace(("parameter 'db' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }
  if (!pred) {
    ham_trace(("parameter 'pred' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }
  if (!result) {
    ham_trace(("parameter 'result' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  // Remote databases are not yet supported
  LocalDatabase *db = dynamic_cast<LocalDatabase *>((Database *)hdb);
  if (!db) {
    ham_trace(("hola_* functions are not yet supported for remote databases"));
    return (HAM_INV_PARAMETER);
  }

  std::auto_ptr<ScanVisitor> visitor;
  result->u.result_u64 = 0;
  result->type = HAM_TYPE_UINT64;

  switch (db->config().key_type) {
    case HAM_TYPE_UINT8:
      visitor.reset(new CountIfScanVisitor<uint8_t>(pred));
      break;
    case HAM_TYPE_UINT16:
      visitor.reset(new CountIfScanVisitor<uint16_t>(pred));
      break;
    case HAM_TYPE_UINT32:
      visitor.reset(new CountIfScanVisitor<uint32_t>(pred));
      break;
    case HAM_TYPE_UINT64:
      visitor.reset(new CountIfScanVisitor<uint64_t>(pred));
      break;
    case HAM_TYPE_REAL32:
      visitor.reset(new CountIfScanVisitor<float>(pred));
      break;
    case HAM_TYPE_REAL64:
      visitor.reset(new CountIfScanVisitor<double>(pred));
      break;
    case HAM_TYPE_BINARY:
      visitor.reset(new CountIfScanVisitorBinary(db->config().key_size,
                              pred));
      break;
    default:
      ham_assert(!"shouldn't be here");
      return (HAM_INV_PARAMETER);
  }

  ScopedLock lock(db->get_env()->mutex());
  ham_status_t st = db->scan((Transaction *)txn, visitor.get(), false);
  if (st == 0)
    visitor->assign_result(result);
  return (db->set_error(st));
}

ham_status_t HAM_CALLCONV
hola_count_distinct(ham_db_t *hdb, ham_txn_t *htxn, hola_result_t *result)
{
  if (!hdb) {
    ham_trace(("parameter 'db' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }
  if (!result) {
    ham_trace(("parameter 'result' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  Database *db = (Database *)hdb;
  Transaction *txn = (Transaction *)htxn;

  result->type = HAM_TYPE_UINT64;
  result->u.result_u64 = 0;

  ScopedLock lock(db->get_env()->mutex());
  return (db->set_error(db->count(txn, true, &result->u.result_u64)));
}

ham_status_t HAM_CALLCONV
hola_count_distinct_if(ham_db_t *hdb, ham_txn_t *txn,
                hola_bool_predicate_t *pred, hola_result_t *result)
{
  if (!hdb) {
    ham_trace(("parameter 'db' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }
  if (!pred) {
    ham_trace(("parameter 'pred' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }
  if (!result) {
    ham_trace(("parameter 'result' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  // Remote databases are not yet supported
  LocalDatabase *db = dynamic_cast<LocalDatabase *>((Database *)hdb);
  if (!db) {
    ham_trace(("hola_* functions are not yet supported for remote databases"));
    return (HAM_INV_PARAMETER);
  }

  std::auto_ptr<ScanVisitor> visitor;
  result->u.result_u64 = 0;
  result->type = HAM_TYPE_UINT64;

  switch (db->config().key_type) {
    case HAM_TYPE_UINT8:
      visitor.reset(new CountIfScanVisitor<uint8_t>(pred));
      break;
    case HAM_TYPE_UINT16:
      visitor.reset(new CountIfScanVisitor<uint16_t>(pred));
      break;
    case HAM_TYPE_UINT32:
      visitor.reset(new CountIfScanVisitor<uint32_t>(pred));
      break;
    case HAM_TYPE_UINT64:
      visitor.reset(new CountIfScanVisitor<uint64_t>(pred));
      break;
    case HAM_TYPE_REAL32:
      visitor.reset(new CountIfScanVisitor<float>(pred));
      break;
    case HAM_TYPE_REAL64:
      visitor.reset(new CountIfScanVisitor<double>(pred));
      break;
    case HAM_TYPE_BINARY:
      visitor.reset(new CountIfScanVisitorBinary(db->config().key_size,
                              pred));
      break;
    default:
      ham_assert(!"shouldn't be here");
      return (HAM_INV_PARAMETER);
  }

  ScopedLock lock(db->get_env()->mutex());
  ham_status_t st = db->scan((Transaction *)txn, visitor.get(), true);
  if (st == 0)
    visitor->assign_result(result);
  return (db->set_error(st));
}

//
// A ScanVisitor for hola_average
//
template<typename PodType, typename ResultType>
struct AverageScanVisitor : public ScanVisitor {
  AverageScanVisitor()
    : m_sum(0), m_count(0) {
  }

  // Operates on a single key
  virtual void operator()(const void *key_data, uint16_t key_size, 
                  size_t duplicate_count) {
    ham_assert(key_size == sizeof(PodType));

    m_sum += *(const PodType *)key_data * duplicate_count;
    m_count++;
  }

  // Operates on an array of keys
  virtual void operator()(const void *key_array, size_t key_count) {
    const PodType *p = (const PodType *)key_array;
    const PodType *end = &p[key_count];
    for (; p < end; p++)
      m_sum += *p;
    m_count += key_count;
  }

  // Assigns the result to |result|
  virtual void assign_result(hola_result_t *result) {
    ResultType res = m_sum / m_count;
    memcpy(&result->u.result_u64, &res, sizeof(uint64_t));
  }

  // The sum of all keys
  ResultType m_sum;

  // For counting the keys
  uint64_t m_count;
};

ham_status_t HAM_CALLCONV
hola_average(ham_db_t *hdb, ham_txn_t *txn, hola_result_t *result)
{
  if (!hdb) {
    ham_trace(("parameter 'db' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }
  if (!result) {
    ham_trace(("parameter 'result' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  // Remote databases are not yet supported
  LocalDatabase *db = dynamic_cast<LocalDatabase *>((Database *)hdb);
  if (!db) {
    ham_trace(("hola_* functions are not yet supported for remote databases"));
    return (HAM_INV_PARAMETER);
  }

  std::auto_ptr<ScanVisitor> visitor;
  result->u.result_u64 = 0;

  switch (db->config().key_type) {
    case HAM_TYPE_UINT8:
      result->type = HAM_TYPE_UINT64;
      visitor.reset(new AverageScanVisitor<uint8_t, uint64_t>());
      break;
    case HAM_TYPE_UINT16:
      result->type = HAM_TYPE_UINT64;
      visitor.reset(new AverageScanVisitor<uint16_t, uint64_t>());
      break;
    case HAM_TYPE_UINT32:
      result->type = HAM_TYPE_UINT64;
      visitor.reset(new AverageScanVisitor<uint32_t, uint64_t>());
      break;
    case HAM_TYPE_UINT64:
      result->type = HAM_TYPE_UINT64;
      visitor.reset(new AverageScanVisitor<uint64_t, uint64_t>());
      break;
    case HAM_TYPE_REAL32:
      result->type = HAM_TYPE_REAL64;
      visitor.reset(new AverageScanVisitor<float, double>());
      break;
    case HAM_TYPE_REAL64:
      result->type = HAM_TYPE_REAL64;
      visitor.reset(new AverageScanVisitor<double, double>());
      break;
    default:
      ham_trace(("hola_avg* can only be applied to numerical data"));
      return (HAM_INV_PARAMETER);
  }

  ScopedLock lock(db->get_env()->mutex());
  ham_status_t st = db->scan((Transaction *)txn, visitor.get(), false);
  if (st == 0)
    visitor->assign_result(result);
  return (db->set_error(st));
}

//
// A ScanVisitor for hola_average_if
//
template<typename PodType, typename ResultType>
struct AverageIfScanVisitor : public ScanVisitor {
  AverageIfScanVisitor(hola_bool_predicate_t *pred)
    : m_sum(0), m_count(0), m_pred(pred) {
  }

  // Operates on a single key
  virtual void operator()(const void *key_data, uint16_t key_size, 
                  size_t duplicate_count) {
    ham_assert(key_size == sizeof(PodType));

    if (m_pred->predicate_func(key_data, key_size, m_pred->context)) {
      m_sum += *(const PodType *)key_data * duplicate_count;
      m_count++;
    }
  }

  // Operates on an array of keys
  virtual void operator()(const void *key_array, size_t key_count) {
    const PodType *p = (const PodType *)key_array;
    const PodType *end = &p[key_count];
    for (; p < end; p++) {
      if (m_pred->predicate_func(p, sizeof(PodType), m_pred->context)) {
        m_sum += *p;
        m_count++;
      }
    }
  }

  // Assigns the result to |result|
  virtual void assign_result(hola_result_t *result) {
    ResultType res = m_sum / m_count;
    memcpy(&result->u.result_u64, &res, sizeof(uint64_t));
  }

  // The sum of all keys
  ResultType m_sum;

  // For counting the keys
  uint64_t m_count;

  // The user's predicate function
  hola_bool_predicate_t *m_pred;
};

ham_status_t HAM_CALLCONV
hola_average_if(ham_db_t *hdb, ham_txn_t *txn, hola_bool_predicate_t *pred,
                hola_result_t *result)
{
  if (!hdb) {
    ham_trace(("parameter 'db' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }
  if (!pred) {
    ham_trace(("parameter 'pred' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }
  if (!result) {
    ham_trace(("parameter 'result' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  // Remote databases are not yet supported
  LocalDatabase *db = dynamic_cast<LocalDatabase *>((Database *)hdb);
  if (!db) {
    ham_trace(("hola_* functions are not yet supported for remote databases"));
    return (HAM_INV_PARAMETER);
  }

  std::auto_ptr<ScanVisitor> visitor;
  result->u.result_u64 = 0;

  switch (db->config().key_type) {
    case HAM_TYPE_UINT8:
      result->type = HAM_TYPE_UINT64;
      visitor.reset(new AverageIfScanVisitor<uint8_t, uint64_t>(pred));
      break;
    case HAM_TYPE_UINT16:
      result->type = HAM_TYPE_UINT64;
      visitor.reset(new AverageIfScanVisitor<uint16_t, uint64_t>(pred));
      break;
    case HAM_TYPE_UINT32:
      result->type = HAM_TYPE_UINT64;
      visitor.reset(new AverageIfScanVisitor<uint32_t, uint64_t>(pred));
      break;
    case HAM_TYPE_UINT64:
      result->type = HAM_TYPE_UINT64;
      visitor.reset(new AverageIfScanVisitor<uint64_t, uint64_t>(pred));
      break;
    case HAM_TYPE_REAL32:
      result->type = HAM_TYPE_REAL64;
      visitor.reset(new AverageIfScanVisitor<float, double>(pred));
      break;
    case HAM_TYPE_REAL64:
      result->type = HAM_TYPE_REAL64;
      visitor.reset(new AverageIfScanVisitor<double, double>(pred));
      break;
    default:
      ham_trace(("hola_avg* can only be applied to numerical data"));
      return (HAM_INV_PARAMETER);
  }

  ScopedLock lock(db->get_env()->mutex());
  ham_status_t st = db->scan((Transaction *)txn, visitor.get(), false);
  if (st == 0)
    visitor->assign_result(result);
  return (db->set_error(st));
}

//
// A ScanVisitor for hola_sum
//
template<typename PodType, typename ResultType>
struct SumScanVisitor : public ScanVisitor {
  SumScanVisitor()
    : m_sum(0) {
  }

  // Operates on a single key
  virtual void operator()(const void *key_data, uint16_t key_size, 
                  size_t duplicate_count) {
    ham_assert(key_size == sizeof(PodType));
    m_sum += *(const PodType *)key_data * duplicate_count;
  }

  // Operates on an array of keys
  virtual void operator()(const void *key_array, size_t key_count) {
    const PodType *p = (const PodType *)key_array;
    const PodType *end = &p[key_count];
    const int kMax = 8;
    ResultType sums[kMax] = {0};
    for (; p + kMax < end; p += kMax) {
#if defined __GNUC__
      __builtin_prefetch(((char *)p) + kMax * sizeof(PodType));
#endif
      sums[0] += p[0];
      sums[1] += p[1];
      sums[2] += p[2];
      sums[3] += p[3];
      sums[4] += p[4];
      sums[5] += p[5];
      sums[6] += p[6];
      sums[7] += p[7];
    }
    for (; p < end; p++)
      m_sum += *p;
    for (int i = 0; i < kMax; i++)
      m_sum += sums[i];
  }

  // Assigns the result to |result|
  virtual void assign_result(hola_result_t *result) {
    memcpy(&result->u.result_u64, &m_sum, sizeof(uint64_t));
  }

  // The sum of all keys
  ResultType m_sum;
};

ham_status_t HAM_CALLCONV
hola_sum(ham_db_t *hdb, ham_txn_t *txn, hola_result_t *result)
{
  if (!hdb) {
    ham_trace(("parameter 'hdb' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }
  if (!result) {
    ham_trace(("parameter 'result' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  std::auto_ptr<ScanVisitor> visitor;
  result->u.result_u64 = 0;

  // Remote databases are not yet supported
  LocalDatabase *db = dynamic_cast<LocalDatabase *>((Database *)hdb);
  if (!db) {
    ham_trace(("hola_* functions are not yet supported for remote databases"));
    return (HAM_INV_PARAMETER);
  }

  switch (db->config().key_type) {
    case HAM_TYPE_UINT8:
      result->type = HAM_TYPE_UINT64;
      visitor.reset(new SumScanVisitor<uint8_t, uint64_t>());
      break;
    case HAM_TYPE_UINT16:
      result->type = HAM_TYPE_UINT64;
      visitor.reset(new SumScanVisitor<uint16_t, uint64_t>());
      break;
    case HAM_TYPE_UINT32:
      result->type = HAM_TYPE_UINT64;
      visitor.reset(new SumScanVisitor<uint32_t, uint64_t>());
      break;
    case HAM_TYPE_UINT64:
      result->type = HAM_TYPE_UINT64;
      visitor.reset(new SumScanVisitor<uint64_t, uint64_t>());
      break;
    case HAM_TYPE_REAL32:
      result->type = HAM_TYPE_REAL64;
      visitor.reset(new SumScanVisitor<float, double>());
      break;
    case HAM_TYPE_REAL64:
      result->type = HAM_TYPE_REAL64;
      visitor.reset(new SumScanVisitor<double, double>());
      break;
    default:
      ham_trace(("hola_sum* can only be applied to numerical data"));
      return (HAM_INV_PARAMETER);
  }

  ScopedLock lock(db->get_env()->mutex());
  ham_status_t st = db->scan((Transaction *)txn, visitor.get(), false);
  if (st == 0)
    visitor->assign_result(result);
  return (db->set_error(st));
}

//
// A ScanVisitor for hola_sum_if
//
template<typename PodType, typename ResultType>
struct SumIfScanVisitor : public ScanVisitor {
  SumIfScanVisitor(hola_bool_predicate_t *pred)
    : m_sum(0), m_pred(pred) {
  }

  // Operates on a single key
  virtual void operator()(const void *key_data, uint16_t key_size, 
                  size_t duplicate_count) {
    ham_assert(key_size == sizeof(PodType));

    if (m_pred->predicate_func(key_data, key_size, m_pred->context))
      m_sum += *(const PodType *)key_data * duplicate_count;
  }

  // Operates on an array of keys
  virtual void operator()(const void *key_array, size_t key_count) {
    const PodType *p = (const PodType *)key_array;
    const PodType *end = &p[key_count];
    for (; p < end; p++) {
      if (m_pred->predicate_func(p, sizeof(PodType), m_pred->context))
        m_sum += *p;
    }
  }

  // Assigns the result to |result|
  virtual void assign_result(hola_result_t *result) {
    memcpy(&result->u.result_u64, &m_sum, sizeof(uint64_t));
  }

  // The sum of all keys
  ResultType m_sum;

  // The user's predicate function
  hola_bool_predicate_t *m_pred;
};

ham_status_t HAM_CALLCONV
hola_sum_if(ham_db_t *hdb, ham_txn_t *txn, hola_bool_predicate_t *pred,
                hola_result_t *result)
{
  if (!hdb) {
    ham_trace(("parameter 'db' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }
  if (!pred) {
    ham_trace(("parameter 'pred' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }
  if (!result) {
    ham_trace(("parameter 'result' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  // Remote databases are not yet supported
  LocalDatabase *db = dynamic_cast<LocalDatabase *>((Database *)hdb);
  if (!db) {
    ham_trace(("hola_* functions are not yet supported for remote databases"));
    return (HAM_INV_PARAMETER);
  }

  std::auto_ptr<ScanVisitor> visitor;
  result->u.result_u64 = 0;

  switch (db->config().key_type) {
    case HAM_TYPE_UINT8:
      result->type = HAM_TYPE_UINT64;
      visitor.reset(new SumIfScanVisitor<uint8_t, uint64_t>(pred));
      break;
    case HAM_TYPE_UINT16:
      result->type = HAM_TYPE_UINT64;
      visitor.reset(new SumIfScanVisitor<uint16_t, uint64_t>(pred));
      break;
    case HAM_TYPE_UINT32:
      result->type = HAM_TYPE_UINT64;
      visitor.reset(new SumIfScanVisitor<uint32_t, uint64_t>(pred));
      break;
    case HAM_TYPE_UINT64:
      result->type = HAM_TYPE_UINT64;
      visitor.reset(new SumIfScanVisitor<uint64_t, uint64_t>(pred));
      break;
    case HAM_TYPE_REAL32:
      result->type = HAM_TYPE_REAL64;
      visitor.reset(new SumIfScanVisitor<float, double>(pred));
      break;
    case HAM_TYPE_REAL64:
      result->type = HAM_TYPE_REAL64;
      visitor.reset(new SumIfScanVisitor<double, double>(pred));
      break;
    default:
      ham_trace(("hola_sum* can only be applied to numerical data"));
      return (HAM_INV_PARAMETER);
  }

  ScopedLock lock(db->get_env()->mutex());
  ham_status_t st = db->scan((Transaction *)txn, visitor.get(), false);
  if (st == 0)
    visitor->assign_result(result);
  return (db->set_error(st));
}
