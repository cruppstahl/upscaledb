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

#include "config.h"

#include <ham/hamsterdb_ola.h>

#include "error.h"
#include "db.h"
#include "db_local.h"

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

  try {
    ScopedLock lock(db->get_env()->get_mutex());

    db->count(txn, false, &result->u.result_u64);
    return (db->set_error(0));
  }
  catch (Exception &ex) {
    return (db->set_error(ex.code));
  }
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
  virtual void operator()(const void *key_data, ham_u16_t key_size, 
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
    memcpy(&result->u.result_u64, &m_count, sizeof(ham_u64_t));
  }

  // The counter
  ham_u64_t m_count;

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

  switch (db->get_key_type()) {
    case HAM_TYPE_UINT8:
      visitor.reset(new CountIfScanVisitor<ham_u8_t>(pred));
      break;
    case HAM_TYPE_UINT16:
      visitor.reset(new CountIfScanVisitor<ham_u16_t>(pred));
      break;
    case HAM_TYPE_UINT32:
      visitor.reset(new CountIfScanVisitor<ham_u32_t>(pred));
      break;
    case HAM_TYPE_UINT64:
      visitor.reset(new CountIfScanVisitor<ham_u64_t>(pred));
      break;
    case HAM_TYPE_REAL32:
      visitor.reset(new CountIfScanVisitor<float>(pred));
      break;
    case HAM_TYPE_REAL64:
      visitor.reset(new CountIfScanVisitor<double>(pred));
      break;
    case HAM_TYPE_BINARY:
      // template parameter is irrelevant - BINARY keys do not call any
      // template-specific function in CountIfScanVisitor
      // TODO this is nevertheless UGLY!
      visitor.reset(new CountIfScanVisitor<ham_u8_t>(pred));
      break;
    default:
      ham_assert(!"shouldn't be here");
      return (HAM_INV_PARAMETER);
  }

  try {
    ScopedLock lock(db->get_env()->get_mutex());

    db->scan((Transaction *)txn, visitor.get(), false);
    visitor->assign_result(result);
    return (db->set_error(0));
  }
  catch (Exception &ex) {
    return (db->set_error(ex.code));
  }
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

  try {
    ScopedLock lock(db->get_env()->get_mutex());

    db->count(txn, true, &result->u.result_u64);
    return (db->set_error(0));
  }
  catch (Exception &ex) {
    return (db->set_error(ex.code));
  }
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

  switch (db->get_key_type()) {
    case HAM_TYPE_UINT8:
      visitor.reset(new CountIfScanVisitor<ham_u8_t>(pred));
      break;
    case HAM_TYPE_UINT16:
      visitor.reset(new CountIfScanVisitor<ham_u16_t>(pred));
      break;
    case HAM_TYPE_UINT32:
      visitor.reset(new CountIfScanVisitor<ham_u32_t>(pred));
      break;
    case HAM_TYPE_UINT64:
      visitor.reset(new CountIfScanVisitor<ham_u64_t>(pred));
      break;
    case HAM_TYPE_REAL32:
      visitor.reset(new CountIfScanVisitor<float>(pred));
      break;
    case HAM_TYPE_REAL64:
      visitor.reset(new CountIfScanVisitor<double>(pred));
      break;
    case HAM_TYPE_BINARY:
      // template parameter is irrelevant - BINARY keys do not call any
      // template-specific function in CountIfScanVisitor
      // TODO this is nevertheless UGLY!
      visitor.reset(new CountIfScanVisitor<ham_u8_t>(pred));
      break;
    default:
      ham_assert(!"shouldn't be here");
      return (HAM_INV_PARAMETER);
  }

  try {
    ScopedLock lock(db->get_env()->get_mutex());
    db->scan((Transaction *)txn, visitor.get(), true);
    visitor->assign_result(result);
    return (db->set_error(0));
  }
  catch (Exception &ex) {
    return (db->set_error(ex.code));
  }
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
  virtual void operator()(const void *key_data, ham_u16_t key_size, 
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
    memcpy(&result->u.result_u64, &res, sizeof(ham_u64_t));
  }

  // The sum of all keys
  ResultType m_sum;

  // For counting the keys
  ham_u64_t m_count;
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

  switch (db->get_key_type()) {
    case HAM_TYPE_UINT8:
      result->type = HAM_TYPE_UINT64;
      visitor.reset(new AverageScanVisitor<ham_u8_t, ham_u64_t>());
      break;
    case HAM_TYPE_UINT16:
      result->type = HAM_TYPE_UINT64;
      visitor.reset(new AverageScanVisitor<ham_u16_t, ham_u64_t>());
      break;
    case HAM_TYPE_UINT32:
      result->type = HAM_TYPE_UINT64;
      visitor.reset(new AverageScanVisitor<ham_u32_t, ham_u64_t>());
      break;
    case HAM_TYPE_UINT64:
      result->type = HAM_TYPE_UINT64;
      visitor.reset(new AverageScanVisitor<ham_u64_t, ham_u64_t>());
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

  try {
    ScopedLock lock(db->get_env()->get_mutex());
    db->scan((Transaction *)txn, visitor.get(), false);
    visitor->assign_result(result);
    return (db->set_error(0));
  }
  catch (Exception &ex) {
    return (db->set_error(ex.code));
  }
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
  virtual void operator()(const void *key_data, ham_u16_t key_size, 
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
    memcpy(&result->u.result_u64, &res, sizeof(ham_u64_t));
  }

  // The sum of all keys
  ResultType m_sum;

  // For counting the keys
  ham_u64_t m_count;

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

  switch (db->get_key_type()) {
    case HAM_TYPE_UINT8:
      result->type = HAM_TYPE_UINT64;
      visitor.reset(new AverageIfScanVisitor<ham_u8_t, ham_u64_t>(pred));
      break;
    case HAM_TYPE_UINT16:
      result->type = HAM_TYPE_UINT64;
      visitor.reset(new AverageIfScanVisitor<ham_u16_t, ham_u64_t>(pred));
      break;
    case HAM_TYPE_UINT32:
      result->type = HAM_TYPE_UINT64;
      visitor.reset(new AverageIfScanVisitor<ham_u32_t, ham_u64_t>(pred));
      break;
    case HAM_TYPE_UINT64:
      result->type = HAM_TYPE_UINT64;
      visitor.reset(new AverageIfScanVisitor<ham_u64_t, ham_u64_t>(pred));
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

  try {
    ScopedLock lock(db->get_env()->get_mutex());
    db->scan((Transaction *)txn, visitor.get(), false);
    visitor->assign_result(result);
    return (db->set_error(0));
  }
  catch (Exception &ex) {
    return (db->set_error(ex.code));
  }
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
  virtual void operator()(const void *key_data, ham_u16_t key_size, 
                  size_t duplicate_count) {
    ham_assert(key_size == sizeof(PodType));
    m_sum += *(const PodType *)key_data * duplicate_count;
  }

  // Operates on an array of keys
  virtual void operator()(const void *key_array, size_t key_count) {
    const PodType *p = (const PodType *)key_array;
    const PodType *end = &p[key_count];
    for (; p < end; p++)
      m_sum += *p;
  }

  // Assigns the result to |result|
  virtual void assign_result(hola_result_t *result) {
    memcpy(&result->u.result_u64, &m_sum, sizeof(ham_u64_t));
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

  switch (db->get_key_type()) {
    case HAM_TYPE_UINT8:
      result->type = HAM_TYPE_UINT64;
      visitor.reset(new SumScanVisitor<ham_u8_t, ham_u64_t>());
      break;
    case HAM_TYPE_UINT16:
      result->type = HAM_TYPE_UINT64;
      visitor.reset(new SumScanVisitor<ham_u16_t, ham_u64_t>());
      break;
    case HAM_TYPE_UINT32:
      result->type = HAM_TYPE_UINT64;
      visitor.reset(new SumScanVisitor<ham_u32_t, ham_u64_t>());
      break;
    case HAM_TYPE_UINT64:
      result->type = HAM_TYPE_UINT64;
      visitor.reset(new SumScanVisitor<ham_u64_t, ham_u64_t>());
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

  try {
    ScopedLock lock(db->get_env()->get_mutex());
    db->scan((Transaction *)txn, visitor.get(), false);
    visitor->assign_result(result);
    return (db->set_error(0));
  }
  catch (Exception &ex) {
    return (db->set_error(ex.code));
  }
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
  virtual void operator()(const void *key_data, ham_u16_t key_size, 
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
    memcpy(&result->u.result_u64, &m_sum, sizeof(ham_u64_t));
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

  switch (db->get_key_type()) {
    case HAM_TYPE_UINT8:
      result->type = HAM_TYPE_UINT64;
      visitor.reset(new SumIfScanVisitor<ham_u8_t, ham_u64_t>(pred));
      break;
    case HAM_TYPE_UINT16:
      result->type = HAM_TYPE_UINT64;
      visitor.reset(new SumIfScanVisitor<ham_u16_t, ham_u64_t>(pred));
      break;
    case HAM_TYPE_UINT32:
      result->type = HAM_TYPE_UINT64;
      visitor.reset(new SumIfScanVisitor<ham_u32_t, ham_u64_t>(pred));
      break;
    case HAM_TYPE_UINT64:
      result->type = HAM_TYPE_UINT64;
      visitor.reset(new SumIfScanVisitor<ham_u64_t, ham_u64_t>(pred));
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

  try {
    ScopedLock lock(db->get_env()->get_mutex());
    db->scan((Transaction *)txn, visitor.get(), false);
    visitor->assign_result(result);
    return (db->set_error(0));
  }
  catch (Exception &ex) {
    return (db->set_error(ex.code));
  }
}
