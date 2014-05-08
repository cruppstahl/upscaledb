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
hola_count(ham_db_t *db, ham_txn_t *txn, hola_result_t *result)
{
  if (!db) {
    ham_trace(("parameter 'db' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }
  if (!result) {
    ham_trace(("parameter 'result' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  result->type = HAM_TYPE_UINT64;
  result->u.result_u64 = 0;

  return (ham_db_get_key_count(db, txn, 0, &result->u.result_u64));
}

ham_status_t HAM_CALLCONV
hola_count_if(ham_db_t *db, ham_txn_t *txn, hola_bool_predicate_t *pred,
                hola_result_t *result)
{
  if (!db) {
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

  // TODO TODO TODO

  result->type = HAM_TYPE_UINT64;
  result->u.result_u64 = 0;
  return (0);
}

ham_status_t HAM_CALLCONV
hola_count_distinct(ham_db_t *db, ham_txn_t *txn, hola_result_t *result)
{
  if (!db) {
    ham_trace(("parameter 'db' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }
  if (!result) {
    ham_trace(("parameter 'result' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  result->type = HAM_TYPE_UINT64;
  result->u.result_u64 = 0;

  return (ham_db_get_key_count(db, txn, HAM_SKIP_DUPLICATES,
                          &result->u.result_u64));
}

ham_status_t HAM_CALLCONV
hola_count_distinct_if(ham_db_t *db, ham_txn_t *txn,
                hola_bool_predicate_t *pred, hola_result_t *result)
{
  if (!db) {
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

  // TODO TODO TODO

  result->type = HAM_TYPE_UINT64;
  result->u.result_u64 = 0;
  return (0);
}

ham_status_t HAM_CALLCONV
hola_average(ham_db_t *db, ham_txn_t *txn, hola_result_t *result)
{
  if (!db) {
    ham_trace(("parameter 'db' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }
  if (!result) {
    ham_trace(("parameter 'result' must not be NULL"));
    return (HAM_INV_PARAMETER);
  }

  // TODO TODO TODO

  result->type = HAM_TYPE_UINT64;
  result->u.result_u64 = 0;
  return (0);
}

ham_status_t HAM_CALLCONV
hola_average_if(ham_db_t *db, ham_txn_t *txn, hola_bool_predicate_t *pred,
                hola_result_t *result)
{
  if (!db) {
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

  // TODO TODO TODO

  result->type = HAM_TYPE_UINT64;
  result->u.result_u64 = 0;
  return (0);
}

template<typename PodType, typename ResultType>
struct SumScanVisitor : public ScanVisitor {
  SumScanVisitor()
    : m_sum(0) {
  }

  virtual void operator()(const void *key_data, ham_u16_t key_size, 
                  ham_u32_t duplicate_count) {
    ham_assert(key_size == sizeof(PodType));
    m_sum += *(const PodType *)key_data * duplicate_count;
  }

  virtual void operator()(const void *key_data, ham_u32_t key_count) {
    const PodType *p = (const PodType *)key_data;
    const PodType *end = &p[key_count];
    for (; p < end; p++)
      m_sum += *p;
  }

  virtual void assign_result(hola_result_t *result) {
    memcpy(&result->u.result_u64, &m_sum, sizeof(ham_u64_t));
  }

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

  ScanVisitor *visitor = 0;
  result->u.result_u64 = 0;

  // TODO where is the switch to remote?
  LocalDatabase *db = (LocalDatabase *)hdb;

  switch (db->get_key_type()) {
    case HAM_TYPE_UINT8:
      result->type = HAM_TYPE_UINT64;
      visitor = new SumScanVisitor<ham_u8_t, ham_u64_t>();
      break;
    case HAM_TYPE_UINT16:
      result->type = HAM_TYPE_UINT64;
      visitor = new SumScanVisitor<ham_u16_t, ham_u64_t>();
      break;
    case HAM_TYPE_UINT32:
      result->type = HAM_TYPE_UINT64;
      visitor = new SumScanVisitor<ham_u32_t, ham_u64_t>();
      break;
    case HAM_TYPE_UINT64:
      result->type = HAM_TYPE_UINT64;
      visitor = new SumScanVisitor<ham_u64_t, ham_u64_t>();
      break;
    case HAM_TYPE_REAL32:
      result->type = HAM_TYPE_REAL64;
      visitor = new SumScanVisitor<float, double>();
      break;
    case HAM_TYPE_REAL64:
      result->type = HAM_TYPE_REAL64;
      visitor = new SumScanVisitor<double, double>();
      break;
    default:
      ham_trace(("hola_sum* can only be applied to numerical data"));
      return (HAM_INV_PARAMETER);
  }

  db->scan((Transaction *)txn, visitor, false);

  visitor->assign_result(result);
  delete visitor;
  return (0);
}

ham_status_t HAM_CALLCONV
hola_sum_if(ham_db_t *db, ham_txn_t *txn, hola_bool_predicate_t *pred,
                hola_result_t *result)
{
  if (!db) {
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

  // TODO TODO TODO

  result->type = HAM_TYPE_UINT64;
  result->u.result_u64 = 0;
  return (0);
}
