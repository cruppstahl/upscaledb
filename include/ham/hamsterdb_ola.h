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

/**
 * @file hamsterdb_hola.h
 * @brief Include file for hamsterdb OnLine Analytical functions
 * @author Christoph Rupp, chris@crupp.de
 * @version 2.1.10
 *
 * This API is EXPERIMENTAL!! The interface is not yet stable.
 */

#ifndef HAM_HAMSTERDB_OLA_H
#define HAM_HAMSTERDB_OLA_H

#include <ham/hamsterdb.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * A predicate function with context parameters returning a bool value.
 *
 * The predicate function is applied to various analytical functions
 * of this API and is generally used to select keys where a predicate applies.
 */
typedef struct {
  /** A function pointer; receives a key, returns a bool */
  ham_bool_t (*predicate_func)(const void *key_data, uint16_t key_size,
                  void *context);

  /** User-supplied context data */
  void *context;

} hola_bool_predicate_t;


/**
 * A structure which returns the result of an operation.
 *
 * For now, the result is either a @a uint64_t counter or a @a double value.
 * The @a type parameter specifies which one is used; @a type's value is
 * one of @a HAM_TYPE_UINT64 or @a HAM_TYPE_REAL64.
 */
typedef struct {
  union {
    /** The result as a 64bit unsigned integer */
    uint64_t result_u64;

    /** The result as a 64bit real */
    double result_double;
  } u; 

  /** The actual type in the union - one of the @a HAM_TYPE_* macros */
  int type;

} hola_result_t;


/**
 * Counts the keys in a Database
 *
 * This is a non-distinct count. If the Database has duplicate keys then
 * they are included in the count.
 *
 * The actual count is returned in @a result->u.result_u64. @a result->type
 * is set to @a HAM_TYPE_U64.
 *
 * @return @ref HAM_SUCCESS upon success
 * @return @ref HAM_INV_PARAMETER if one of the parameters is NULL
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
hola_count(ham_db_t *db, ham_txn_t *txn, hola_result_t *result);

/**
 * Selectively counts the keys in a Database
 *
 * This is a non-distinct count. If the Database has duplicate keys then
 * they are included in the count. The predicate function is applied to
 * each key. If it returns true then the key (and its duplicates) is included
 * in the count; otherwise the key is ignored.
 *
 * The actual count is returned in @a result->u.result_u64. @a result->type
 * is set to @a HAM_TYPE_U64.
 *
 * @return @ref HAM_SUCCESS upon success
 * @return @ref HAM_INV_PARAMETER if one of the parameters is NULL
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
hola_count_if(ham_db_t *db, ham_txn_t *txn, hola_bool_predicate_t *pred,
                hola_result_t *result);

/**
 * Counts the distinct keys in a Database
 *
 * This is a distinct count. If the Database has duplicate keys then
 * they are not included in the count.
 *
 * The actual count is returned in @a result->u.result_u64. @a result->type
 * is set to @a HAM_TYPE_U64.
 *
 * @return @ref HAM_SUCCESS upon success
 * @return @ref HAM_INV_PARAMETER if one of the parameters is NULL
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
hola_count_distinct(ham_db_t *db, ham_txn_t *txn, hola_result_t *result);

/**
 * Selectively counts the distinct keys in a Database
 *
 * This is a distinct count. If the Database has duplicate keys then
 * they are not included in the count. The predicate function is applied to
 * each key. If it returns true then the key is included in the count;
 * otherwise the key is ignored.
 *
 * The actual count is returned in @a result->u.result_u64. @a result->type
 * is set to @a HAM_TYPE_U64.
 *
 * @return @ref HAM_SUCCESS upon success
 * @return @ref HAM_INV_PARAMETER if one of the parameters is NULL
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
hola_count_distinct_if(ham_db_t *db, ham_txn_t *txn,
                hola_bool_predicate_t *pred, hola_result_t *result);

/**
 * Calculates the average of all keys.
 *
 * This is a non-distinct function and includes all duplicate keys.
 *
 * Internally, a 64bit counter is used for the calculation. This function
 * does not protect against an overflow of this counter.
 *
 * The keys in the database (@a db) have to be numeric, which means that
 * the Database's type must be one of @a HAM_TYPE_UINT8, @a HAM_TYPE_UINT16,
 * HAM_TYPE_UINT32, @a HAM_TYPE_UINT64, @a HAM_TYPE_REAL32 or
 * @a HAM_TYPE_REAL64.
 *
 * The actual result is returned in @a result->u.result_u64 or
 * @a result->u.result_double, depending on the Database's configuration.
 *
 * @return @ref HAM_SUCCESS upon success
 * @return @ref HAM_INV_PARAMETER if one of the parameters is NULL
 * @return @ref HAM_INV_PARAMETER if the database is not numeric
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
hola_average(ham_db_t *db, ham_txn_t *txn, hola_result_t *result);

/**
 * Calculates the average of all keys where a predicate applies.
 *
 * This is a non-distinct function and includes all duplicate keys for which
 * the predicate function returns true.
 *
 * Internally, a 64bit counter is used for the calculation. This function
 * does not protect against an overflow of this counter.
 *
 * The keys in the database (@a db) have to be numeric, which means that
 * the Database's type must be one of @a HAM_TYPE_UINT8, @a HAM_TYPE_UINT16,
 * HAM_TYPE_UINT32, @a HAM_TYPE_UINT64, @a HAM_TYPE_REAL32 or
 * @a HAM_TYPE_REAL64.
 *
 * The actual result is returned in @a result->u.result_u64 or
 * @a result->u.result_double, depending on the Database's configuration.
 *
 * @return @ref HAM_SUCCESS upon success
 * @return @ref HAM_INV_PARAMETER if one of the parameters is NULL
 * @return @ref HAM_INV_PARAMETER if the database is not numeric
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
hola_average_if(ham_db_t *db, ham_txn_t *txn, hola_bool_predicate_t *pred,
                hola_result_t *result);

/**
 * Calculates the sum of all keys.
 *
 * This is a non-distinct function and includes all duplicate keys.
 *
 * Internally, a 64bit counter is used for the calculation. This function
 * does not protect against an overflow of this counter.
 *
 * The keys in the database (@a db) have to be numeric, which means that
 * the Database's type must be one of @a HAM_TYPE_UINT8, @a HAM_TYPE_UINT16,
 * HAM_TYPE_UINT32, @a HAM_TYPE_UINT64, @a HAM_TYPE_REAL32 or
 * @a HAM_TYPE_REAL64.
 *
 * The actual result is returned in @a result->u.result_u64 or
 * @a result->u.result_double, depending on the Database's configuration.
 *
 * @return @ref HAM_SUCCESS upon success
 * @return @ref HAM_INV_PARAMETER if one of the parameters is NULL
 * @return @ref HAM_INV_PARAMETER if the database is not numeric
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
hola_sum(ham_db_t *db, ham_txn_t *txn, hola_result_t *result);

/**
 * Calculates the sum of all keys where a predicate applies.
 *
 * This is a non-distinct function and includes all duplicate keys for which
 * the predicate function returns true.
 *
 * Internally, a 64bit counter is used for the calculation. This function
 * does not protect against an overflow of this counter.
 *
 * The keys in the database (@a db) have to be numeric, which means that
 * the Database's type must be one of @a HAM_TYPE_UINT8, @a HAM_TYPE_UINT16,
 * HAM_TYPE_UINT32, @a HAM_TYPE_UINT64, @a HAM_TYPE_REAL32 or
 * @a HAM_TYPE_REAL64.
 *
 * The actual result is returned in @a result->u.result_u64 or
 * @a result->u.result_double, depending on the Database's configuration.
 *
 * @return @ref HAM_SUCCESS upon success
 * @return @ref HAM_INV_PARAMETER if one of the parameters is NULL
 * @return @ref HAM_INV_PARAMETER if the database is not numeric
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
hola_sum_if(ham_db_t *db, ham_txn_t *txn, hola_bool_predicate_t *pred,
                hola_result_t *result);

/**
 * @}
 */

#ifdef __cplusplus
} // extern "C"
#endif

#endif /* HAM_HAMSTERDB_OLA_H */
