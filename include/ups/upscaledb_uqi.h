/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
 * All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * See the file COPYING for License information.
 */

/**
 * @file upscaledb_hola.h
 * @brief Include file for upscaledb Query Interface
 * @author Christoph Rupp, chris@crupp.de
 * @version 2.1.12
 *
 * This API is EXPERIMENTAL!! The interface is not yet stable.
 */

#ifndef UPS_UPSCALEDB_OLA_H
#define UPS_UPSCALEDB_OLA_H

#include <ups/upscaledb.h>

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
  ups_bool_t (*predicate_func)(const void *key_data, uint16_t key_size,
                  void *context);

  /** User-supplied context data */
  void *context;

} uqi_bool_predicate_t;


/**
 * A structure which returns the result of an operation.
 *
 * For now, the result is either a @a uint64_t counter or a @a double value.
 * The @a type parameter specifies which one is used; @a type's value is
 * one of @a UPS_TYPE_UINT64 or @a UPS_TYPE_REAL64.
 */
typedef struct {
  union {
    /** The result as a 64bit unsigned integer */
    uint64_t result_u64;

    /** The result as a 64bit real */
    double result_double;
  } u; 

  /** The actual type in the union - one of the @a UPS_TYPE_* macros */
  int type;

} uqi_result_t;


/**
 * Counts the keys in a Database
 *
 * This is a non-distinct count. If the Database has duplicate keys then
 * they are included in the count.
 *
 * The actual count is returned in @a result->u.result_u64. @a result->type
 * is set to @a UPS_TYPE_U64.
 *
 * @return @ref UPS_SUCCESS upon success
 * @return @ref UPS_INV_PARAMETER if one of the parameters is NULL
 */
UPS_EXPORT ups_status_t UPS_CALLCONV
uqi_count(ups_db_t *db, ups_txn_t *txn, uqi_result_t *result);

/**
 * Selectively counts the keys in a Database
 *
 * This is a non-distinct count. If the Database has duplicate keys then
 * they are included in the count. The predicate function is applied to
 * each key. If it returns true then the key (and its duplicates) is included
 * in the count; otherwise the key is ignored.
 *
 * The actual count is returned in @a result->u.result_u64. @a result->type
 * is set to @a UPS_TYPE_U64.
 *
 * @return @ref UPS_SUCCESS upon success
 * @return @ref UPS_INV_PARAMETER if one of the parameters is NULL
 */
UPS_EXPORT ups_status_t UPS_CALLCONV
uqi_count_if(ups_db_t *db, ups_txn_t *txn, uqi_bool_predicate_t *pred,
                uqi_result_t *result);

/**
 * Counts the distinct keys in a Database
 *
 * This is a distinct count. If the Database has duplicate keys then
 * they are not included in the count.
 *
 * The actual count is returned in @a result->u.result_u64. @a result->type
 * is set to @a UPS_TYPE_U64.
 *
 * @return @ref UPS_SUCCESS upon success
 * @return @ref UPS_INV_PARAMETER if one of the parameters is NULL
 */
UPS_EXPORT ups_status_t UPS_CALLCONV
uqi_count_distinct(ups_db_t *db, ups_txn_t *txn, uqi_result_t *result);

/**
 * Selectively counts the distinct keys in a Database
 *
 * This is a distinct count. If the Database has duplicate keys then
 * they are not included in the count. The predicate function is applied to
 * each key. If it returns true then the key is included in the count;
 * otherwise the key is ignored.
 *
 * The actual count is returned in @a result->u.result_u64. @a result->type
 * is set to @a UPS_TYPE_U64.
 *
 * @return @ref UPS_SUCCESS upon success
 * @return @ref UPS_INV_PARAMETER if one of the parameters is NULL
 */
UPS_EXPORT ups_status_t UPS_CALLCONV
uqi_count_distinct_if(ups_db_t *db, ups_txn_t *txn,
                uqi_bool_predicate_t *pred, uqi_result_t *result);

/**
 * Calculates the average of all keys.
 *
 * This is a non-distinct function and includes all duplicate keys.
 *
 * Internally, a 64bit counter is used for the calculation. This function
 * does not protect against an overflow of this counter.
 *
 * The keys in the database (@a db) have to be numeric, which means that
 * the Database's type must be one of @a UPS_TYPE_UINT8, @a UPS_TYPE_UINT16,
 * UPS_TYPE_UINT32, @a UPS_TYPE_UINT64, @a UPS_TYPE_REAL32 or
 * @a UPS_TYPE_REAL64.
 *
 * The actual result is returned in @a result->u.result_u64 or
 * @a result->u.result_double, depending on the Database's configuration.
 *
 * @return @ref UPS_SUCCESS upon success
 * @return @ref UPS_INV_PARAMETER if one of the parameters is NULL
 * @return @ref UPS_INV_PARAMETER if the database is not numeric
 */
UPS_EXPORT ups_status_t UPS_CALLCONV
uqi_average(ups_db_t *db, ups_txn_t *txn, uqi_result_t *result);

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
 * the Database's type must be one of @a UPS_TYPE_UINT8, @a UPS_TYPE_UINT16,
 * UPS_TYPE_UINT32, @a UPS_TYPE_UINT64, @a UPS_TYPE_REAL32 or
 * @a UPS_TYPE_REAL64.
 *
 * The actual result is returned in @a result->u.result_u64 or
 * @a result->u.result_double, depending on the Database's configuration.
 *
 * @return @ref UPS_SUCCESS upon success
 * @return @ref UPS_INV_PARAMETER if one of the parameters is NULL
 * @return @ref UPS_INV_PARAMETER if the database is not numeric
 */
UPS_EXPORT ups_status_t UPS_CALLCONV
uqi_average_if(ups_db_t *db, ups_txn_t *txn, uqi_bool_predicate_t *pred,
                uqi_result_t *result);

/**
 * Calculates the sum of all keys.
 *
 * This is a non-distinct function and includes all duplicate keys.
 *
 * Internally, a 64bit counter is used for the calculation. This function
 * does not protect against an overflow of this counter.
 *
 * The keys in the database (@a db) have to be numeric, which means that
 * the Database's type must be one of @a UPS_TYPE_UINT8, @a UPS_TYPE_UINT16,
 * UPS_TYPE_UINT32, @a UPS_TYPE_UINT64, @a UPS_TYPE_REAL32 or
 * @a UPS_TYPE_REAL64.
 *
 * The actual result is returned in @a result->u.result_u64 or
 * @a result->u.result_double, depending on the Database's configuration.
 *
 * @return @ref UPS_SUCCESS upon success
 * @return @ref UPS_INV_PARAMETER if one of the parameters is NULL
 * @return @ref UPS_INV_PARAMETER if the database is not numeric
 */
UPS_EXPORT ups_status_t UPS_CALLCONV
uqi_sum(ups_db_t *db, ups_txn_t *txn, uqi_result_t *result);

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
 * the Database's type must be one of @a UPS_TYPE_UINT8, @a UPS_TYPE_UINT16,
 * UPS_TYPE_UINT32, @a UPS_TYPE_UINT64, @a UPS_TYPE_REAL32 or
 * @a UPS_TYPE_REAL64.
 *
 * The actual result is returned in @a result->u.result_u64 or
 * @a result->u.result_double, depending on the Database's configuration.
 *
 * @return @ref UPS_SUCCESS upon success
 * @return @ref UPS_INV_PARAMETER if one of the parameters is NULL
 * @return @ref UPS_INV_PARAMETER if the database is not numeric
 */
UPS_EXPORT ups_status_t UPS_CALLCONV
uqi_sum_if(ups_db_t *db, ups_txn_t *txn, uqi_bool_predicate_t *pred,
                uqi_result_t *result);

/**
 * @}
 */

#ifdef __cplusplus
} // extern "C"
#endif

#endif /* UPS_UPSCALEDB_OLA_H */
