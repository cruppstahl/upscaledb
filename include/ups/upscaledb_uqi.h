/*
 * Copyright (C) 2005-2016 Christoph Rupp (chris@crupp.de).
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
 * @file upscaledb_uqi.h
 * @brief Include file for upscaledb Query Interface
 * @author Christoph Rupp, chris@crupp.de
 * @version 2.1.13
 *
 * This API is EXPERIMENTAL!! The interface is not yet stable.
 */

#ifndef UPS_UPSCALEDB_UQI_H
#define UPS_UPSCALEDB_UQI_H

#include <ups/upscaledb.h>

#ifdef __cplusplus
extern "C" {
#endif

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
 * The plugins are stateless and threadsafe. However, the "init" function is
 * called prior to the actual usage, and it can allocate (and return) a
 * state variable.
 *
 * |type| is the key type specified by the user (i.e. @a UPS_TYPE_UINT32),
 * |size| is the specified key size
 */
typedef void *(*uqi_plugin_init_function)(int type, uint16_t size,
                    const char *reserved);

/** Cleans up the state variable and can release resources */
typedef void (*uqi_plugin_cleanup_function)(void *state);

/** Performs the actual aggregation on a single value */
typedef void (*uqi_plugin_aggregate_single_function)(void *state,
                    const void *data, uint16_t size,
                    size_t duplicate_count);

/** Performs the actual aggregation on a list of values */
typedef void (*uqi_plugin_aggregate_many_function)(void *state,
                    const void *data_list, size_t list_length);

/**
 * Predicate function; returns true if the value matches the predicate,
 * otherwise false
 */
typedef bool (*uqi_plugin_predicate_function)(void *state,
                    const void *data, uint16_t size);

/** Assigns the results to an @a uqi_result_t structure */
typedef void (*uqi_plugin_result_function)(void *state, uqi_result_t *result);

/** Describes a plugin for predicates; see below */
#define UQI_PLUGIN_PREDICATE            1

/** Describes a plugin for aggregation; see below */
#define UQI_PLUGIN_AGGREGATE            2

/**
 * A plugin descriptor. Describes the implementation of a user-supplied
 * aggregation or predicate function and can be loaded dynamically from
 * an external library.
 *
 * Plugins can be loaded dynamically from a library (.DLL/.SO etc) by
 * specifying a function name in a query string, i.e.
 *
 *     foo@path/to/library.dll
 *
 *  or
 *
 *     foo@library.so
 *
 * The library name can be either an absolute path or a (relative) file name,
 * in the latter case the system's library directories will be searched
 * for the file. The library can be ommitted if the plugin was registered
 * with @a uqi_register_plugin. 
 *
 * After the file is loaded, a function with the following interface is
 * invoked:
 *
 *      uqi_plugin_t *plugin_descriptor(const char *name);
 *
 * The parameter |name| is "foo" in our example. The function
 * |plugin_descriptor| must be an exported symbol with the "C"
 * calling convention.
 *
 */
typedef struct {
  /** The name of this plugin */
  const char *name;

  /**
   * The type of this plugin - either @a UQI_PLUGIN_PREDICATE or
   * @a UQI_PLUGIN_AGGREGATE
   */
  int type;

  /** The version of the plugin's interface; always set to 0 */
  int plugin_version;

  /** The initialization function; can be null */
  uqi_plugin_init_function init;

  /** The de-initialization function; can be null */
  uqi_plugin_cleanup_function cleanup;

  /**
   * The aggregation function; must be implemented if
   * @a type is @a UQI_PLUGIN_AGGREGATE, otherwise set to null
   */
  uqi_plugin_aggregate_single_function agg_single;

  /**
   * The aggregation function; must be implemented if
   * @a type is @a UQI_PLUGIN_AGGREGATE, otherwise set to null
   */
  uqi_plugin_aggregate_many_function agg_many;

  /**
   * The predicate function; must be implemented if
   * @a type is @a UQI_PLUGIN_PREDICATE, otherwise set to null
   */
  uqi_plugin_predicate_function pred;

  /** Assigns the result to a @a uqi_result_t structure; must not be null */
  uqi_plugin_result_function results;

} uqi_plugin_t;


/**
 * Manually registers a UQI plugiRegisters a UQI plugin
 *
 * This is the pro-active alternative to exporting a plugin descriptor
 * from a dynamic library. Use this if your plugin is linked statically
 * into your application.
 *
 * @return @ref UPS_SUCCESS upon success
 * @return @ref UPS_PLUGIN_ALREADY_EXISTS if a plugin with this name already
 *      exists
 * @return @ref UPS_INV_PARAMETER if any of the pointers is null
 */
UPS_EXPORT ups_status_t UPS_CALLCONV
uqi_register_plugin(uqi_plugin_t *descriptor);

/**
 * Performs a "UQI Select" query.
 *
 * See below for a description of the query syntax. In short, this function
 * can execute aggregation functions like SUM, AVERAGE or COUNT over a
 * database. The result is returned in @a result.
 *
 * @return UPS_PLUGIN_NOT_FOUND The specified function is not available
 * @return UPS_PARSER_ERROR Failed to parse the @a query string
 */
UPS_EXPORT ups_status_t UPS_CALLCONV
uqi_select(ups_env_t *env, const char *query, uqi_result_t *result);

/**
 * Performs a paginated "UQI Select" query.
 *
 * This function is similar to @a uqi_db_select, but uses two cursors for
 * specifying the range of the data. Make sure that both cursors are
 * operating on the same database as the query. Both cursors are optional;
 * if @a begin is null then the query will start at the first element (with
 * the lowest key) in the database. If @a end is null then it will run till
 * the last element of the database.
 *
 * If @a begin is not null then it will be moved to the first key behind the
 * processed range.
 *
 * If the specified Database is not yet opened, it will be reopened in
 * background and immediately closed again after the query. Closing the
 * Database can hurt performance. To avoid this, manually open the
 * Database (see @a ups_env_open_db) prior to the query. The
 * @a uqi_select_range method will then re-use the existing Database handle.
 *
 * @return UPS_PLUGIN_NOT_FOUND The specified function is not available
 * @return UPS_PARSER_ERROR Failed to parse the @a query string
 */
UPS_EXPORT ups_status_t UPS_CALLCONV
uqi_select_range(ups_env_t *env, const char *query, ups_cursor_t **begin,
                            const ups_cursor_t *end, uqi_result_t *result);

/**
 * @}
 */

#ifdef __cplusplus
} // extern "C"
#endif

#endif /* UPS_UPSCALEDB_UQI_H */
