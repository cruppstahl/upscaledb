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
 * @file upscaledb_srv.h
 * @brief Header file for the upscaledb network server.
 * @author Christoph Rupp, chris@crupp.de
 *
 */

#ifndef UPS_UPSCALEDB_SRV_H
#define UPS_UPSCALEDB_SRV_H

#ifdef __cplusplus
extern "C" {
#endif

#include <ups/upscaledb.h>

/**
 * @defgroup ups_server upscaledb Embedded Server
 * @{
 */

/**
 * A configuration structure
 *
 * It is always recommended to initialize the full structure with zeroes
 * before using it.
 */
typedef struct {
  /** The server port */
  uint16_t port;

  /** Only accept incoming connections from this address; set to NULL to
   * accept from all endpoints */
  const char *bind_addr;

  /* Path of the access log, or NULL if no log should be written 
   * - currently NOT USED! */
  const char *access_log_path;

  /** Path of the error log, or NULL if no log should be written
   * - currently NOT USED! */
  const char *error_log_path;

} ups_srv_config_t;

/**
 * A server handle
 */
struct ups_srv_t;
typedef struct ups_srv_t ups_srv_t;

/**
 * Initialize the server
 *
 * This function initializes a ups_srv_t handle and starts the upscaledb
 * database server on the port specified in the configuration object.
 *
 * @param config A configuration structure
 * @param srv A pointer to a ups_srv_t pointer; will be allocated
 *    if this function returns successfully
 *
 * @return UPS_SUCCESS on success
 * @return UPS_OUT_OF_MEMORY if memory could not be allocated
 */
extern ups_status_t
ups_srv_init(ups_srv_config_t *config, ups_srv_t **srv);

/**
 * Add a upscaledb Environment
 *
 * This function adds a new upscaledb Environment to the server. The
 * Environment has to be initialized properly by the caller. It will be
 * served at ups://localhost:port/urlname, where @a port was specified
 * for @ref ups_srv_init and @a urlname is the third parameter to this
 * function.
 *
 * A client accessing this Environment will specify this URL as a filename,
 * and upscaledb will transparently connect to this server.
 *
 * @param srv A valid ups_srv_t handle
 * @param env A valid upscaledb Environment handle
 * @param urlname URL of this Environment
 *
 * @return UPS_SUCCESS on success
 * @return UPS_LIMITS_REACHED if more than the max. number of Environments
 *    were added (default limit: 128)
 */
extern ups_status_t
ups_srv_add_env(ups_srv_t *srv, ups_env_t *env, const char *urlname);

/**
 * Removes an upscaledb Environment
 *
 * This function removes an upscaledb Environment from the server. It can
 * then no longer be accessed by the clients.
 *
 * @param srv A valid ups_srv_t handle
 * @param env A valid upscaledb Environment handle
 *
 * @return UPS_SUCCESS on success
 */
extern ups_status_t
ups_srv_remove_env(ups_srv_t *srv, ups_env_t *env);

/*
 * Release memory and clean up
 *
 * @param srv A valid ups_srv_t handle
 *
 * @warning
 * This function will not close open handles (i.e. of Databases, Cursors
 * or Txns). The caller has to close the remaining Environment
 * handles (@see ups_env_close).
 */
extern void
ups_srv_close(ups_srv_t *srv);

/**
 * @}
 */


#ifdef __cplusplus
} // extern "C"
#endif

#endif /* UPS_UPSCALEDB_SRV_H */
