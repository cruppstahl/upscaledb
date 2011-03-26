/*
 * Copyright (C) 2005-2011 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

#ifndef HAM_HAMSTERDB_SRV_H__
#define HAM_HAMSTERDB_SRV_H__

#ifdef __cplusplus
extern "C" {
#endif 

#include <ham/hamsterdb.h>

/**
 * @defgroup ham_server hamsterdb Embedded Server
 * @{ 
 */

/**
 * A configuration structure
 *
 * It is always recommended to initialize the full structure with zeroes
 * before using it.
 */
typedef struct
{
    /** The server port */
    ham_u16_t port;

    /** Path of the access log, or NULL if no log should be written */
    const char *access_log_path;

    /** Path of the error log, or NULL if no log should be written */
    const char *error_log_path;

} ham_srv_config_t;

/**
 * A server handle
 */
struct ham_srv_t;
typedef struct ham_srv_t ham_srv_t;

/**
 * Initialize the server
 *
 * This function initializes a ham_srv_t handle and starts the hamsterdb
 * database server on the port specified in the configuration object.
 *
 * @param config A configuration structure
 * @param srv A pointer to a ham_srv_t pointer; will be allocated 
 *      if this function returns successfully
 *
 * @return HAM_SUCCESS on success
 * @return HAM_OUT_OF_MEMORY if memory could not be allocated
 */
extern ham_status_t 
ham_srv_init(ham_srv_config_t *config, ham_srv_t **srv);

/**
 * Add a hamsterdb Environment
 *
 * This function adds a new hamsterdb Environment to the server. The 
 * Environment has to be initialized properly by the caller. It will be
 * served at http://localhost:<port>/<urlname>, where <port> was specified
 * for @ref ham_srv_init and @ref urlname is the third parameter to this
 * function. 
 *
 * A client accessing this Environment will specify this URL as a filename,
 * and hamsterdb will transparently connect to this server.
 *
 * @param srv A valid ham_srv_t handle 
 * @param env A valid hamsterdb Environment handle
 * @param urlname URL of this Environment
 *
 * @return HAM_SUCCESS on success
 * @return HAM_LIMITS_REACHED if more than the max. number of Environments
 *      were added (default limit: 128)
 */
extern ham_status_t 
ham_srv_add_env(ham_srv_t *srv, ham_env_t *env, const char *urlname);

/*
 * Release memory and clean up
 *
 * @param srv A valid ham_srv_t handle 
 *
 * @warning
 * This function will not close open handles (i.e. of Databases, Cursors
 * or Transactions). The caller has to close the remaining Environment
 * handles (@see ham_env_close).
 */
extern void
ham_srv_close(ham_srv_t *srv);

/**
 * @}
 */


#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_HAMSTERDB_SRV_H__ */
