/*
 * Copyright (C) 2005-2010 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

#ifndef HAM_SRV_H__
#define HAM_SRV_H__


#ifdef __cplusplus
extern "C" {
#endif 

#include <ham/hamsterdb.h>

/**
 * A configuration structure
 */
typedef struct
{
    ham_u16_t port;

} hamserver_config_t;

/**
 * A server handle
 */
struct hamserver_t;
typedef struct hamserver_t hamserver_t;

/**
 * Initialize the server
 *
 * @return true on success, false on failure
 */
extern ham_bool_t 
hamserver_init(hamserver_config_t *config, hamserver_t **srv);

/**
 * Add a hamsterdb Environment
 *
 * @return true on success, false on failure
 */
extern ham_bool_t 
hamserver_add_env(hamserver_t *srv, ham_env_t *env, const char *urlname);

/*
 * Release memory and clean up
 *
 * @warning
 * This function will not close open handles (i.e. of Databases, Cursors
 * or Transactions). The caller has to close the remaining Environment
 * handles (@see ham_env_close).
 */
extern void
hamserver_close(hamserver_t *srv);


#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_SRV_H__ */
