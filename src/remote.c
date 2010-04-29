/**
 * Copyright (C) 2005-2008 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 *
 */

#include "config.h"

#include <string.h>

#include "db.h"
#include "env.h"

#if HAM_ENABLE_REMOTE

#include <curl/curl.h>

#define env_set_curl(env, c)    env_set_device(env, (ham_device_t *)c)

static ham_status_t 
_remote_fun_create(ham_env_t *env, const char *filename,
            ham_u32_t flags, ham_u32_t mode, const ham_parameter_t *param)
{
    CURL *handle=curl_easy_init();
    curl_easy_setopt(handle, CURLOPT_URL, filename);

    /* TODO send packet, initialize connection and get a handle */

    env_set_curl(env, handle);

    return (0);
}

static ham_status_t 
_remote_fun_open(ham_env_t *env, const char *filename, ham_u32_t flags, 
        const ham_parameter_t *param)
{
    CURL *handle=curl_easy_init();
    curl_easy_setopt(handle, CURLOPT_URL, filename);

    /* TODO send packet, initialize connection and get a handle */

    env_set_curl(env, handle);

    return (0);
}

static ham_status_t
_remote_fun_rename_db(ham_env_t *env, ham_u16_t oldname, 
                ham_u16_t newname, ham_u32_t flags)
{
    return (0);
}

static ham_status_t
_remote_fun_erase_db(ham_env_t *env, ham_u16_t name, ham_u32_t flags)
{
    return (0);
}

static ham_status_t
_remote_fun_get_database_names(ham_env_t *env, ham_u16_t *names, 
            ham_size_t *count)
{
    return (0);
}

static ham_status_t
_remote_fun_close(ham_env_t *env, ham_u32_t flags)
{
    return (0);
}

static ham_status_t 
_remote_fun_get_parameters(ham_env_t *env, ham_parameter_t *param)
{
    return (0);
}

static ham_status_t
_remote_fun_flush(ham_env_t *env, ham_u32_t flags)
{
    return (0);
}

#endif /* HAM_ENABLE_REMOTE */

ham_status_t
env_initialize_remote(ham_env_t *env)
{
    static int initialized=0;

#if HAM_ENABLE_REMOTE
    if (!initialized) {
        initialized=1;
        curl_global_init(CURL_GLOBAL_ALL);
    }

    env->_fun_create             =_remote_fun_create;
    env->_fun_open               =_remote_fun_open;
    env->_fun_rename_db          =_remote_fun_rename_db;
    env->_fun_erase_db           =_remote_fun_erase_db;
    env->_fun_get_database_names =_remote_fun_get_database_names;
    env->_fun_get_parameters     =_remote_fun_get_parameters;
    env->_fun_flush              =_remote_fun_flush;
    env->_fun_close              =_remote_fun_close;
#else
    return (HAM_NOT_IMPLEMENTED);
#endif

    return (0);
}

