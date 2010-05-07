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
#include "mem.h"
#include "messages.pb-c.h"

#if HAM_ENABLE_REMOTE

#include <curl/curl.h>
#include <curl/easy.h>

typedef struct curl_buffer_t
{
    ham_size_t packed_size;
    ham_u8_t *packed_data;
    ham_size_t offset;
} curl_buffer_t;

static size_t
__writefunc(void *buffer, size_t size, size_t nmemb, void *ptr)
{
    curl_buffer_t *buf=(curl_buffer_t *)ptr;
    (void)buf;
    printf("XXX writing %d/%d bytes\n", size, nmemb);
    return size*nmemb;
}

static size_t
__readfunc(char *buffer, size_t size, size_t nmemb, void *ptr)
{
    curl_buffer_t *buf=(curl_buffer_t *)ptr;

    ham_assert(nmemb>=buf->packed_size, (""));
    if (buf->offset==buf->packed_size)
        return (0);

    ham_assert(buf->offset==0, (""));

    printf("XXX reading %d/%d\n", size, nmemb);
    memcpy(buffer, buf->packed_data, buf->packed_size);
    buf->offset=buf->packed_size;
    return (buf->packed_size);
}

#define SETOPT(curl, opt, val)                                                \
                    if ((cc=curl_easy_setopt(curl, opt, val))) {              \
                        ham_trace(("curl_easy_setopt failed: %d/%s", cc,      \
                                    curl_easy_strerror(cc)));                 \
                        return (HAM_INTERNAL_ERROR);                          \
                    }

static ham_status_t
_perform_request(ham_env_t *env, CURL *handle, Ham__Wrapper *wrapper)
{
    CURLcode cc;
    long response=0;
    char header[128];
    curl_buffer_t buf={0};
    struct curl_slist *slist=0;

    buf.packed_size=ham__wrapper__get_packed_size(wrapper);
    buf.packed_data=allocator_alloc(env_get_allocator(env), buf.packed_size);
    if (!buf.packed_data)
        return (HAM_OUT_OF_MEMORY);
    ham__wrapper__pack(wrapper, buf.packed_data);

    sprintf(header, "Content-Length: %u", buf.packed_size);
    slist=curl_slist_append(slist, header);
    slist=curl_slist_append(slist, "Transfer-Encoding:");
    slist=curl_slist_append(slist, "Expect:");

    SETOPT(handle, CURLOPT_VERBOSE, 1);
    SETOPT(handle, CURLOPT_URL, env_get_filename(env));
    SETOPT(handle, CURLOPT_READFUNCTION, __readfunc);
    SETOPT(handle, CURLOPT_READDATA, &buf);
    SETOPT(handle, CURLOPT_UPLOAD, 1);
    SETOPT(handle, CURLOPT_PUT, 1);
    SETOPT(handle, CURLOPT_WRITEFUNCTION, __writefunc);
    SETOPT(handle, CURLOPT_WRITEDATA, &buf);
    SETOPT(handle, CURLOPT_HTTPHEADER, slist);

    cc=curl_easy_perform(handle);

    allocator_free(env_get_allocator(env), buf.packed_data);
    curl_slist_free_all(slist);

    if (cc) {
        ham_trace(("network transmission failed: %s", curl_easy_strerror(cc)));
        return (HAM_NETWORK_ERROR);
    }

    cc=curl_easy_getinfo(handle, CURLINFO_RESPONSE_CODE, &response);
    if (cc) {
        ham_trace(("network transmission failed: %s", curl_easy_strerror(cc)));
        return (HAM_NETWORK_ERROR);
    }

    if (response!=200) {
        ham_trace(("server returned error %u", response));
        return (HAM_NETWORK_ERROR);
    }

    return (0);
}

static ham_status_t 
_remote_fun_create(ham_env_t *env, const char *filename,
            ham_u32_t flags, ham_u32_t mode, const ham_parameter_t *param)
{
    ham_status_t st;
    Ham__ConnectRequest msg;
    Ham__Wrapper wrapper;
    CURL *handle=curl_easy_init();

    ham__wrapper__init(&wrapper);
    ham__connect_request__init(&msg);
    msg.id=1;
    msg.path=(char *)filename;
    wrapper.type=HAM__WRAPPER__TYPE__CONNECT_REQUEST;
    wrapper.connect_request=&msg;

    st=_perform_request(env, handle, &wrapper);
    if (st)
        return (st);

    env_set_curl(env, handle);

    return (0);
}

static ham_status_t 
_remote_fun_open(ham_env_t *env, const char *filename, ham_u32_t flags, 
        const ham_parameter_t *param)
{
    ham_status_t st;
    Ham__ConnectRequest msg;
    Ham__Wrapper wrapper;
    CURL *handle=curl_easy_init();

    ham__wrapper__init(&wrapper);
    ham__connect_request__init(&msg);
    msg.id=1;
    msg.path=(char *)filename;
    wrapper.type=HAM__WRAPPER__TYPE__CONNECT_REQUEST;
    wrapper.connect_request=&msg;

    st=_perform_request(env, handle, &wrapper);
    if (st)
        return (st);

    env_set_curl(env, handle);

    return (0);
}

static ham_status_t
_remote_fun_rename_db(ham_env_t *env, ham_u16_t oldname, 
                ham_u16_t newname, ham_u32_t flags)
{
    return (HAM_NOT_IMPLEMENTED);
}

static ham_status_t
_remote_fun_erase_db(ham_env_t *env, ham_u16_t name, ham_u32_t flags)
{
    return (HAM_NOT_IMPLEMENTED);
}

static ham_status_t
_remote_fun_get_database_names(ham_env_t *env, ham_u16_t *names, 
            ham_size_t *count)
{
    return (HAM_NOT_IMPLEMENTED);
}

static ham_status_t
_remote_fun_close(ham_env_t *env, ham_u32_t flags)
{
    (void)flags;

    if (env_get_curl(env)) {
        curl_easy_cleanup(env_get_curl(env));
        env_set_curl(env, 0);
    }

    return (0);
}

static ham_status_t 
_remote_fun_get_parameters(ham_env_t *env, ham_parameter_t *param)
{
    ham_status_t st;
    Ham__EnvGetParametersRequest msg;
    Ham__Wrapper wrapper;
    ham_size_t i=0, num_names=0;
    ham_u32_t *names;
    ham_parameter_t *p;
    
    /* count number of parameters */
    p=param;
    if (p) {
        for (; p->name; p++) {
            num_names++;
        }
    }

    /* allocate a memory and copy the parameter names */
    names=(ham_u32_t *)allocator_alloc(env_get_allocator(env), 
            num_names*sizeof(ham_u32_t));
    if (!names)
        return (HAM_OUT_OF_MEMORY);
    p=param;
    if (p) {
        for (; p->name; p++) {
            names[i]=p->name;
            i++;
        }
    }

    ham__wrapper__init(&wrapper);
    ham__env_get_parameters_request__init(&msg);
    msg.id=1;
    msg.names=(int *)&names[0];
    msg.n_names=num_names;
    wrapper.type=HAM__WRAPPER__TYPE__ENV_GET_PARAMETERS_REQUEST;
    wrapper.env_get_parameters_request=&msg;

    st=_perform_request(env, env_get_curl(env), &wrapper);
    if (st)
        return (st);

    return (0);
}

static ham_status_t
_remote_fun_flush(ham_env_t *env, ham_u32_t flags)
{
    return (HAM_NOT_IMPLEMENTED);
}

#endif /* HAM_ENABLE_REMOTE */

ham_status_t
env_initialize_remote(ham_env_t *env)
{
#if HAM_ENABLE_REMOTE
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

