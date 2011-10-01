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
#include "txn.h"
#include "env.h"
#include "mem.h"
#include "cursor.h"

#if HAM_ENABLE_REMOTE

#define CURL_STATICLIB /* otherwise libcurl uses wrong __declspec */
#include <curl/curl.h>
#include <curl/easy.h>

#include "protocol/protocol.h"

typedef struct curl_buffer_t
{
    ham_size_t packed_size;
    ham_u8_t *packed_data;
    ham_size_t offset;
    proto_wrapper_t *wrapper;
    mem_allocator_t *alloc;
} curl_buffer_t;

static size_t
__writefunc(void *buffer, size_t size, size_t nmemb, void *ptr)
{
    curl_buffer_t *buf=(curl_buffer_t *)ptr;
    char *cbuf=(char *)buffer;
    ham_size_t payload_size=0;

    if (buf->offset==0) {
        if (*(ham_u32_t *)&cbuf[0]!=ham_db2h32(HAM_TRANSFER_MAGIC_V1)) {
            ham_trace(("invalid protocol version"));
            return (0);
        }
        payload_size=ham_h2db32(*(ham_u32_t *)&cbuf[4]);

        /* did we receive the whole data in this packet? */
        if (payload_size+8==size*nmemb) {
            buf->wrapper=proto_unpack((ham_size_t)(size*nmemb), 
                        (ham_u8_t *)&cbuf[0]);
            if (!buf->wrapper)
                return (0);
            return (size*nmemb);
        }

        /* otherwise we have to buffer the received data */
        buf->packed_size=payload_size+8;
        buf->packed_data=allocator_alloc(buf->alloc, buf->packed_size);
        if (!buf->packed_data)
            return (0);
        memcpy(buf->packed_data, &cbuf[0], size*nmemb);
        buf->offset+=(ham_size_t)(size*nmemb);
    }
    /* append to an existing buffer? */
    else {
        memcpy(buf->packed_data+buf->offset, &cbuf[0], size*nmemb);
        buf->offset+=(ham_size_t)(size*nmemb);
    }

    /* check if we've received the whole data */
    if (buf->offset==buf->packed_size) {
        buf->wrapper=proto_unpack(buf->packed_size, buf->packed_data);
        if (!buf->wrapper)
            return (0);
        allocator_free(buf->alloc, buf->packed_data);
        if (!buf->wrapper)
            return 0;
    }

    return (size*nmemb);
}

static size_t
__readfunc(char *buffer, size_t size, size_t nmemb, void *ptr)
{
    curl_buffer_t *buf=(curl_buffer_t *)ptr;
    size_t remaining=buf->packed_size-buf->offset;

    if (remaining==0)
        return (0);

    if (nmemb>remaining)
        nmemb=remaining;

    memcpy(buffer, buf->packed_data+buf->offset, nmemb);
    buf->offset+=(ham_size_t)nmemb;
    return (nmemb);
}

#define SETOPT(curl, opt, val)                                                \
                    if ((cc=curl_easy_setopt(curl, opt, val))) {              \
                        ham_trace(("curl_easy_setopt failed: %d/%s", cc,      \
                                    curl_easy_strerror(cc)));                 \
                        return (HAM_INTERNAL_ERROR);                          \
                    }

static ham_status_t
_perform_request(ham_env_t *env, CURL *handle, proto_wrapper_t *request,
                proto_wrapper_t **reply)
{
    CURLcode cc;
    long response=0;
    char header[128];
    curl_buffer_t rbuf={0};
    curl_buffer_t wbuf={0};
    struct curl_slist *slist=0;

    wbuf.alloc=env_get_allocator(env);

    *reply=0;

    if (!proto_pack(request, wbuf.alloc, &rbuf.packed_data, &rbuf.packed_size))
        return (HAM_INTERNAL_ERROR);

    sprintf(header, "Content-Length: %u", rbuf.packed_size);
    slist=curl_slist_append(slist, header);
    slist=curl_slist_append(slist, "Transfer-Encoding:");
    slist=curl_slist_append(slist, "Expect:");

#ifdef HAM_DEBUG
    SETOPT(handle, CURLOPT_VERBOSE, 1);
#endif
    SETOPT(handle, CURLOPT_URL, env_get_filename(env));
    SETOPT(handle, CURLOPT_READFUNCTION, __readfunc);
    SETOPT(handle, CURLOPT_READDATA, &rbuf);
    SETOPT(handle, CURLOPT_UPLOAD, 1);
    SETOPT(handle, CURLOPT_PUT, 1);
    SETOPT(handle, CURLOPT_WRITEFUNCTION, __writefunc);
    SETOPT(handle, CURLOPT_WRITEDATA, &wbuf);
    SETOPT(handle, CURLOPT_HTTPHEADER, slist);

    cc=curl_easy_perform(handle);

    if (rbuf.packed_data)
        allocator_free(env_get_allocator(env), rbuf.packed_data);
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

    *reply=wbuf.wrapper;

    return (0);
}

static ham_status_t 
_remote_fun_create(ham_env_t *env, const char *filename,
            ham_u32_t flags, ham_u32_t mode, const ham_parameter_t *param)
{
    ham_status_t st;
    proto_wrapper_t *request, *reply;
    CURL *handle=curl_easy_init();

    request=proto_init_connect_request(filename);

    st=_perform_request(env, handle, request, &reply);
    proto_delete(request);
    if (st) {
        curl_easy_cleanup(handle);
        if (reply)
            proto_delete(reply);
        return (st);
    }

    ham_assert(reply!=0, (""));
    ham_assert(proto_has_connect_reply(reply), (""));

    st=proto_connect_reply_get_status(reply);
    if (st==0) {
        env_set_curl(env, handle);
        env_set_rt_flags(env, 
                env_get_rt_flags(env)
                    |proto_connect_reply_get_env_flags(reply));
    }

    proto_delete(reply);

    return (st);
}

static ham_status_t 
_remote_fun_open(ham_env_t *env, const char *filename, ham_u32_t flags, 
        const ham_parameter_t *param)
{
    ham_status_t st;
    proto_wrapper_t *request, *reply;
    CURL *handle=curl_easy_init();

    request=proto_init_connect_request(filename);

    st=_perform_request(env, handle, request, &reply);
    proto_delete(request);
    if (st) {
        curl_easy_cleanup(handle);
        if (reply)
            proto_delete(reply);
        return (st);
    }

    ham_assert(reply!=0, (""));
    ham_assert(proto_has_connect_reply(reply), (""));

    st=proto_connect_reply_get_status(reply);
    if (st==0) {
        env_set_curl(env, handle);
        env_set_rt_flags(env, 
                env_get_rt_flags(env)
                    |proto_connect_reply_get_env_flags(reply));
    }

    proto_delete(reply);

    return (st);
}

static ham_status_t
_remote_fun_rename_db(ham_env_t *env, ham_u16_t oldname, 
                ham_u16_t newname, ham_u32_t flags)
{
    ham_status_t st;
    proto_wrapper_t *request, *reply;

    request=proto_init_env_rename_request(oldname, newname, flags);

    st=_perform_request(env, env_get_curl(env), request, &reply);
    proto_delete(request);
    if (st) {
        if (reply)
            proto_delete(reply);
        return (st);
    }

    ham_assert(reply!=0, (""));
    ham_assert(proto_has_env_rename_reply(reply), (""));

    st=proto_env_rename_reply_get_status(reply);

    proto_delete(reply);

    return (st);
}

static ham_status_t
_remote_fun_erase_db(ham_env_t *env, ham_u16_t name, ham_u32_t flags)
{
    ham_status_t st;
    proto_wrapper_t *request, *reply;
    
    request=proto_init_env_erase_db_request(name, flags);

    st=_perform_request(env, env_get_curl(env), request, &reply);
    proto_delete(request);
    if (st) {
        if (reply)
            proto_delete(reply);
        return (st);
    }

    ham_assert(reply!=0, (""));
    ham_assert(proto_has_env_erase_db_reply(reply), (""));

    st=proto_env_erase_db_reply_get_status(reply);

    proto_delete(reply);

    return (st);
}

static ham_status_t
_remote_fun_get_database_names(ham_env_t *env, ham_u16_t *names, 
            ham_size_t *count)
{
    ham_status_t st;
    ham_size_t i;
    proto_wrapper_t *request, *reply;

    request=proto_init_env_get_database_names_request();

    st=_perform_request(env, env_get_curl(env), request, &reply);
    proto_delete(request);
    if (st) {
        if (reply)
            proto_delete(reply);
        return (st);
    }

    ham_assert(reply!=0, (""));
    ham_assert(proto_has_env_get_database_names_reply(reply), (""));

    st=proto_env_get_database_names_reply_get_status(reply);
    if (st) {
        proto_delete(reply);
        return (st);
    }

    /* copy the retrieved names */
    for (i=0; i<proto_env_get_database_names_reply_get_names_size(reply)
            && i<*count; i++) {
        names[i]=proto_env_get_database_names_reply_get_names(reply)[i];
    }

    *count=i;

    proto_delete(reply);

    return (0);
}

static ham_status_t 
_remote_fun_env_get_parameters(ham_env_t *env, ham_parameter_t *param)
{
    static char filename[1024];
    ham_status_t st;
    proto_wrapper_t *request, *reply;
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
        for (i=0; p->name; p++) {
            names[i]=p->name;
            i++;
        }
    }

    request=proto_init_env_get_parameters_request(names, num_names);

    st=_perform_request(env, env_get_curl(env), request, &reply);
    proto_delete(request);

    allocator_free(env_get_allocator(env), names);

    if (st) {
        if (reply)
            proto_delete(reply);
        return (st);
    }

    ham_assert(reply!=0, (""));
    ham_assert(proto_has_env_get_parameters_reply(reply), (""));

    st=proto_env_get_parameters_reply_get_status(reply);
    if (st) {
        proto_delete(reply);
        return (st);
    }

    p=param;
    while (p && p->name) {
        switch (p->name) {
        case HAM_PARAM_CACHESIZE:
            ham_assert(proto_env_get_parameters_reply_has_cachesize(reply), (""));
            p->value=proto_env_get_parameters_reply_get_cachesize(reply);
            break;
        case HAM_PARAM_PAGESIZE:
            ham_assert(proto_env_get_parameters_reply_has_pagesize(reply), (""));
            p->value=proto_env_get_parameters_reply_get_pagesize(reply);
            break;
        case HAM_PARAM_MAX_ENV_DATABASES:
            ham_assert(proto_env_get_parameters_reply_has_max_env_databases(reply), (""));
            p->value=proto_env_get_parameters_reply_get_max_env_databases(reply);
            break;
        case HAM_PARAM_GET_FLAGS:
            ham_assert(proto_env_get_parameters_reply_has_flags(reply), (""));
            p->value=proto_env_get_parameters_reply_get_flags(reply);
            break;
        case HAM_PARAM_GET_FILEMODE:
            ham_assert(proto_env_get_parameters_reply_has_filemode(reply), (""));
            p->value=proto_env_get_parameters_reply_get_filemode(reply);
            break;
        case HAM_PARAM_GET_FILENAME:
            if (proto_env_get_parameters_reply_has_filename(reply)) {
                strncpy(filename, 
                        proto_env_get_parameters_reply_get_filename(reply),
                            sizeof(filename));
                p->value=PTR_TO_U64(&filename[0]);
            }
            break;
        default:
            ham_trace(("unknown parameter %d", (int)p->name));
            break;
        }
        p++;
    }

    proto_delete(reply);

    return (0);
}

static ham_status_t
_remote_fun_env_flush(ham_env_t *env, ham_u32_t flags)
{
    ham_status_t st;
    proto_wrapper_t *request, *reply;

    request=proto_init_env_flush_request(flags);

    st=_perform_request(env, env_get_curl(env), request, &reply);
    proto_delete(request);
    if (st) {
        if (reply)
            proto_delete(reply);
        return (st);
    }

    ham_assert(reply!=0, (""));
    ham_assert(proto_has_env_flush_reply(reply), (""));

    st=proto_env_flush_reply_get_status(reply);

    proto_delete(reply);

    return (st);
}

static ham_status_t 
_remote_fun_create_db(ham_env_t *env, ham_db_t *db, 
        ham_u16_t dbname, ham_u32_t flags, const ham_parameter_t *param)
{
    ham_status_t st;
    proto_wrapper_t *request, *reply;
    ham_size_t i=0, num_params=0;
    ham_u32_t *names;
    ham_u64_t *values;
    const ham_parameter_t *p;
    
    /* count number of parameters */
    p=param;
    if (p) {
        for (; p->name; p++) {
            num_params++;
        }
    }

    /* allocate a memory and copy the parameter names */
    names=(ham_u32_t *)allocator_alloc(env_get_allocator(env), 
            num_params*sizeof(ham_u32_t));
    values=(ham_u64_t *)allocator_alloc(env_get_allocator(env), 
            num_params*sizeof(ham_u64_t));
    if (!names || !values)
        return (HAM_OUT_OF_MEMORY);
    p=param;
    if (p) {
        for (; p->name; p++) {
            names[i]=p->name;
            values[i]=p->value;
            i++;
        }
    }

    request=proto_init_env_create_db_request(dbname, flags, 
                names, values, num_params);

    st=_perform_request(env, env_get_curl(env), request, &reply);
    proto_delete(request);

    allocator_free(env_get_allocator(env), names);
    allocator_free(env_get_allocator(env), values);

    if (st) {
        if (reply)
            proto_delete(reply);
        return (st);
    }

    ham_assert(reply!=0, (""));
    ham_assert(proto_has_env_create_db_reply(reply), (""));

    st=proto_env_create_db_reply_get_status(reply);
    if (st) {
        proto_delete(reply);
        return (st);
    }

    db_set_remote_handle(db, proto_env_create_db_reply_get_db_handle(reply));
    db_set_rt_flags(db, proto_env_create_db_reply_get_flags(reply));

    /*
     * store the env pointer in the database
     */
    db_set_env(db, env);

    proto_delete(reply);

    /*
     * on success: store the open database in the environment's list of
     * opened databases
     */
    db_set_next(db, env_get_list(env));
    env_set_list(env, db);

    /*
     * initialize the remaining function pointers in ham_db_t
     */
    return (db_initialize_remote(db));
}

static ham_status_t 
_remote_fun_open_db(ham_env_t *env, ham_db_t *db, 
        ham_u16_t dbname, ham_u32_t flags, const ham_parameter_t *param)
{
    ham_status_t st;
    proto_wrapper_t *request, *reply;
    ham_size_t i=0, num_params=0;
    ham_u32_t *names;
    ham_u64_t *values;
    const ham_parameter_t *p;
    
    /* count number of parameters */
    p=param;
    if (p) {
        for (; p->name; p++) {
            num_params++;
        }
    }

    /* allocate a memory and copy the parameter names */
    names=(ham_u32_t *)allocator_alloc(env_get_allocator(env), 
            num_params*sizeof(ham_u32_t));
    values=(ham_u64_t *)allocator_alloc(env_get_allocator(env), 
            num_params*sizeof(ham_u64_t));
    if (!names || !values)
        return (HAM_OUT_OF_MEMORY);
    p=param;
    if (p) {
        for (; p->name; p++) {
            names[i]=p->name;
            values[i]=p->value;
            i++;
        }
    }

    request=proto_init_env_open_db_request(dbname, flags, 
                names, values, num_params);

    st=_perform_request(env, env_get_curl(env), request, &reply);
    proto_delete(request);

    allocator_free(env_get_allocator(env), names);
    allocator_free(env_get_allocator(env), values);

    if (st) {
        if (reply)
            proto_delete(reply);
        return (st);
    }

    ham_assert(reply!=0, (""));
    ham_assert(proto_has_env_open_db_reply(reply), (""));

    st=proto_env_open_db_reply_get_status(reply);
    if (st) {
        proto_delete(reply);
        return (st);
    }

    /*
     * store the env pointer in the database
     */
    db_set_env(db, env);
    db_set_remote_handle(db, proto_env_open_db_reply_get_db_handle(reply));
    db_set_rt_flags(db, proto_env_open_db_reply_get_flags(reply));

    proto_delete(reply);

    /*
     * on success: store the open database in the environment's list of
     * opened databases
     */
    db_set_next(db, env_get_list(env));
    env_set_list(env, db);

    /*
     * initialize the remaining function pointers in ham_db_t
     */
    return (db_initialize_remote(db));
}

static ham_status_t
_remote_fun_env_close(ham_env_t *env, ham_u32_t flags)
{
    (void)flags;

    if (env_get_curl(env)) {
        curl_easy_cleanup(env_get_curl(env));
        env_set_curl(env, 0);
    }
    
    return (0);
}

static ham_status_t
_remote_fun_txn_begin(ham_env_t *env, ham_db_t *db, 
                ham_txn_t **txn, ham_u32_t flags)
{
    ham_status_t st;
    proto_wrapper_t *request, *reply;
    
    request=proto_init_txn_begin_request(db_get_remote_handle(db), flags);

    st=_perform_request(env, env_get_curl(env), request, &reply);
    proto_delete(request);
    if (st) {
        if (reply)
            proto_delete(reply);
        return (st);
    }

    ham_assert(reply!=0, (""));
    ham_assert(proto_has_txn_begin_reply(reply), (""));

    st=proto_txn_begin_reply_get_status(reply);
    if (st) {
        proto_delete(reply);
        return (st);
    }

    *txn=(ham_txn_t *)allocator_alloc(env_get_allocator(env), 
                            sizeof(ham_txn_t));
    if (!(*txn))
        return (HAM_OUT_OF_MEMORY);

    st=txn_begin(*txn, env, flags);
    if (st) {
        allocator_free(env_get_allocator(env), *txn);
        *txn=0;
    }
    else {
        txn_set_remote_handle(*txn, 
                    proto_txn_begin_reply_get_txn_handle(reply));
    }

    proto_delete(reply);

    return (st);
}

static ham_status_t
_remote_fun_txn_commit(ham_env_t *env, ham_txn_t *txn, ham_u32_t flags)
{
    ham_status_t st;
    proto_wrapper_t *request, *reply;
    
    request=proto_init_txn_commit_request(txn_get_remote_handle(txn), flags);

    st=_perform_request(env, env_get_curl(env), request, &reply);
    proto_delete(request);
    if (st) {
        if (reply)
            proto_delete(reply);
        return (st);
    }

    ham_assert(reply!=0, (""));
    ham_assert(proto_has_txn_commit_reply(reply), (""));

    st=proto_txn_commit_reply_get_status(reply);

    if (st==0) {
        memset(txn, 0, sizeof(*txn));
        allocator_free(env_get_allocator(env), txn);

        /* remove the link between env and txn */
    	env_set_txn(env, 0);
    }

    proto_delete(reply);

    return (st);
}

static ham_status_t
_remote_fun_txn_abort(ham_env_t *env, ham_txn_t *txn, ham_u32_t flags)
{
    ham_status_t st;
    proto_wrapper_t *request, *reply;

    request=proto_init_txn_abort_request(txn_get_remote_handle(txn), flags);
    
    st=_perform_request(env, env_get_curl(env), request, &reply);
    proto_delete(request);
    if (st) {
        if (reply)
            proto_delete(reply);
        return (st);
    }

    ham_assert(reply!=0, (""));
    ham_assert(proto_has_txn_abort_reply(reply), (""));

    st=proto_txn_abort_reply_get_status(reply);

    if (st==0) {
        memset(txn, 0, sizeof(*txn));
        allocator_free(env_get_allocator(env), txn);

        /* remove the link between env and txn */
    	env_set_txn(env, 0);
    }

    proto_delete(reply);

    return (st);
}

static ham_status_t
_remote_fun_close(ham_db_t *db, ham_u32_t flags)
{
    ham_status_t st;
    ham_env_t *env=db_get_env(db);
    proto_wrapper_t *request, *reply;
    
    /*
     * auto-cleanup cursors?
     */
    if (flags&HAM_AUTO_CLEANUP) {
        ham_cursor_t *cursor=db_get_cursors(db);
        while ((cursor=db_get_cursors(db))) {
            (void)ham_cursor_close(cursor);
        }
    }
    else if (db_get_cursors(db)) {
        return (HAM_CURSOR_STILL_OPEN);
    }

    request=proto_init_db_close_request(db_get_remote_handle(db), flags);

    st=_perform_request(env, env_get_curl(env), request, &reply);
    proto_delete(request);
    if (st) {
        if (reply)
            proto_delete(reply);
        return (st);
    }

    ham_assert(reply!=0, (""));
    ham_assert(proto_has_db_close_reply(reply), (""));

    st=proto_db_close_reply_get_status(reply);

    proto_delete(reply);

    if (st==0)
        db_set_remote_handle(db, 0);

    return (st);
}

static ham_status_t
_remote_fun_get_parameters(ham_db_t *db, ham_parameter_t *param)
{
    static char filename[1024];
    ham_status_t st;
    ham_env_t *env=db_get_env(db);
    proto_wrapper_t *request, *reply;
    ham_size_t i, num_names=0;
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
        for (i=0; p->name; p++) {
            names[i]=p->name;
            i++;
        }
    }

    request=proto_init_db_get_parameters_request(db_get_remote_handle(db),
                        names, num_names);

    st=_perform_request(env, env_get_curl(env), request, &reply);
    proto_delete(request);

    allocator_free(env_get_allocator(env), names);

    if (st) {
        if (reply)
            proto_delete(reply);
        return (st);
    }

    ham_assert(reply!=0, (""));
    ham_assert(proto_has_db_get_parameters_reply(reply), (""));

    st=proto_db_get_parameters_reply_get_status(reply);
    if (st) {
        proto_delete(reply);
        return (st);
    }

    p=param;
    while (p && p->name) {
        switch (p->name) {
        case HAM_PARAM_CACHESIZE:
            ham_assert(proto_db_get_parameters_reply_has_cachesize(reply), (""));
            p->value=proto_db_get_parameters_reply_get_cachesize(reply);
            break;
        case HAM_PARAM_PAGESIZE:
            ham_assert(proto_db_get_parameters_reply_has_pagesize(reply), (""));
            p->value=proto_db_get_parameters_reply_get_pagesize(reply);
            break;
        case HAM_PARAM_MAX_ENV_DATABASES:
            ham_assert(proto_db_get_parameters_reply_has_max_env_databases(reply), (""));
            p->value=proto_db_get_parameters_reply_get_max_env_databases(reply);
            break;
        case HAM_PARAM_GET_FLAGS:
            ham_assert(proto_db_get_parameters_reply_has_flags(reply), (""));
            p->value=proto_db_get_parameters_reply_get_flags(reply);
            break;
        case HAM_PARAM_GET_FILEMODE:
            ham_assert(proto_db_get_parameters_reply_has_filemode(reply), (""));
            p->value=proto_db_get_parameters_reply_get_filemode(reply);
            break;
        case HAM_PARAM_GET_FILENAME:
            ham_assert(proto_db_get_parameters_reply_has_filename(reply), (""));
            strncpy(filename, proto_db_get_parameters_reply_get_filename(reply),
                        sizeof(filename));
            p->value=PTR_TO_U64(&filename[0]);
            break;
        case HAM_PARAM_KEYSIZE:
            ham_assert(proto_db_get_parameters_reply_has_keysize(reply), (""));
            p->value=proto_db_get_parameters_reply_get_keysize(reply);
            break;
        case HAM_PARAM_GET_DATABASE_NAME:
            ham_assert(proto_db_get_parameters_reply_has_dbname(reply), (""));
            p->value=proto_db_get_parameters_reply_get_dbname(reply);
            break;
        case HAM_PARAM_GET_KEYS_PER_PAGE:
            ham_assert(proto_db_get_parameters_reply_has_keys_per_page(reply), (""));
            p->value=proto_db_get_parameters_reply_get_keys_per_page(reply);
            break;
        case HAM_PARAM_GET_DATA_ACCESS_MODE:
            ham_assert(proto_db_get_parameters_reply_has_dam(reply), (""));
            p->value=proto_db_get_parameters_reply_get_dam(reply);
            break;
        default:
            ham_trace(("unknown parameter %d", (int)p->name));
            break;
        }
        p++;
    }

    proto_delete(reply);

    return (st);
}

static ham_status_t
_remote_fun_check_integrity(ham_db_t *db, ham_txn_t *txn)
{
    ham_status_t st;
    ham_env_t *env=db_get_env(db);
    proto_wrapper_t *request, *reply;
    
    request=proto_init_check_integrity_request(db_get_remote_handle(db), 
                        txn ? txn_get_remote_handle(txn) : 0);

    st=_perform_request(env, env_get_curl(env), request, &reply);
    proto_delete(request);
    if (st) {
        if (reply)
            proto_delete(reply);
        return (st);
    }

    ham_assert(reply!=0, (""));
    ham_assert(proto_has_check_integrity_reply(reply), (""));
    st=proto_check_integrity_reply_get_status(reply);

    proto_delete(reply);

    return (st);
}

static ham_status_t
_remote_fun_get_key_count(ham_db_t *db, ham_txn_t *txn, ham_u32_t flags,
            ham_offset_t *keycount)
{
    ham_status_t st;
    ham_env_t *env=db_get_env(db);
    proto_wrapper_t *request, *reply;
    
    request=proto_init_db_get_key_count_request(db_get_remote_handle(db), 
                        txn ? txn_get_remote_handle(txn) : 0, flags);

    st=_perform_request(env, env_get_curl(env), request, &reply);
    proto_delete(request);
    if (st) {
        if (reply)
            proto_delete(reply);
        return (st);
    }

    ham_assert(reply!=0, (""));
    ham_assert(proto_has_db_get_key_count_reply(reply), (""));

    st=proto_db_get_key_count_reply_get_status(reply);
    if (!st)
        *keycount=proto_db_get_key_count_reply_get_key_count(reply);

    proto_delete(reply);

    return (st);
}

static ham_status_t
_remote_fun_insert(ham_db_t *db, ham_txn_t *txn, ham_key_t *key,
            ham_record_t *record, ham_u32_t flags)
{
    ham_status_t st;
    ham_env_t *env=db_get_env(db);
    proto_wrapper_t *request, *reply;
    ham_bool_t send_key=HAM_TRUE;

    /* recno: do not send the key */
    if (db_get_rt_flags(db)&HAM_RECORD_NUMBER)
        send_key=HAM_FALSE;
    
    request=proto_init_db_insert_request(db_get_remote_handle(db), 
                        txn ? txn_get_remote_handle(txn) : 0, 
                        send_key ? key : 0, record, flags);

    st=_perform_request(env, env_get_curl(env), request, &reply);
    proto_delete(request);
    if (st) {
        if (reply)
            proto_delete(reply);
        return (st);
    }

    ham_assert(reply!=0, (""));
    ham_assert(proto_has_db_insert_reply(reply)!=0, (""));
    st=proto_db_insert_reply_get_status(reply);

    /* recno: the key was modified! */
    if (st==0 && proto_db_insert_reply_has_key(reply)) {
        if (proto_db_insert_reply_get_key_size(reply)==sizeof(ham_offset_t)) {
            ham_assert(key->data!=0, (""));
            ham_assert(key->size==sizeof(ham_offset_t), (""));
            memcpy(key->data, proto_db_insert_reply_get_key_data(reply),
                    sizeof(ham_offset_t));
        }
    }

    proto_delete(reply);

    return (st);
}

static ham_status_t
_remote_fun_find(ham_db_t *db, ham_txn_t *txn, ham_key_t *key,
            ham_record_t *record, ham_u32_t flags)
{
    ham_status_t st;
    ham_env_t *env=db_get_env(db);
    proto_wrapper_t *request, *reply;

    request=proto_init_db_find_request(db_get_remote_handle(db), 
                        txn ? txn_get_remote_handle(txn) : 0, 
                        key, record, flags);

    st=_perform_request(env, env_get_curl(env), request, &reply);
    proto_delete(request);
    if (st) {
        if (reply)
            proto_delete(reply);
        return (st);
    }

    ham_assert(reply!=0, (""));
    ham_assert(proto_has_db_find_reply(reply)!=0, (""));

    st=proto_db_find_reply_get_status(reply);
    if (st==0) {
        /* approx. matching: need to copy the _flags and the key data! */
        if (proto_db_find_reply_has_key(reply)) {
            ham_assert(key, (""));
            key->_flags=proto_db_find_reply_get_key_intflags(reply);
            key->size=proto_db_find_reply_get_key_size(reply);
            if (!(key->flags&HAM_KEY_USER_ALLOC)) {
                st=db_resize_key_allocdata(db, key->size);
                if (st)
                    goto bail;
                key->data=db_get_key_allocdata(db);
            }
            memcpy(key->data, proto_db_find_reply_get_key_data(reply),
                    key->size);
        }
        if (proto_db_find_reply_has_record(reply)) {
            record->size=proto_db_find_reply_get_record_size(reply);
            if (!(record->flags&HAM_RECORD_USER_ALLOC)) {
                st=db_resize_record_allocdata(db, record->size);
                if (st)
                    goto bail;
                record->data=db_get_record_allocdata(db);
            }
            memcpy(record->data, proto_db_find_reply_get_record_data(reply),
                    record->size);
        }
    }

bail:
    proto_delete(reply);

    return (st);
}

static ham_status_t
_remote_fun_erase(ham_db_t *db, ham_txn_t *txn, ham_key_t *key, ham_u32_t flags)
{
    ham_status_t st;
    ham_env_t *env=db_get_env(db);
    proto_wrapper_t *request, *reply;
    
    request=proto_init_db_erase_request(db_get_remote_handle(db), 
                        txn ? txn_get_remote_handle(txn) : 0, 
                        key, flags);

    st=_perform_request(env, env_get_curl(env), request, &reply);
    proto_delete(request);
    if (st) {
        if (reply)
            proto_delete(reply);
        return (st);
    }

    ham_assert(reply!=0, (""));
    ham_assert(proto_has_db_erase_reply(reply)!=0, (""));
    st=proto_db_erase_reply_get_status(reply);

    proto_delete(reply);

    return (st);
}

static ham_status_t
_remote_cursor_create(ham_db_t *db, ham_txn_t *txn, ham_u32_t flags,
        ham_cursor_t **cursor)
{
    ham_env_t *env=db_get_env(db);
    ham_status_t st;
    proto_wrapper_t *request, *reply;
    
    request=proto_init_cursor_create_request(db_get_remote_handle(db), 
                        txn ? txn_get_remote_handle(txn) : 0, flags);

    st=_perform_request(env, env_get_curl(env), request, &reply);
    proto_delete(request);
    if (st) {
        if (reply)
            proto_delete(reply);
        return (st);
    }

    ham_assert(reply!=0, (""));
    ham_assert(proto_has_cursor_create_reply(reply)!=0, (""));

    st=proto_cursor_create_reply_get_status(reply);
    if (st) {
        proto_delete(reply);
        return (st);
    }

    *cursor=(ham_cursor_t *)allocator_calloc(env_get_allocator(env), 
                            sizeof(ham_cursor_t));
    if (!(*cursor))
        return (HAM_OUT_OF_MEMORY);

    cursor_set_remote_handle(*cursor, 
                proto_cursor_create_reply_get_cursor_handle(reply));

    proto_delete(reply);

    return (st);
}

static ham_status_t
_remote_cursor_clone(ham_cursor_t *src, ham_cursor_t **dest)
{
    ham_env_t *env=db_get_env(cursor_get_db(src));
    ham_status_t st;
    proto_wrapper_t *request, *reply;
    
    request=proto_init_cursor_clone_request(cursor_get_remote_handle(src));

    st=_perform_request(env, env_get_curl(env), request, &reply);
    proto_delete(request);
    if (st) {
        if (reply)
            proto_delete(reply);
        return (st);
    }

    ham_assert(reply!=0, (""));
    ham_assert(proto_has_cursor_clone_reply(reply)!=0, (""));

    st=proto_cursor_clone_reply_get_status(reply);
    if (st) {
        proto_delete(reply);
        return (st);
    }

    *dest=(ham_cursor_t *)allocator_calloc(env_get_allocator(env), 
                            sizeof(ham_cursor_t));
    if (!(*dest))
        return (HAM_OUT_OF_MEMORY);

    cursor_set_remote_handle(*dest, 
                proto_cursor_clone_reply_get_cursor_handle(reply));

    proto_delete(reply);

    return (st);
}

static ham_status_t
_remote_cursor_close(ham_cursor_t *cursor)
{
    ham_status_t st;
    ham_env_t *env=db_get_env(cursor_get_db(cursor));
    proto_wrapper_t *request, *reply;
    
    request=proto_init_cursor_close_request(cursor_get_remote_handle(cursor));

    st=_perform_request(env, env_get_curl(env), request, &reply);
    proto_delete(request);
    if (st) {
        if (reply)
            proto_delete(reply);
        return (st);
    }

    ham_assert(reply!=0, (""));
    ham_assert(proto_has_cursor_close_reply(reply)!=0, (""));

    st=proto_cursor_close_reply_get_status(reply);

    proto_delete(reply);

    return (st);
}

static ham_status_t
_remote_cursor_insert(ham_cursor_t *cursor, ham_key_t *key,
            ham_record_t *record, ham_u32_t flags)
{
    ham_status_t st;
    ham_db_t *db=cursor_get_db(cursor);
    ham_env_t *env=db_get_env(db);
    proto_wrapper_t *request, *reply;
    ham_bool_t send_key=HAM_TRUE;

    /* recno: do not send the key */
    if (db_get_rt_flags(db)&HAM_RECORD_NUMBER)
        send_key=HAM_FALSE;
    
    request=proto_init_cursor_insert_request(cursor_get_remote_handle(cursor), 
                        send_key ? key : 0, record, flags);

    st=_perform_request(env, env_get_curl(env), request, &reply);
    proto_delete(request);
    if (st) {
        if (reply)
            proto_delete(reply);
        return (st);
    }

    ham_assert(reply!=0, (""));
    ham_assert(proto_has_cursor_insert_reply(reply)!=0, (""));

    st=proto_cursor_insert_reply_get_status(reply);

    /* recno: the key was modified! */
    if (st==0 && proto_cursor_insert_reply_has_key(reply)) {
        if (proto_cursor_insert_reply_get_key_size(reply)
                ==sizeof(ham_offset_t)) {
            ham_assert(key->data!=0, (""));
            ham_assert(key->size==sizeof(ham_offset_t), (""));
            memcpy(key->data, proto_cursor_insert_reply_get_key_data(reply),
                    sizeof(ham_offset_t));
        }
    }

    proto_delete(reply);

    return (st);
}

static ham_status_t
_remote_cursor_erase(ham_cursor_t *cursor, ham_u32_t flags)
{
    ham_status_t st;
    ham_db_t *db=cursor_get_db(cursor);
    ham_env_t *env=db_get_env(db);
    proto_wrapper_t *request, *reply;
    
    request=proto_init_cursor_erase_request(cursor_get_remote_handle(cursor), 
                                    flags);

    st=_perform_request(env, env_get_curl(env), request, &reply);
    proto_delete(request);
    if (st) {
        if (reply)
            proto_delete(reply);
        return (st);
    }

    ham_assert(reply!=0, (""));
    ham_assert(proto_has_cursor_erase_reply(reply)!=0, (""));

    st=proto_cursor_erase_reply_get_status(reply);

    proto_delete(reply);

    return (st);
}

static ham_status_t
_remote_cursor_find(ham_cursor_t *cursor, ham_key_t *key, 
                ham_record_t *record, ham_u32_t flags)
{
    ham_status_t st;
    ham_db_t *db=cursor_get_db(cursor);
    ham_env_t *env=db_get_env(db);
    proto_wrapper_t *request, *reply;

    request=proto_init_cursor_find_request(cursor_get_remote_handle(cursor), 
                        key, record, flags);
    
    st=_perform_request(env, env_get_curl(env), request, &reply);
    proto_delete(request);
    if (st) {
        if (reply)
            proto_delete(reply);
        return (st);
    }

    ham_assert(reply!=0, (""));
    ham_assert(proto_has_cursor_find_reply(reply)!=0, (""));

    st=proto_cursor_find_reply_get_status(reply);
    if (st) 
        goto bail;

    /* approx. matching: need to copy the _flags! */
    if (proto_cursor_find_reply_has_key(reply)) {
        key->_flags=proto_cursor_find_reply_get_key_intflags(reply);
    }
    if (proto_cursor_find_reply_has_record(reply)) {
        ham_assert(record, (""));
        record->size=proto_cursor_find_reply_get_record_size(reply);
        if (!(record->flags&HAM_RECORD_USER_ALLOC)) {
            st=db_resize_record_allocdata(db, record->size);
            if (st)
                goto bail;
            record->data=db_get_record_allocdata(db);
        }
        memcpy(record->data, proto_cursor_find_reply_get_record_data(reply),
                record->size);
    }

bail:
    proto_delete(reply);
    return (st);
}

static ham_status_t
_remote_cursor_get_duplicate_count(ham_cursor_t *cursor, 
        ham_size_t *count, ham_u32_t flags)
{
    ham_status_t st;
    ham_db_t *db=cursor_get_db(cursor);
    ham_env_t *env=db_get_env(db);
    proto_wrapper_t *request, *reply;
    
    request=proto_init_cursor_get_duplicate_count_request(
                        cursor_get_remote_handle(cursor), flags);

    st=_perform_request(env, env_get_curl(env), request, &reply);
    proto_delete(request);
    if (st) {
        if (reply)
            proto_delete(reply);
        return (st);
    }

    ham_assert(reply!=0, (""));
    ham_assert(proto_has_cursor_get_duplicate_count_reply(reply)!=0, (""));

    st=proto_cursor_get_duplicate_count_reply_get_status(reply);
    if (st) 
        goto bail;

    *count=proto_cursor_get_duplicate_count_reply_get_count(reply);

bail:
    proto_delete(reply);
    return (st);
}

static ham_status_t
_remote_cursor_get_record_size(ham_cursor_t *cursor, ham_u64_t *size)
{
    (void)cursor;
    (void)size;
    /* need this? send me a mail and i will implement it. */
    return (HAM_NOT_IMPLEMENTED);
}

static ham_status_t
_remote_cursor_overwrite(ham_cursor_t *cursor, 
            ham_record_t *record, ham_u32_t flags)
{
    ham_status_t st;
    ham_db_t *db=cursor_get_db(cursor);
    ham_env_t *env=db_get_env(db);
    proto_wrapper_t *request, *reply;
    
    request=proto_init_cursor_overwrite_request(
                        cursor_get_remote_handle(cursor), record, flags);

    st=_perform_request(env, env_get_curl(env), request, &reply);
    proto_delete(request);
    if (st) {
        if (reply)
            proto_delete(reply);
        return (st);
    }

    ham_assert(reply!=0, (""));
    ham_assert(proto_has_cursor_overwrite_reply(reply)!=0, (""));

    st=proto_cursor_overwrite_reply_get_status(reply);

    proto_delete(reply);

    return (st);
}

static ham_status_t
_remote_cursor_move(ham_cursor_t *cursor, ham_key_t *key, 
                ham_record_t *record, ham_u32_t flags)
{
    ham_status_t st;
    ham_db_t *db=cursor_get_db(cursor);
    ham_env_t *env=db_get_env(db);
    proto_wrapper_t *request, *reply;
    
    request=proto_init_cursor_move_request(cursor_get_remote_handle(cursor), 
                        key, record, flags);

    st=_perform_request(env, env_get_curl(env), request, &reply);
    proto_delete(request);
    if (st) {
        if (reply)
            proto_delete(reply);
        return (st);
    }

    ham_assert(reply!=0, (""));
    ham_assert(proto_has_cursor_move_reply(reply)!=0, (""));

    st=proto_cursor_move_reply_get_status(reply);
    if (st) 
        goto bail;

    /* modify key/record, but make sure that USER_ALLOC is respected! */
    if (proto_cursor_move_reply_has_key(reply)) {
        ham_assert(key, (""));
        key->_flags=proto_cursor_move_reply_get_key_intflags(reply);
        key->size=proto_cursor_move_reply_get_key_size(reply);
        if (!(key->flags&HAM_KEY_USER_ALLOC)) {
            st=db_resize_key_allocdata(db, key->size);
            if (st)
                goto bail;
            key->data=db_get_key_allocdata(db);
        }
        memcpy(key->data, proto_cursor_move_reply_get_key_data(reply),
                key->size);
    }

    /* same for the record */
    if (proto_cursor_move_reply_has_record(reply)) {
        ham_assert(record, (""));
        record->size=proto_cursor_move_reply_get_record_size(reply);
        if (!(record->flags&HAM_RECORD_USER_ALLOC)) {
            st=db_resize_record_allocdata(db, record->size);
            if (st)
                goto bail;
            record->data=db_get_record_allocdata(db);
        }
        memcpy(record->data, proto_cursor_move_reply_get_record_data(reply),
                record->size);
    }

bail:
    proto_delete(reply);
    return (st);
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
    env->_fun_get_parameters     =_remote_fun_env_get_parameters;
    env->_fun_flush              =_remote_fun_env_flush;
    env->_fun_create_db          =_remote_fun_create_db;
    env->_fun_open_db            =_remote_fun_open_db;
    env->_fun_close              =_remote_fun_env_close;
    env->_fun_txn_begin          =_remote_fun_txn_begin;
    env->_fun_txn_commit         =_remote_fun_txn_commit;
    env->_fun_txn_abort          =_remote_fun_txn_abort;

    env_set_rt_flags(env, env_get_rt_flags(env)|DB_IS_REMOTE);
#else
    return (HAM_NOT_IMPLEMENTED);
#endif

    return (0);
}

ham_status_t
db_initialize_remote(ham_db_t *db)
{
#if HAM_ENABLE_REMOTE
    db->_fun_close          =_remote_fun_close;
    db->_fun_get_parameters =_remote_fun_get_parameters;
    db->_fun_check_integrity=_remote_fun_check_integrity;
    db->_fun_get_key_count  =_remote_fun_get_key_count;
    db->_fun_insert         =_remote_fun_insert;
    db->_fun_find           =_remote_fun_find;
    db->_fun_erase          =_remote_fun_erase;
    db->_fun_cursor_create  =_remote_cursor_create;
    db->_fun_cursor_clone   =_remote_cursor_clone;
    db->_fun_cursor_close   =_remote_cursor_close;
    db->_fun_cursor_insert  =_remote_cursor_insert;
    db->_fun_cursor_erase   =_remote_cursor_erase;
    db->_fun_cursor_find    =_remote_cursor_find;
    db->_fun_cursor_get_duplicate_count=_remote_cursor_get_duplicate_count;
    db->_fun_cursor_get_record_size=_remote_cursor_get_record_size;
    db->_fun_cursor_overwrite=_remote_cursor_overwrite;
    db->_fun_cursor_move    =_remote_cursor_move;
    return (0);
#else
    return (HAM_NOT_IMPLEMENTED);
#endif
}
