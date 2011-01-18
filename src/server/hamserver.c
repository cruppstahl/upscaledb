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


#include <stdio.h> /* needed for mongoose.h */
#include <malloc.h>
#include <string.h>

#include <mongoose/mongoose.h>

#include <ham/types.h>
#include <ham/hamsterdb_srv.h>
#include "../protocol/protocol.h"
#include "os.h"
#include "db.h"
#include "error.h"
#include "assert.h"
#include "cursor.h"
#include "mem.h"
#include "env.h"

/* max. number of open hamsterdb Environments - if you change this, also change
 * MAX_CALLBACKS in 3rdparty/mongoose/mongoose.c! */
#define MAX_ENVIRONMENTS    128
#define MAX_DATABASES       512

static const char *standard_reply =	"HTTP/1.1 200 OK\r\n"
                                    "Content-Type: text/plain\r\n"
                                    "Connection: close\r\n\r\n";

#define HANDLE_TYPE_DATABASE 1
#define HANDLE_TYPE_TRANSACTION 2
#define HANDLE_TYPE_CURSOR 3

typedef struct srv_handle_t
{
    void *ptr;
    int type;
    ham_u64_t handle;
} srv_handle_t;

struct ham_srv_t
{
    /* the mongoose context structure */
    struct mg_context *mg_ctxt;

    /* handler for each Environment */
    struct env_t {
        ham_env_t *env;
        os_critsec_t cs;
        char *urlname;
        srv_handle_t *handles;
        ham_u32_t handles_ctr;
        ham_u32_t handles_size;
    } environments[MAX_ENVIRONMENTS];
};

static ham_u64_t
__store_handle(struct env_t *envh, void *ptr, int type)
{
    unsigned i;
    ham_u64_t ret;

    for (i=0; i<envh->handles_size; i++) {
        if (envh->handles[i].ptr==0) {
            break;
        }
    }

    if (i==envh->handles_size) {
        envh->handles_size+=10;
        envh->handles=(srv_handle_t *)realloc(envh->handles, 
                        sizeof(srv_handle_t)*envh->handles_size);
        if (!envh->handles)
            return 0; /* not so nice, but if we're out of memory then 
                       * it does not make sense to go on... */
        memset(&envh->handles[envh->handles_size-10], 0, 
                        sizeof(srv_handle_t)*10);
    }

    ret=++envh->handles_ctr;
    ret=ret<<32;

    envh->handles[i].ptr=ptr;
    envh->handles[i].handle=ret|i;
    envh->handles[i].type=type;

    return (envh->handles[i].handle);
}

static void *
__get_handle(struct env_t *envh, ham_u64_t handle)
{
    srv_handle_t *h=&envh->handles[handle&0xffffffff];
    ham_assert(h->handle==handle, (""));
    if (h->handle!=handle)
        return (0);
    return h->ptr;
}

static void
__remove_handle(struct env_t *envh, ham_u64_t handle)
{
    srv_handle_t *h=&envh->handles[handle&0xffffffff];
    ham_assert(h->handle==handle, (""));
    if (h->handle!=handle)
        return;
    memset(h, 0, sizeof(*h));
}

static void
send_wrapper(ham_env_t *env, struct mg_connection *conn, 
                proto_wrapper_t *wrapper)
{
    ham_u8_t *data;
    ham_size_t data_size;

    if (!proto_pack(wrapper, env_get_allocator(env), &data, &data_size))
        return;

    ham_trace(("type %u: sending %d bytes", 
                proto_get_type(wrapper), data_size));
	mg_printf(conn, "%s", standard_reply);
    mg_write(conn, data, data_size);

    allocator_free(env_get_allocator(env), data);
}

static void
handle_connect(ham_env_t *env, struct mg_connection *conn, 
                const struct mg_request_info *ri, proto_wrapper_t *request)
{
    proto_wrapper_t *reply;

    ham_assert(request!=0, (""));
    ham_assert(proto_has_connect_request(request), (""));

    reply=proto_init_connect_reply(HAM_SUCCESS, env_get_rt_flags(env)); 

    send_wrapper(env, conn, reply);
    proto_delete(reply);
}

static void
handle_env_get_parameters(ham_env_t *env, struct mg_connection *conn, 
                const struct mg_request_info *ri, proto_wrapper_t *request)
{
    proto_wrapper_t *reply;
    ham_size_t i;
    ham_status_t st=0;
    ham_parameter_t params[100]; /* 100 should be enough... */

    ham_assert(request!=0, (""));
    ham_assert(proto_has_env_get_parameters_request(request), (""));

    /* initialize the ham_parameters_t array */
    memset(&params[0], 0, sizeof(params));
    for (i=0; i<proto_env_get_parameters_request_get_names_size(request) 
            && i<100; i++)
        params[i].name=proto_env_get_parameters_request_get_names(request)[i];

    /* and request the parameters from the Environment */
    st=ham_env_get_parameters(env, &params[0]);
    if (st) {
        reply=proto_init_env_get_parameters_reply(st);
        send_wrapper(env, conn, reply);
        proto_delete(reply);
        return;
    }

    reply=proto_init_env_get_parameters_reply(HAM_SUCCESS);

    /* initialize the reply package */
    for (i=0; i<proto_env_get_parameters_request_get_names_size(request); i++) {
        switch (params[i].name) {
        case HAM_PARAM_CACHESIZE:
            proto_env_get_parameters_reply_set_cachesize(reply, 
                            (int)params[i].value);
            break;
        case HAM_PARAM_PAGESIZE:
            proto_env_get_parameters_reply_set_pagesize(reply, 
                            (int)params[i].value);
            break;
        case HAM_PARAM_MAX_ENV_DATABASES:
            proto_env_get_parameters_reply_set_max_env_databases(reply, 
                            (int)params[i].value);
            break;
        case HAM_PARAM_GET_FLAGS:
            proto_env_get_parameters_reply_set_flags(reply, 
                            (int)params[i].value);
            break;
        case HAM_PARAM_GET_FILEMODE:
            proto_env_get_parameters_reply_set_filemode(reply, 
                            (int)params[i].value);
            break;
        case HAM_PARAM_GET_FILENAME:
            if (params[i].value)
                proto_env_get_parameters_reply_set_filename(reply, 
                            (const char *)(U64_TO_PTR(params[i].value)));
            break;
        default:
            ham_trace(("unsupported parameter %u", (unsigned)params[i].name));
            break;
        }
    }

    send_wrapper(env, conn, reply);
    proto_delete(reply);
}

static void
handle_db_get_parameters(struct env_t *envh, struct mg_connection *conn, 
                const struct mg_request_info *ri, proto_wrapper_t *request)
{
    ham_env_t *env=envh->env;
    ham_db_t *db;
    proto_wrapper_t *reply;
    ham_status_t st=0;
    ham_size_t i;
    ham_parameter_t params[100]; /* 100 should be enough... */

    ham_assert(request!=0, (""));
    ham_assert(proto_has_db_get_parameters_request(request), (""));

    /* initialize the ham_parameters_t array */
    memset(&params[0], 0, sizeof(params));
    for (i=0; i<proto_db_get_parameters_request_get_names_size(request) 
            && i<100; i++)
        params[i].name=proto_db_get_parameters_request_get_names(request)[i];

    /* and request the parameters from the Environment */
    db=__get_handle(envh, 
            proto_db_get_parameters_request_get_db_handle(request));
    if (!db) {
        st=HAM_INV_PARAMETER;
    }
    else {
        st=ham_get_parameters(db, &params[0]);
    }
    if (st) {
        reply=proto_init_db_get_parameters_reply(st);
        send_wrapper(env, conn, &reply);
        proto_delete(reply);
        return;
    }

    reply=proto_init_db_get_parameters_reply(HAM_SUCCESS);

    /* initialize the reply package */
    for (i=0; i<proto_db_get_parameters_request_get_names_size(request); i++) {
        switch (params[i].name) {
        case 0:
            continue;
        case HAM_PARAM_CACHESIZE:
            proto_db_get_parameters_reply_set_cachesize(reply, 
                            (int)params[i].value);
            break;
        case HAM_PARAM_PAGESIZE:
            proto_db_get_parameters_reply_set_pagesize(reply, 
                            (int)params[i].value);
            break;
        case HAM_PARAM_MAX_ENV_DATABASES:
            proto_db_get_parameters_reply_set_max_env_databases(reply, 
                            (int)params[i].value);
            break;
        case HAM_PARAM_GET_FLAGS:
            proto_db_get_parameters_reply_set_flags(reply, 
                            (int)params[i].value);
            break;
        case HAM_PARAM_GET_FILEMODE:
            proto_db_get_parameters_reply_set_filemode(reply, 
                            (int)params[i].value);
            break;
        case HAM_PARAM_GET_FILENAME:
            proto_db_get_parameters_reply_set_filename(reply, 
                            (char *)(U64_TO_PTR(params[i].value)));
            break;
        case HAM_PARAM_KEYSIZE:
            proto_db_get_parameters_reply_set_keysize(reply, 
                            (int)params[i].value);
            break;
        case HAM_PARAM_GET_DATABASE_NAME:
            proto_db_get_parameters_reply_set_dbname(reply, 
                            (int)params[i].value);
            break;
        case HAM_PARAM_GET_KEYS_PER_PAGE:
            proto_db_get_parameters_reply_set_keys_per_page(reply, 
                            (int)params[i].value);
            break;
        case HAM_PARAM_GET_DATA_ACCESS_MODE:
            proto_db_get_parameters_reply_set_dam(reply, 
                            (int)params[i].value);
            break;
        default:
            ham_trace(("unsupported parameter %u", (unsigned)params[i].name));
            break;
        }
    }

    send_wrapper(env, conn, reply);
    proto_delete(reply);
}

static void
handle_env_get_database_names(ham_env_t *env, struct mg_connection *conn, 
                const struct mg_request_info *ri, proto_wrapper_t *request)
{
    proto_wrapper_t *reply;
    ham_status_t st=0;
    ham_size_t num_names=1024;
    ham_u16_t names[1024]; /* should be enough */

    ham_assert(request!=0, (""));
    ham_assert(proto_has_env_get_database_names_request(request), (""));

    /* request the database names from the Environment */
    st=ham_env_get_database_names(env, &names[0], &num_names);
    reply=proto_init_env_get_database_names_reply(st, names, num_names);
    send_wrapper(env, conn, reply);
    proto_delete(reply);
}

static void
handle_env_flush(ham_env_t *env, struct mg_connection *conn, 
                const struct mg_request_info *ri, proto_wrapper_t *request)
{
    proto_wrapper_t *reply;

    ham_assert(request!=0, (""));
    ham_assert(proto_has_env_flush_request(request), (""));

    /* request the database names from the Environment */
    reply=proto_init_env_flush_reply(ham_env_flush(env, 
                proto_env_flush_request_get_flags(request)));

    send_wrapper(env, conn, reply);
    proto_delete(reply);
}

static void
handle_env_rename(ham_env_t *env, struct mg_connection *conn, 
                const struct mg_request_info *ri, proto_wrapper_t *request)
{
    proto_wrapper_t *reply;

    ham_assert(request!=0, (""));
    ham_assert(proto_has_env_rename_request(request), (""));

    /* rename the databases */
    reply=proto_init_env_rename_reply(ham_env_rename_db(env, 
                proto_env_rename_request_get_oldname(request),
                proto_env_rename_request_get_newname(request),
                proto_env_rename_request_get_flags(request)));

    send_wrapper(env, conn, reply);
    proto_delete(reply);
}

static void
handle_env_create_db(struct env_t *envh, ham_env_t *env, 
                struct mg_connection *conn, const struct mg_request_info *ri,
                proto_wrapper_t *request)
{
    unsigned i;
    ham_db_t *db;
    ham_status_t st=0;
    ham_u64_t db_handle=0;
    proto_wrapper_t *reply;
    ham_parameter_t params[100]={{0, 0}};

    ham_assert(request!=0, (""));
    ham_assert(proto_has_env_create_db_request(request), (""));

    /* convert parameters */
    ham_assert(proto_env_create_db_request_get_num_params(request)<100, (""));
    for (i=0; i<proto_env_create_db_request_get_num_params(request); i++) {
        params[i].name =proto_env_create_db_request_get_param_names(request)[i];
        params[i].value=proto_env_create_db_request_get_param_values(request)[i];
    }

    /* create the database */
    ham_new(&db);
    st=ham_env_create_db(env, db, 
            proto_env_create_db_request_get_dbname(request),
            proto_env_create_db_request_get_flags(request), &params[0]);

    if (st==0) {
        /* allocate a new database handle in the Env wrapper structure */
        db_handle=__store_handle(envh, db, HANDLE_TYPE_DATABASE);
    }
    else {
        ham_delete(db);
    }

    reply=proto_init_env_create_db_reply(st, db_handle,
            db->_rt_flags); /* do not use db_get_rt_flags() because they're
                          * mixed with the flags from the Environment! */
    send_wrapper(env, conn, reply);
    proto_delete(reply);
}

static void
handle_env_open_db(struct env_t *envh, ham_env_t *env, 
                struct mg_connection *conn, const struct mg_request_info *ri,
                proto_wrapper_t *request)
{
    unsigned i;
    ham_db_t *db=0;
    ham_u64_t db_handle=0;
    ham_status_t st=0;
    proto_wrapper_t *reply;
    ham_u16_t dbname=proto_env_open_db_request_get_dbname(request);
    ham_parameter_t params[100]={{0, 0}};

    ham_assert(request!=0, (""));
    ham_assert(proto_has_env_open_db_request(request), (""));

    /* convert parameters */
    ham_assert(proto_env_open_db_request_get_num_params(request)<100, (""));
    for (i=0; i<proto_env_open_db_request_get_num_params(request); i++) {
        params[i].name =proto_env_open_db_request_get_param_names(request)[i];
        params[i].value=proto_env_open_db_request_get_param_values(request)[i];
    }

    /* check if the database is already open */
    for (i=0; i<envh->handles_size; i++) {
        if (envh->handles[i].ptr!=0) {
            if (envh->handles[i].type==HANDLE_TYPE_DATABASE) {
                db=envh->handles[i].ptr;
                if (db_get_dbname(db)==dbname)
                    break;
                else
                    db=0;
            }
        }
    }

    /* if not found: open the database */
    if (!db) {
        ham_new(&db);
        st=ham_env_open_db(env, db, dbname, 
                            proto_env_open_db_request_get_flags(request),
                            &params[0]);

        if (st==0) {
            /* allocate a new database handle in the Env wrapper structure */
            db_handle=__store_handle(envh, db, HANDLE_TYPE_DATABASE);
        }
        else {
            ham_delete(db);
        }
    }

    reply=proto_init_env_open_db_reply(st, db_handle,
            db->_rt_flags); /* do not use db_get_rt_flags() because they're
                          * mixed with the flags from the Environment! */
    send_wrapper(env, conn, reply);
    proto_delete(reply);
}

static void
handle_env_erase_db(ham_env_t *env, struct mg_connection *conn, 
                const struct mg_request_info *ri, proto_wrapper_t *request)
{
    proto_wrapper_t *reply;

    ham_assert(request!=0, (""));
    ham_assert(proto_has_env_erase_db_request(request), (""));

    reply=proto_init_env_erase_db_reply(ham_env_erase_db(env, 
                proto_env_erase_db_request_get_dbname(request),
                proto_env_erase_db_request_get_flags(request)));
            
    send_wrapper(env, conn, reply);
    proto_delete(reply);
}

static void
handle_db_close(struct env_t *envh, struct mg_connection *conn, 
                const struct mg_request_info *ri, proto_wrapper_t *request)
{
    proto_wrapper_t *reply;
    ham_db_t *db;
    ham_status_t st=0;
    ham_env_t *env=envh->env;

    ham_assert(request!=0, (""));
    ham_assert(proto_has_db_close_request(request), (""));

    db=__get_handle(envh, proto_db_close_request_get_db_handle(request));
    if (!db) {
        /* accept this - most likely the database was already closed by
         * another process */
        st=0;
    }
    else {
        st=ham_close(db, proto_db_close_request_get_flags(request));
        if (st==0) {
            ham_delete(db);
            __remove_handle(envh, 
                    proto_db_close_request_get_db_handle(request));
        }
    }

    reply=proto_init_db_close_reply(st);
    send_wrapper(env, conn, reply);
    proto_delete(reply);
}

static void
handle_txn_begin(struct env_t *envh, struct mg_connection *conn, 
                const struct mg_request_info *ri, proto_wrapper_t *request)
{
    proto_wrapper_t *reply;
    ham_txn_t *txn;
    ham_status_t st=0;
    ham_u64_t handle=0;
    ham_db_t *db;
    ham_env_t *env=envh->env;

    ham_assert(request!=0, (""));
    ham_assert(proto_has_txn_begin_request(request), (""));

    db=__get_handle(envh, proto_txn_begin_request_get_db_handle(request));
    if (!db) {
        st=HAM_INV_PARAMETER;
    }
    else {
        st=ham_txn_begin(&txn, db, proto_txn_begin_request_get_flags(request));
    }

    if (st==0)
        handle=__store_handle(envh, txn, HANDLE_TYPE_TRANSACTION);

    reply=proto_init_txn_begin_reply(st, handle);
    send_wrapper(env, conn, reply);
    proto_delete(reply);
}

static void
handle_txn_commit(struct env_t *envh, struct mg_connection *conn, 
                const struct mg_request_info *ri, proto_wrapper_t *request)
{
    proto_wrapper_t *reply;
    ham_txn_t *txn;
    ham_env_t *env=envh->env;
    ham_status_t st=0;

    ham_assert(request!=0, (""));
    ham_assert(proto_has_txn_commit_request(request), (""));

    txn=__get_handle(envh, proto_txn_commit_request_get_txn_handle(request));
    if (!txn) {
        st=HAM_INV_PARAMETER;
    }
    else {
        st=ham_txn_commit(txn, proto_txn_commit_request_get_flags(request));
        if (st==0) {
            /* remove the handle from the Env wrapper structure */
            __remove_handle(envh, 
                    proto_txn_commit_request_get_txn_handle(request));
        }
    }

    reply=proto_init_txn_commit_reply(st);
    send_wrapper(env, conn, reply);
    proto_delete(reply);
}

static void
handle_txn_abort(struct env_t *envh, struct mg_connection *conn, 
                const struct mg_request_info *ri, proto_wrapper_t *request)
{
    proto_wrapper_t *reply;
    ham_txn_t *txn;
    ham_status_t st=0;
    ham_env_t *env=envh->env;

    ham_assert(request!=0, (""));
    ham_assert(proto_has_txn_abort_request(request), (""));

    txn=__get_handle(envh, proto_txn_abort_request_get_txn_handle(request));
    if (!txn) {
        st=HAM_INV_PARAMETER;
    }
    else {
        st=ham_txn_abort(txn, proto_txn_abort_request_get_flags(request));
        if (st==0) {
            /* remove the handle from the Env wrapper structure */
            __remove_handle(envh, 
                    proto_txn_abort_request_get_txn_handle(request));
        }
    }

    reply=proto_init_txn_abort_reply(st);
    send_wrapper(env, conn, reply);
    proto_delete(reply);
}

static void
handle_db_check_integrity(struct env_t *envh, struct mg_connection *conn, 
                const struct mg_request_info *ri, proto_wrapper_t *request)
{
    proto_wrapper_t *reply;
    ham_txn_t *txn=0;
    ham_db_t *db;
    ham_status_t st=0;

    ham_assert(request!=0, (""));
    ham_assert(proto_has_check_integrity_request(request), (""));

    if (proto_check_integrity_request_get_txn_handle(request)) {
        txn=__get_handle(envh, 
                proto_check_integrity_request_get_txn_handle(request));
        if (!txn) {
            st=HAM_INV_PARAMETER;
        }
    }

    if (st==0) {
        db=__get_handle(envh, 
                proto_check_integrity_request_get_db_handle(request));
        if (!db) {
            st=HAM_INV_PARAMETER;
        }
        else {
            st=ham_check_integrity(db, txn);
        }
    }

    reply=proto_init_check_integrity_reply(st);
    send_wrapper(envh->env, conn, reply);
    proto_delete(reply);
}

static void
handle_db_get_key_count(struct env_t *envh, struct mg_connection *conn, 
                const struct mg_request_info *ri, proto_wrapper_t *request)
{
    proto_wrapper_t *reply;
    ham_txn_t *txn=0;
    ham_db_t *db;
    ham_status_t st=0;
    ham_u64_t keycount;

    ham_assert(request!=0, (""));
    ham_assert(proto_has_db_get_key_count_request(request), (""));

    if (proto_db_get_key_count_request_get_txn_handle(request)) {
        txn=__get_handle(envh, 
                proto_db_get_key_count_request_get_txn_handle(request));
        if (!txn) {
            st=HAM_INV_PARAMETER;
        }
    }

    if (st==0) {
        db=__get_handle(envh, 
                proto_db_get_key_count_request_get_db_handle(request));
        if (!db) {
            st=HAM_INV_PARAMETER;
        }
        else {
            st=ham_get_key_count(db, txn,
                    proto_db_get_key_count_request_get_flags(request),
                    &keycount);
        }
    }

    reply=proto_init_db_get_key_count_reply(st, keycount);
    send_wrapper(envh->env, conn, reply);
    proto_delete(reply);
}

static void
handle_db_insert(struct env_t *envh, struct mg_connection *conn, 
                const struct mg_request_info *ri, proto_wrapper_t *request)
{
    proto_wrapper_t *reply;
    ham_txn_t *txn=0;
    ham_db_t *db;
    ham_status_t st=0;
    ham_bool_t send_key=HAM_FALSE;
    ham_key_t key;
    ham_record_t rec;

    ham_assert(request!=0, (""));
    ham_assert(proto_has_db_insert_request(request), (""));

    if (proto_db_insert_request_get_txn_handle(request)) {
        txn=__get_handle(envh, proto_db_insert_request_get_txn_handle(request));
        if (!txn) {
            st=HAM_INV_PARAMETER;
        }
    }

    if (st==0) {
        db=__get_handle(envh, proto_db_insert_request_get_db_handle(request));
        if (!db) {
            st=HAM_INV_PARAMETER;
        }
        else {
            memset(&key, 0, sizeof(key));
            if (proto_db_insert_request_has_key(request)) {
                key.size=proto_db_insert_request_get_key_size(request);
                if (key.size)
                    key.data=proto_db_insert_request_get_key_data(request);
                key.flags=proto_db_insert_request_get_key_flags(request)
                            & (~HAM_KEY_USER_ALLOC);
            }

            memset(&rec, 0, sizeof(rec));
            if (proto_db_insert_request_has_record(request)) {
                rec.size=proto_db_insert_request_get_record_size(request);
                if (rec.size)
                    rec.data=proto_db_insert_request_get_record_data(request);
                rec.partial_size=proto_db_insert_request_get_record_partial_size(request);
                rec.partial_offset=proto_db_insert_request_get_record_partial_offset(request);
                rec.flags=proto_db_insert_request_get_record_flags(request) 
                            & (~HAM_RECORD_USER_ALLOC);
            }
            st=ham_insert(db, txn, &key, &rec, 
                            proto_db_insert_request_get_flags(request));

            /* recno: return the modified key */
            if ((st==0) && (ham_get_flags(db)&HAM_RECORD_NUMBER)) {
                ham_assert(key.size==sizeof(ham_offset_t), (""));
                send_key=HAM_TRUE;
            }
        }
    }

    reply=proto_init_db_insert_reply(st, send_key ? &key : 0);
    send_wrapper(envh->env, conn, reply);
    proto_delete(reply);
}

static void
handle_db_find(struct env_t *envh, struct mg_connection *conn, 
                const struct mg_request_info *ri, proto_wrapper_t *request)
{
    proto_wrapper_t *reply;
    ham_txn_t *txn=0;
    ham_db_t *db;
    ham_status_t st=0;
    ham_key_t key;
    ham_record_t rec;
    ham_bool_t send_key=HAM_FALSE;

    ham_assert(request!=0, (""));
    ham_assert(proto_has_db_find_request(request), (""));

    if (proto_db_find_request_get_txn_handle(request)) {
        txn=__get_handle(envh, proto_db_find_request_get_txn_handle(request));
        if (!txn) {
            st=HAM_INV_PARAMETER;
        }
    }

    if (st==0) {
        db=__get_handle(envh, proto_db_find_request_get_db_handle(request));
        if (!db) {
            st=HAM_INV_PARAMETER;
        }
        else {
            memset(&key, 0, sizeof(key));
            key.data=proto_db_find_request_get_key_data(request);
            key.size=proto_db_find_request_get_key_size(request);
            key.flags=proto_db_find_request_get_key_flags(request)
                        & (~HAM_KEY_USER_ALLOC);

            memset(&rec, 0, sizeof(rec));
            rec.data=proto_db_find_request_get_record_data(request);
            rec.size=proto_db_find_request_get_record_size(request);
            rec.partial_size=proto_db_find_request_get_record_partial_size(request);
            rec.partial_offset=proto_db_find_request_get_record_partial_offset(request);
            rec.flags=proto_db_find_request_get_record_flags(request) 
                        & (~HAM_RECORD_USER_ALLOC);

            st=ham_find(db, txn, &key, &rec, 
                            proto_db_find_request_get_flags(request));

            if (st==0) {
                /* approx matching: key->_flags was modified! */
                if (key._flags) {
                    send_key=HAM_TRUE;
                }
            }
        }
    }

    reply=proto_init_db_find_reply(st, send_key ? &key : 0, &rec);
    send_wrapper(envh->env, conn, reply);
    proto_delete(reply);
}

static void
handle_db_erase(struct env_t *envh, struct mg_connection *conn, 
                const struct mg_request_info *ri, proto_wrapper_t *request)
{
    proto_wrapper_t *reply;
    ham_txn_t *txn=0;
    ham_db_t *db;
    ham_status_t st=0;

    ham_assert(request!=0, (""));
    ham_assert(proto_has_db_erase_request(request), (""));

    if (proto_db_erase_request_get_txn_handle(request)) {
        txn=__get_handle(envh, proto_db_erase_request_get_txn_handle(request));
        if (!txn) {
            st=HAM_INV_PARAMETER;
        }
    }

    if (st==0) {
        db=__get_handle(envh, proto_db_erase_request_get_db_handle(request));
        if (!db) {
            st=HAM_INV_PARAMETER;
        }
        else {
            ham_key_t key;

            memset(&key, 0, sizeof(key));
            key.data=proto_db_erase_request_get_key_data(request);
            key.size=proto_db_erase_request_get_key_size(request);
            key.flags=proto_db_erase_request_get_key_flags(request)
                        & (~HAM_KEY_USER_ALLOC);

            st=ham_erase(db, txn, &key, 
                    proto_db_erase_request_get_flags(request));
        }
    }

    reply=proto_init_db_erase_reply(st);
    send_wrapper(envh->env, conn, reply);
    proto_delete(reply);
}

static void
handle_cursor_create(struct env_t *envh, struct mg_connection *conn, 
                const struct mg_request_info *ri, proto_wrapper_t *request)
{
    proto_wrapper_t *reply;
    ham_txn_t *txn=0;
    ham_db_t *db;
    ham_cursor_t *cursor;
    ham_status_t st=0;
    ham_u64_t handle=0;

    ham_assert(request!=0, (""));
    ham_assert(proto_has_cursor_create_request(request), (""));

    if (proto_cursor_create_request_get_txn_handle(request)) {
        txn=__get_handle(envh, 
                        proto_cursor_create_request_get_txn_handle(request));
        if (!txn) {
            st=HAM_INV_PARAMETER;
            goto bail;
        }
    }

    db=__get_handle(envh, proto_cursor_create_request_get_db_handle(request));
    if (!db) {
        st=HAM_INV_PARAMETER;
        goto bail;
    }

    /* create the cursor */
    st=ham_cursor_create(db, txn, 
            proto_cursor_create_request_get_flags(request), &cursor);

    if (st==0) {
        /* allocate a new handle in the Env wrapper structure */
        handle=__store_handle(envh, cursor, HANDLE_TYPE_CURSOR);
    }

bail:
    reply=proto_init_cursor_create_reply(st, handle);
    send_wrapper(envh->env, conn, reply);
    proto_delete(reply);
}

static void
handle_cursor_clone(struct env_t *envh, struct mg_connection *conn, 
                const struct mg_request_info *ri, proto_wrapper_t *request)
{
    proto_wrapper_t *reply;
    ham_cursor_t *src;
    ham_cursor_t *dest;
    ham_status_t st=0;
    ham_u64_t handle=0;

    ham_assert(request!=0, (""));
    ham_assert(proto_has_cursor_clone_request(request), (""));

    src=__get_handle(envh, 
            proto_cursor_clone_request_get_cursor_handle(request));
    if (!src) {
        st=HAM_INV_PARAMETER;
        goto bail;
    }

    /* clone the cursor */
    st=ham_cursor_clone(src, &dest);
    if (st==0) {
        /* allocate a new handle in the Env wrapper structure */
        handle=__store_handle(envh, dest, HANDLE_TYPE_CURSOR);
    }

bail:
    reply=proto_init_cursor_clone_reply(st, handle);
    send_wrapper(envh->env, conn, reply);
    proto_delete(reply);
}

static void
handle_cursor_insert(struct env_t *envh, struct mg_connection *conn, 
                const struct mg_request_info *ri, proto_wrapper_t *request)
{
    proto_wrapper_t *reply;
    ham_cursor_t *cursor;
    ham_key_t key;
    ham_record_t rec;
    ham_status_t st=0;
    ham_bool_t send_key=HAM_FALSE;

    ham_assert(request!=0, (""));
    ham_assert(proto_has_cursor_insert_request(request), (""));

    cursor=__get_handle(envh,
            proto_cursor_insert_request_get_cursor_handle(request));
    if (!cursor) {
        st=HAM_INV_PARAMETER;
        goto bail;
    }

    memset(&key, 0, sizeof(key));
    if (proto_cursor_insert_request_has_key(request)) {
        key.size=proto_cursor_insert_request_get_key_size(request);
        if (key.size)
            key.data=proto_cursor_insert_request_get_key_data(request);
        key.flags=proto_cursor_insert_request_get_key_flags(request) 
                & (~HAM_KEY_USER_ALLOC);
    }

    memset(&rec, 0, sizeof(rec));
    if (proto_cursor_insert_request_has_record(request)) {
        rec.size=proto_cursor_insert_request_get_record_size(request);
        if (rec.size)
            rec.data=proto_cursor_insert_request_get_record_data(request);
        rec.partial_size=proto_cursor_insert_request_get_record_partial_size(request);
        rec.partial_offset=proto_cursor_insert_request_get_record_partial_offset(request);
        rec.flags=proto_cursor_insert_request_get_record_flags(request) 
                        & (~HAM_RECORD_USER_ALLOC);
    }

    st=ham_cursor_insert(cursor, &key, &rec, 
                    proto_cursor_insert_request_get_flags(request));

    /* recno: return the modified key */
    if ((st==0) && 
            (ham_get_flags(cursor_get_db(cursor))&HAM_RECORD_NUMBER)) {
        ham_assert(key.size==sizeof(ham_offset_t), (""));
        send_key=HAM_TRUE;
    }

bail:
    reply=proto_init_cursor_insert_reply(st, send_key ? &key : 0);
    send_wrapper(envh->env, conn, reply);
    proto_delete(reply);
}

static void
handle_cursor_erase(struct env_t *envh, struct mg_connection *conn, 
                const struct mg_request_info *ri, proto_wrapper_t *request)
{
    proto_wrapper_t *reply;
    ham_cursor_t *cursor;
    ham_status_t st=0;

    ham_assert(request!=0, (""));
    ham_assert(proto_has_cursor_erase_request(request), (""));

    cursor=__get_handle(envh,
            proto_cursor_erase_request_get_cursor_handle(request));
    if (!cursor) {
        st=HAM_INV_PARAMETER;
        goto bail;
    }

    st=ham_cursor_erase(cursor,
            proto_cursor_erase_request_get_flags(request));

bail:
    reply=proto_init_cursor_erase_reply(st);
    send_wrapper(envh->env, conn, reply);
    proto_delete(reply);
}

static void
handle_cursor_find(struct env_t *envh, struct mg_connection *conn, 
                const struct mg_request_info *ri, proto_wrapper_t *request)
{
    proto_wrapper_t *reply;
    ham_cursor_t *cursor;
    ham_key_t key; 
    ham_record_t rec; 
    ham_status_t st=0;
    ham_bool_t send_key=HAM_FALSE;
    ham_bool_t send_rec=HAM_FALSE;

    ham_assert(request!=0, (""));
    ham_assert(proto_has_cursor_find_request(request), (""));

    cursor=__get_handle(envh,
            proto_cursor_find_request_get_cursor_handle(request));
    if (!cursor) {
        st=HAM_INV_PARAMETER;
        goto bail;
    }

    memset(&key, 0, sizeof(key));
    key.data=proto_cursor_find_request_get_key_data(request);
    key.size=proto_cursor_find_request_get_key_size(request);
    key.flags=proto_cursor_find_request_get_key_flags(request) 
                & (~HAM_KEY_USER_ALLOC);

    if (proto_cursor_find_request_has_record(request)) {
        send_rec=HAM_TRUE;

        memset(&rec, 0, sizeof(rec));
        rec.data=proto_cursor_find_request_get_record_data(request);
        rec.size=proto_cursor_find_request_get_record_size(request);
        rec.partial_size=proto_cursor_find_request_get_record_partial_size(request);
        rec.partial_offset=proto_cursor_find_request_get_record_partial_offset(request);
        rec.flags=proto_cursor_find_request_get_record_flags(request) 
                        & (~HAM_RECORD_USER_ALLOC);
    }

    st=ham_cursor_find_ex(cursor, &key, 
                    send_rec ? &rec : 0,
                    proto_cursor_find_request_get_flags(request));

    if (st==0) {
        /* approx matching: key->_flags was modified! */
        if (key._flags) {
            send_key=HAM_TRUE;
        }
    }

bail:
    reply=proto_init_cursor_find_reply(st, 
                    send_key ? &key : 0,
                    send_rec ? &rec : 0);
    send_wrapper(envh->env, conn, reply);
    proto_delete(reply);
}

static void
handle_cursor_get_duplicate_count(struct env_t *envh, 
                struct mg_connection *conn, const struct mg_request_info *ri,
                proto_wrapper_t *request)
{
    proto_wrapper_t *reply;
    ham_cursor_t *cursor;
    ham_status_t st=0;
    ham_size_t count=0;

    ham_assert(request!=0, (""));
    ham_assert(proto_has_cursor_get_duplicate_count_request(request), (""));

    cursor=__get_handle(envh,
           proto_cursor_get_duplicate_count_request_get_cursor_handle(request));
    if (!cursor) {
        st=HAM_INV_PARAMETER;
        goto bail;
    }

    st=ham_cursor_get_duplicate_count(cursor, &count, 
                proto_cursor_get_duplicate_count_request_get_flags(request));

bail:
    reply=proto_init_cursor_get_duplicate_count_reply(st, count);
    send_wrapper(envh->env, conn, reply);
    proto_delete(reply);
}

static void
handle_cursor_overwrite(struct env_t *envh, struct mg_connection *conn, 
                const struct mg_request_info *ri, proto_wrapper_t *request)
{
    proto_wrapper_t *reply;
    ham_cursor_t *cursor;
    ham_record_t rec;
    ham_status_t st=0;

    ham_assert(request!=0, (""));
    ham_assert(proto_has_cursor_overwrite_request(request), (""));

    cursor=__get_handle(envh,
           proto_cursor_overwrite_request_get_cursor_handle(request));
    if (!cursor) {
        st=HAM_INV_PARAMETER;
        goto bail;
    }

    memset(&rec, 0, sizeof(rec));
    rec.data=proto_cursor_overwrite_request_get_record_data(request);
    rec.size=proto_cursor_overwrite_request_get_record_size(request);
    rec.partial_size=proto_cursor_overwrite_request_get_record_partial_size(request);
    rec.partial_offset=proto_cursor_overwrite_request_get_record_partial_offset(request);
    rec.flags=proto_cursor_overwrite_request_get_record_flags(request) 
                    & (~HAM_RECORD_USER_ALLOC);

    st=ham_cursor_overwrite(cursor, &rec,
           proto_cursor_overwrite_request_get_flags(request));

bail:
    reply=proto_init_cursor_overwrite_reply(st);
    send_wrapper(envh->env, conn, reply);
    proto_delete(reply);
}

static void
handle_cursor_move(struct env_t *envh, struct mg_connection *conn, 
                const struct mg_request_info *ri, proto_wrapper_t *request)
{
    proto_wrapper_t *reply;
    ham_cursor_t *cursor;
    ham_key_t key; 
    ham_record_t rec; 
    ham_status_t st=0;
    ham_bool_t send_key=HAM_FALSE;
    ham_bool_t send_rec=HAM_FALSE;

    ham_assert(request!=0, (""));
    ham_assert(proto_has_cursor_move_request(request), (""));

    cursor=__get_handle(envh,
           proto_cursor_move_request_get_cursor_handle(request));
    if (!cursor) {
        st=HAM_INV_PARAMETER;
        goto bail;
    }

    if (proto_cursor_move_request_has_key(request)) {
        send_key=HAM_TRUE;

        memset(&key, 0, sizeof(key));
        key.data=proto_cursor_move_request_get_key_data(request);
        key.size=proto_cursor_move_request_get_key_size(request);
        key.flags=proto_cursor_move_request_get_key_flags(request) 
                & (~HAM_KEY_USER_ALLOC);
    }

    if (proto_cursor_move_request_has_record(request)) {
        send_rec=HAM_TRUE;

        memset(&rec, 0, sizeof(rec));
        rec.data=proto_cursor_move_request_get_record_data(request);
        rec.size=proto_cursor_move_request_get_record_size(request);
        rec.partial_size=proto_cursor_move_request_get_record_partial_size(request);
        rec.partial_offset=proto_cursor_move_request_get_record_partial_offset(request);
        rec.flags=proto_cursor_move_request_get_record_flags(request) 
                    & (~HAM_RECORD_USER_ALLOC);
    }

    st=ham_cursor_move(cursor,
                    send_key ? &key : 0,
                    send_rec ? &rec : 0, 
                    proto_cursor_move_request_get_flags(request));

bail:
    reply=proto_init_cursor_move_reply(st, 
            (st==0 && send_key) ? &key : 0,
            (st==0 && send_rec) ? &rec : 0);
    send_wrapper(envh->env, conn, reply);
    proto_delete(reply);
}

static void
handle_cursor_close(struct env_t *envh, struct mg_connection *conn, 
                const struct mg_request_info *ri, proto_wrapper_t *request)
{
    proto_wrapper_t *reply;
    ham_cursor_t *cursor;
    ham_status_t st=0;

    ham_assert(request!=0, (""));
    ham_assert(proto_has_cursor_close_request(request), (""));

    cursor=__get_handle(envh,
           proto_cursor_close_request_get_cursor_handle(request));
    if (!cursor) {
        st=HAM_INV_PARAMETER;
        goto bail;
    }

    /* close the cursor */
    st=ham_cursor_close(cursor);
    if (st==0) {
        /* remove the handle from the Env wrapper structure */
        __remove_handle(envh,
           proto_cursor_close_request_get_cursor_handle(request));
    }

bail:
    reply=proto_init_cursor_close_reply(st);
    send_wrapper(envh->env, conn, reply);
    proto_delete(reply);
}

static void
request_handler(struct mg_connection *conn, const struct mg_request_info *ri,
                void *user_data)
{
    proto_wrapper_t *wrapper;
    struct env_t *env=(struct env_t *)user_data;

    mg_authorize(conn);

    os_critsec_enter(&env->cs);

    wrapper=proto_unpack(ri->post_data_len, (ham_u8_t *)ri->post_data);
    if (!wrapper) {
        ham_trace(("failed to unpack wrapper (%d bytes)\n", ri->post_data_len));
        goto bail;   
    }

    switch (proto_get_type(wrapper)) {
    case HAM__WRAPPER__TYPE__CONNECT_REQUEST:
        ham_trace(("connect request"));
        handle_connect(env->env, conn, ri, wrapper);
        break;
    case HAM__WRAPPER__TYPE__ENV_GET_PARAMETERS_REQUEST:
        ham_trace(("env_get_parameters request"));
        handle_env_get_parameters(env->env, conn, ri, wrapper);
        break;
    case HAM__WRAPPER__TYPE__ENV_GET_DATABASE_NAMES_REQUEST:
        ham_trace(("env_get_database_names request"));
        handle_env_get_database_names(env->env, conn, ri, wrapper);
        break;
    case HAM__WRAPPER__TYPE__ENV_FLUSH_REQUEST:
        ham_trace(("env_flush request"));
        handle_env_flush(env->env, conn, ri, wrapper);
        break;
    case HAM__WRAPPER__TYPE__ENV_RENAME_REQUEST:
        ham_trace(("env_rename request"));
        handle_env_rename(env->env, conn, ri, wrapper);
        break;
    case HAM__WRAPPER__TYPE__ENV_CREATE_DB_REQUEST:
        ham_trace(("env_create_db request"));
        handle_env_create_db(env, env->env, conn, ri, wrapper);
        break;
    case HAM__WRAPPER__TYPE__ENV_OPEN_DB_REQUEST:
        ham_trace(("env_open_db request"));
        handle_env_open_db(env, env->env, conn, ri, wrapper);
        break;
    case HAM__WRAPPER__TYPE__ENV_ERASE_DB_REQUEST:
        ham_trace(("env_erase_db request"));
        handle_env_erase_db(env->env, conn, ri, wrapper);
        break;
    case HAM__WRAPPER__TYPE__DB_CLOSE_REQUEST:
        ham_trace(("db_close request"));
        handle_db_close(env, conn, ri, wrapper);
        break;
    case HAM__WRAPPER__TYPE__DB_GET_PARAMETERS_REQUEST:
        ham_trace(("db_get_parameters request"));
        handle_db_get_parameters(env, conn, ri, wrapper);
        break;
    case HAM__WRAPPER__TYPE__TXN_BEGIN_REQUEST:
        ham_trace(("txn_begin request"));
        handle_txn_begin(env, conn, ri, wrapper);
        break;
    case HAM__WRAPPER__TYPE__TXN_COMMIT_REQUEST:
        ham_trace(("txn_commit request"));
        handle_txn_commit(env, conn, ri, wrapper);
        break;
    case HAM__WRAPPER__TYPE__TXN_ABORT_REQUEST:
        ham_trace(("txn_abort request"));
        handle_txn_abort(env, conn, ri, wrapper);
        break;
    case HAM__WRAPPER__TYPE__DB_CHECK_INTEGRITY_REQUEST:
        ham_trace(("db_check_integrity request"));
        handle_db_check_integrity(env, conn, ri, wrapper);
        break;
    case HAM__WRAPPER__TYPE__DB_GET_KEY_COUNT_REQUEST:
        ham_trace(("db_get_key_count request"));
        handle_db_get_key_count(env, conn, ri, wrapper);
        break;
    case HAM__WRAPPER__TYPE__DB_INSERT_REQUEST:
        ham_trace(("db_insert request"));
        handle_db_insert(env, conn, ri, wrapper);
        break;
    case HAM__WRAPPER__TYPE__DB_FIND_REQUEST:
        ham_trace(("db_find request"));
        handle_db_find(env, conn, ri, wrapper);
        break;
    case HAM__WRAPPER__TYPE__DB_ERASE_REQUEST:
        ham_trace(("db_erase request"));
        handle_db_erase(env, conn, ri, wrapper);
        break;
    case HAM__WRAPPER__TYPE__CURSOR_CREATE_REQUEST:
        ham_trace(("cursor_create request"));
        handle_cursor_create(env, conn, ri, wrapper);
        break;
    case HAM__WRAPPER__TYPE__CURSOR_CLONE_REQUEST:
        ham_trace(("cursor_clone request"));
        handle_cursor_clone(env, conn, ri, wrapper);
        break;
    case HAM__WRAPPER__TYPE__CURSOR_INSERT_REQUEST:
        ham_trace(("cursor_insert request"));
        handle_cursor_insert(env, conn, ri, wrapper);
        break;
    case HAM__WRAPPER__TYPE__CURSOR_ERASE_REQUEST:
        ham_trace(("cursor_erase request"));
        handle_cursor_erase(env, conn, ri, wrapper);
        break;
    case HAM__WRAPPER__TYPE__CURSOR_FIND_REQUEST:
        ham_trace(("cursor_find request"));
        handle_cursor_find(env, conn, ri, wrapper);
        break;
    case HAM__WRAPPER__TYPE__CURSOR_GET_DUPLICATE_COUNT_REQUEST:
        ham_trace(("cursor_get_duplicate_count request"));
        handle_cursor_get_duplicate_count(env, conn, ri, wrapper);
        break;
    case HAM__WRAPPER__TYPE__CURSOR_OVERWRITE_REQUEST:
        ham_trace(("cursor_overwrite request"));
        handle_cursor_overwrite(env, conn, ri, wrapper);
        break;
    case HAM__WRAPPER__TYPE__CURSOR_MOVE_REQUEST:
        ham_trace(("cursor_move request"));
        handle_cursor_move(env, conn, ri, wrapper);
        break;
    case HAM__WRAPPER__TYPE__CURSOR_CLOSE_REQUEST:
        ham_trace(("cursor_close request"));
        handle_cursor_close(env, conn, ri, wrapper);
        break;
    default:
        ham_trace(("ignoring unknown request"));
        break;
    }

#if 0
	printf("Method: [%s]\n", ri->request_method);
	printf("URI: [%s]\n", ri->uri);
	printf("HTTP version: [%d.%d]\n", ri->http_version_major, 
            ri->http_version_minor);

	for (i = 0; i < ri->num_headers; i++)
		printf("HTTP header [%s]: [%s]\n",
			 ri->http_headers[i].name,
			 ri->http_headers[i].value);

	printf("Query string: [%s]\n",
			ri->query_string ? ri->query_string: "");
	printf("POST data: [%.*s]\n",
			ri->post_data_len, ri->post_data);
	printf("Remote IP: [%lu]\n", ri->remote_ip);
	printf("Remote port: [%d]\n", ri->remote_port);
	printf("Remote user: [%s]\n",
			ri->remote_user ? ri->remote_user : "");
	printf("Hamsterdb url: [%s]\n", env->urlname);
#endif

bail:
    if (wrapper)
        proto_delete(wrapper);

    os_critsec_leave(&env->cs);
}

ham_status_t 
ham_srv_init(ham_srv_config_t *config, ham_srv_t **psrv)
{
    ham_srv_t *srv;
    char buf[32];
    sprintf(buf, "%d", (int)config->port);

    srv=(ham_srv_t *)malloc(sizeof(ham_srv_t));
    if (!srv)
        return (HAM_OUT_OF_MEMORY);
    memset(srv, 0, sizeof(*srv));

    srv->mg_ctxt=mg_start();
    mg_set_option(srv->mg_ctxt, "ports", buf);
    mg_set_option(srv->mg_ctxt, "dir_list", "no");
    if (config->access_log_path) {
        if (!mg_set_option(srv->mg_ctxt, "access_log", 
                    config->access_log_path)) {
            ham_log(("failed to write access log file '%s'", 
                        config->access_log_path));
            mg_stop(srv->mg_ctxt);
            free(srv);
            return (HAM_IO_ERROR);
        }
    }
    if (config->error_log_path) {
        if (!mg_set_option(srv->mg_ctxt, "error_log", 
                    config->error_log_path)) {
            ham_log(("failed to write access log file '%s'", 
                        config->access_log_path));
            mg_stop(srv->mg_ctxt);
            free(srv);
            return (HAM_IO_ERROR);
        }
    }

    *psrv=srv;
    return (HAM_SUCCESS);
}

ham_status_t 
ham_srv_add_env(ham_srv_t *srv, ham_env_t *env, const char *urlname)
{
    int i;

    /* search for a free handler */
    for (i=0; i<MAX_ENVIRONMENTS; i++) {
        if (!srv->environments[i].env) {
            srv->environments[i].env=env;
            srv->environments[i].urlname=strdup(urlname);
            os_critsec_init(&srv->environments[i].cs);
            break;
        }
    }

    if (i==MAX_ENVIRONMENTS)
        return (HAM_LIMITS_REACHED);
    
    mg_set_uri_callback(srv->mg_ctxt, urlname, 
                        request_handler, &srv->environments[i]);
    return (HAM_SUCCESS);
}

void
ham_srv_close(ham_srv_t *srv)
{
    int i;

    /* clean up Environment handlers */
    for (i=0; i<MAX_ENVIRONMENTS; i++) {
        if (srv->environments[i].env) {
            if (srv->environments[i].urlname)
                free(srv->environments[i].urlname);
            if (srv->environments[i].handles)
                free(srv->environments[i].handles);
            os_critsec_close(&srv->environments[i].cs);
            /* env will be closed by the caller */
            srv->environments[i].env=0;
        }
    }

    mg_stop(srv->mg_ctxt);
    free(srv);

    /* free libprotocol static data */
    proto_shutdown();
}

