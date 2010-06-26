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

#include <ham/types.h>
#include <mongoose/mongoose.h>

#include "hamserver.h"
#include "messages.pb-c.h"
#include "os.h"
#include "error.h"
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

typedef struct handle_t
{
    void *ptr;
    int type;
    ham_u32_t handle;
} handle_t;

struct hamserver_t
{
    /* the mongoose context structure */
    struct mg_context *mg_ctxt;

    /* handler for each Environment */
    struct env_t {
        ham_env_t *env;
        os_critsec_t cs;
        char *urlname;
        handle_t *handles;
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
        envh->handles=(handle_t *)realloc(envh->handles, 
                        sizeof(handle_t)*envh->handles_size);
        if (!envh->handles)
            exit(-1); /* TODO not so nice... */
    }

    ret=++envh->handles_ctr;
    ret=ret<<31;

    envh->handles[i].ptr=ptr;
    envh->handles[i].handle=ret|i;
    envh->handles[i].type=type;

    return (envh->handles[i].handle);
}

static void *
__get_handle(struct env_t *envh, ham_u64_t handle)
{
    handle_t *h=&envh->handles[handle&0xffffffff];
    assert(h->handle==handle);
    if (h->handle!=handle)
        exit(-1); /* TODO not so nice */
    return h->ptr;
}

static void
__remove_handle(struct env_t *envh, ham_u64_t handle)
{
    handle_t *h=&envh->handles[handle&0xffffffff];
    assert(h->handle==handle);
    if (h->handle!=handle)
        exit(-1); /* TODO not so nice */
    memset(h, 0, sizeof(*h));
}

static void
send_wrapper(ham_env_t *env, struct mg_connection *conn, Ham__Wrapper *wrapper)
{
    ham_u8_t *data;
    ham_size_t data_size;

    data_size=ham__wrapper__get_packed_size(wrapper);
    data=(ham_u8_t *)allocator_alloc(env_get_allocator(env), data_size);
    if (!data_size) {
        /* TODO send error */
        return;
    }
    ham__wrapper__pack(wrapper, data);

    printf("type %u: sending %d bytes\n", wrapper->type, data_size);
	mg_printf(conn, "%s", standard_reply);
    mg_write(conn, data, data_size);

    allocator_free(env_get_allocator(env), data);
}

static void
handle_connect(ham_env_t *env, struct mg_connection *conn, 
                const struct mg_request_info *ri, Ham__ConnectRequest *request)
{
    Ham__ConnectReply reply;
    Ham__Wrapper wrapper;

    ham_assert(request!=0, (""));

    ham__connect_reply__init(&reply);
    ham__wrapper__init(&wrapper);
    reply.status=0;
    wrapper.connect_reply=&reply;
    wrapper.type=HAM__WRAPPER__TYPE__CONNECT_REPLY;

    send_wrapper(env, conn, &wrapper);
}

static void
handle_env_get_parameters(ham_env_t *env, struct mg_connection *conn, 
                const struct mg_request_info *ri,
                Ham__EnvGetParametersRequest *request)
{
    Ham__EnvGetParametersReply reply;
    Ham__Wrapper wrapper;
    ham_size_t i;
    ham_parameter_t params[100]; /* 100 should be enough... */

    ham_assert(request!=0, (""));

    ham__env_get_parameters_reply__init(&reply);
    ham__wrapper__init(&wrapper);
    reply.status=0;
    wrapper.env_get_parameters_reply=&reply;
    wrapper.type=HAM__WRAPPER__TYPE__ENV_GET_PARAMETERS_REPLY;

    /* initialize the ham_parameters_t array */
    memset(&params[0], 0, sizeof(params));
    for (i=0; i<request->n_names && i<100; i++)
        params[i].name=request->names[i];

    /* and request the parameters from the Environment */
    reply.status=ham_env_get_parameters(env, &params[0]);
    if (reply.status) {
        send_wrapper(env, conn, &wrapper);
        return;
    }

    /* initialize the reply package */
    for (i=0; i<request->n_names; i++) {
        switch (params[i].name) {
        case HAM_PARAM_CACHESIZE:
            reply.cachesize=(int)params[i].value;
            reply.has_cachesize=1;
            break;
        case HAM_PARAM_PAGESIZE:
            reply.pagesize=(int)params[i].value;
            reply.has_pagesize=1;
            break;
        case HAM_PARAM_MAX_ENV_DATABASES:
            reply.max_env_databases=(int)params[i].value;
            reply.has_max_env_databases=1;
            break;
        case HAM_PARAM_GET_FLAGS:
            reply.flags=(int)params[i].value;
            reply.has_flags=1;
            break;
        case HAM_PARAM_GET_FILEMODE:
            reply.filemode=(int)params[i].value;
            reply.has_filemode=1;
            break;
        case HAM_PARAM_GET_FILENAME:
            reply.filename=(char *)(U64_TO_PTR(params[i].value));
            break;
        default:
            ham_trace(("unsupported parameter %u", (unsigned)params[i].name));
            break;
        }
    }

    send_wrapper(env, conn, &wrapper);
}

static void
handle_db_get_parameters(struct env_t *envh, struct mg_connection *conn, 
                const struct mg_request_info *ri,
                Ham__DbGetParametersRequest *request)
{
    ham_env_t *env=envh->env;
    ham_db_t *db;
    Ham__DbGetParametersReply reply;
    Ham__Wrapper wrapper;
    ham_size_t i;
    ham_parameter_t params[100]; /* 100 should be enough... */

    ham_assert(request!=0, (""));

    ham__db_get_parameters_reply__init(&reply);
    ham__wrapper__init(&wrapper);
    reply.status=0;
    wrapper.db_get_parameters_reply=&reply;
    wrapper.type=HAM__WRAPPER__TYPE__DB_GET_PARAMETERS_REPLY;

    /* initialize the ham_parameters_t array */
    memset(&params[0], 0, sizeof(params));
    for (i=0; i<request->n_names && i<100; i++)
        params[i].name=request->names[i];

    /* and request the parameters from the Environment */
    db=__get_handle(envh, request->db_handle);
    if (!db) {
        reply.status=HAM_INV_PARAMETER;
    }
    else {
        reply.status=ham_get_parameters(db, &params[0]);
    }
    if (reply.status) {
        send_wrapper(env, conn, &wrapper);
        return;
    }

    /* initialize the reply package */
    for (i=0; i<request->n_names; i++) {
        switch (params[i].name) {
        case HAM_PARAM_CACHESIZE:
            reply.cachesize=(int)params[i].value;
            reply.has_cachesize=1;
            break;
        case HAM_PARAM_PAGESIZE:
            reply.pagesize=(int)params[i].value;
            reply.has_pagesize=1;
            break;
        case HAM_PARAM_MAX_ENV_DATABASES:
            reply.max_env_databases=(int)params[i].value;
            reply.has_max_env_databases=1;
            break;
        case HAM_PARAM_GET_FLAGS:
            reply.flags=(int)params[i].value;
            reply.has_flags=1;
            break;
        case HAM_PARAM_GET_FILEMODE:
            reply.filemode=(int)params[i].value;
            reply.has_filemode=1;
            break;
        case HAM_PARAM_GET_FILENAME:
            reply.filename=(char *)(U64_TO_PTR(params[i].value));
            break;
        default:
            ham_trace(("unsupported parameter %u", (unsigned)params[i].name));
            break;
        }
    }

    send_wrapper(env, conn, &wrapper);
}

static void
handle_db_flush(struct env_t *envh, struct mg_connection *conn, 
                const struct mg_request_info *ri,
                Ham__DbFlushRequest *request)
{
    ham_env_t *env=envh->env;
    ham_db_t *db;
    Ham__DbFlushReply reply;
    Ham__Wrapper wrapper;

    ham_assert(request!=0, (""));

    ham__db_flush_reply__init(&reply);
    ham__wrapper__init(&wrapper);
    reply.status=0;
    wrapper.db_flush_reply=&reply;
    wrapper.type=HAM__WRAPPER__TYPE__DB_FLUSH_REPLY;

    /* and request the parameters from the Environment */
    db=__get_handle(envh, request->db_handle);
    if (!db) {
        reply.status=HAM_INV_PARAMETER;
    }
    else {
        reply.status=ham_flush(db, request->flags);
    }

    send_wrapper(env, conn, &wrapper);
}

static void
handle_env_get_database_names(ham_env_t *env, struct mg_connection *conn, 
                const struct mg_request_info *ri,
                Ham__EnvGetDatabaseNamesRequest *request)
{
    Ham__EnvGetDatabaseNamesReply reply;
    Ham__Wrapper wrapper;
    ham_size_t i, num_names=1024;
    ham_u16_t names[1024]; /* should be enough */

    ham_assert(request!=0, (""));

    ham__env_get_database_names_reply__init(&reply);
    ham__wrapper__init(&wrapper);
    reply.status=0;
    wrapper.env_get_database_names_reply=&reply;
    wrapper.type=HAM__WRAPPER__TYPE__ENV_GET_DATABASE_NAMES_REPLY;

    /* request the database names from the Environment */
    reply.status=ham_env_get_database_names(env, &names[0], &num_names);
    if (reply.status==0) {
        reply.n_names=num_names;
        reply.names=(unsigned *)allocator_alloc(env_get_allocator(env), 
                reply.n_names*sizeof(int));
        if (!reply.names) {
            /* TODO send error */
            return;
        }
        for (i=0; i<num_names; i++)
            reply.names[i]=names[i];
    }

    send_wrapper(env, conn, &wrapper);

    if (reply.names)
        allocator_free(env_get_allocator(env), reply.names);
    reply.names=0;
}

static void
handle_env_flush(ham_env_t *env, struct mg_connection *conn, 
                const struct mg_request_info *ri,
                Ham__EnvFlushRequest *request)
{
    Ham__EnvFlushReply reply;
    Ham__Wrapper wrapper;

    ham_assert(request!=0, (""));

    ham__env_flush_reply__init(&reply);
    ham__wrapper__init(&wrapper);
    reply.status=0;
    wrapper.env_flush_reply=&reply;
    wrapper.type=HAM__WRAPPER__TYPE__ENV_FLUSH_REPLY;

    /* request the database names from the Environment */
    reply.status=ham_env_flush(env, request->flags);

    send_wrapper(env, conn, &wrapper);
}

static void
handle_env_rename(ham_env_t *env, struct mg_connection *conn, 
                const struct mg_request_info *ri,
                Ham__EnvRenameRequest *request)
{
    Ham__EnvRenameReply reply;
    Ham__Wrapper wrapper;

    ham_assert(request!=0, (""));

    ham__env_rename_reply__init(&reply);
    ham__wrapper__init(&wrapper);
    reply.status=0;
    wrapper.env_rename_reply=&reply;
    wrapper.type=HAM__WRAPPER__TYPE__ENV_RENAME_REPLY;

    /* request the database names from the Environment */
    reply.status=ham_env_rename_db(env, request->oldname, request->newname,
                            request->flags);

    send_wrapper(env, conn, &wrapper);
}

static void
handle_env_create_db(struct env_t *envh, ham_env_t *env, 
                struct mg_connection *conn, const struct mg_request_info *ri,
                Ham__EnvCreateDbRequest *request)
{
    ham_db_t *db;
    Ham__EnvCreateDbReply reply;
    Ham__Wrapper wrapper;
    ham_parameter_t params[100]={{0, 0}};

    ham_assert(request!=0, (""));

    ham__env_create_db_reply__init(&reply);
    ham__wrapper__init(&wrapper);
    reply.status=0;
    reply.db_flags=request->flags;
    wrapper.env_create_db_reply=&reply;
    wrapper.type=HAM__WRAPPER__TYPE__ENV_CREATE_DB_REPLY;

    /* convert parameters */
    ham_assert(request->n_param_values==request->n_param_names, (""));
    ham_assert(request->n_param_values<100, (""));

    /* create the database */
    ham_new(&db);
    reply.status=ham_env_create_db(env, db, request->dbname, request->flags,
                            &params[0]);

    if (reply.status==0) {
        /* allocate a new database handle in the Env wrapper structure */
        reply.db_handle=__store_handle(envh, db, HANDLE_TYPE_DATABASE);
    }
    else {
        ham_delete(db);
    }

    send_wrapper(env, conn, &wrapper);
}

static void
handle_env_open_db(struct env_t *envh, ham_env_t *env, 
                struct mg_connection *conn, const struct mg_request_info *ri,
                Ham__EnvOpenDbRequest *request)
{
    unsigned i;
    ham_db_t *db=0;
    Ham__EnvOpenDbReply reply;
    Ham__Wrapper wrapper;
    ham_parameter_t params[100]={{0, 0}};

    ham_assert(request!=0, (""));

    ham__env_open_db_reply__init(&reply);
    ham__wrapper__init(&wrapper);
    reply.status=0;
    wrapper.env_open_db_reply=&reply;
    wrapper.type=HAM__WRAPPER__TYPE__ENV_OPEN_DB_REPLY;

    /* convert parameters */
    ham_assert(request->n_param_values==request->n_param_names, (""));
    ham_assert(request->n_param_values<100, (""));

    /* check if the database is already open */
    for (i=0; i<envh->handles_size; i++) {
        if (envh->handles[i].ptr!=0) {
            if (envh->handles[i].type==HANDLE_TYPE_DATABASE) {
                db=envh->handles[i].ptr;
                if (db_get_dbname(db)==request->dbname)
                    break;
                else
                    db=0;
            }
        }
    }

    /* if not found: open the database */
    if (!db) {
        ham_new(&db);
        reply.status=ham_env_open_db(env, db, request->dbname, request->flags,
                            &params[0]);

        if (reply.status==0) {
            /* allocate a new database handle in the Env wrapper structure */
            reply.db_handle=__store_handle(envh, db, HANDLE_TYPE_DATABASE);
        }
        else {
            ham_delete(db);
        }
    }

    if (db) {
        /* return database flags; do not use ham_get_flags() because it
         * returns different flags! */
        reply.db_flags=db->_rt_flags;
    }

    send_wrapper(env, conn, &wrapper);
}

static void
handle_env_erase_db(ham_env_t *env, struct mg_connection *conn, 
                const struct mg_request_info *ri,
                Ham__EnvEraseDbRequest *request)
{
    Ham__EnvEraseDbReply reply;
    Ham__Wrapper wrapper;

    ham_assert(request!=0, (""));

    ham__env_erase_db_reply__init(&reply);
    ham__wrapper__init(&wrapper);
    reply.status=0;
    wrapper.env_erase_db_reply=&reply;
    wrapper.type=HAM__WRAPPER__TYPE__ENV_ERASE_DB_REPLY;

    reply.status=ham_env_erase_db(env, request->name, request->flags);

    send_wrapper(env, conn, &wrapper);
}

static void
handle_db_close(struct env_t *envh, struct mg_connection *conn, 
                const struct mg_request_info *ri,
                Ham__DbCloseRequest *request)
{
    Ham__DbCloseReply reply;
    Ham__Wrapper wrapper;
    ham_db_t *db;
    ham_env_t *env=envh->env;

    ham_assert(request!=0, (""));

    ham__db_close_reply__init(&reply);
    ham__wrapper__init(&wrapper);
    reply.status=0;
    wrapper.db_close_reply=&reply;
    wrapper.type=HAM__WRAPPER__TYPE__DB_CLOSE_REPLY;

    db=__get_handle(envh, request->db_handle);
    if (!db) {
        /* accept this - most likely the database was already closed by
         * another process */
        reply.status=0;
    }
    else {
        reply.status=ham_close(db, request->flags);
        if (reply.status==0)
            __remove_handle(envh, request->db_handle);
    }

    send_wrapper(env, conn, &wrapper);
}

static void
handle_txn_begin(struct env_t *envh, struct mg_connection *conn, 
                const struct mg_request_info *ri,
                Ham__TxnBeginRequest *request)
{
    Ham__TxnBeginReply reply;
    Ham__Wrapper wrapper;
    ham_txn_t *txn;
    ham_db_t *db;
    ham_env_t *env=envh->env;

    ham_assert(request!=0, (""));

    ham__txn_begin_reply__init(&reply);
    ham__wrapper__init(&wrapper);
    reply.status=0;
    wrapper.txn_begin_reply=&reply;
    wrapper.type=HAM__WRAPPER__TYPE__TXN_BEGIN_REPLY;

    db=__get_handle(envh, request->db_handle);
    if (!db) {
        reply.status=HAM_INV_PARAMETER;
    }
    else {
        reply.status=ham_txn_begin(&txn, db, request->flags);
    }

    if (reply.status==0)
        reply.txn_handle=__store_handle(envh, txn, HANDLE_TYPE_TRANSACTION);

    send_wrapper(env, conn, &wrapper);
}

static void
handle_txn_commit(struct env_t *envh, struct mg_connection *conn, 
                const struct mg_request_info *ri,
                Ham__TxnCommitRequest *request)
{
    Ham__TxnCommitReply reply;
    Ham__Wrapper wrapper;
    ham_txn_t *txn;
    ham_env_t *env=envh->env;

    ham_assert(request!=0, (""));

    ham__txn_commit_reply__init(&reply);
    ham__wrapper__init(&wrapper);
    reply.status=0;
    wrapper.txn_commit_reply=&reply;
    wrapper.type=HAM__WRAPPER__TYPE__TXN_COMMIT_REPLY;

    txn=__get_handle(envh, request->txn_handle);
    if (!txn) {
        reply.status=HAM_INV_PARAMETER;
    }
    else {
        reply.status=ham_txn_commit(txn, request->flags);
    }

    send_wrapper(env, conn, &wrapper);
}

static void
handle_txn_abort(struct env_t *envh, struct mg_connection *conn, 
                const struct mg_request_info *ri,
                Ham__TxnAbortRequest *request)
{
    Ham__TxnAbortReply reply;
    Ham__Wrapper wrapper;
    ham_txn_t *txn;
    ham_env_t *env=envh->env;

    ham_assert(request!=0, (""));

    ham__txn_abort_reply__init(&reply);
    ham__wrapper__init(&wrapper);
    reply.status=0;
    wrapper.txn_abort_reply=&reply;
    wrapper.type=HAM__WRAPPER__TYPE__TXN_ABORT_REPLY;

    txn=__get_handle(envh, request->txn_handle);
    if (!txn) {
        reply.status=HAM_INV_PARAMETER;
    }
    else {
        reply.status=ham_txn_abort(txn, request->flags);
    }

    send_wrapper(env, conn, &wrapper);
}

static void
handle_db_check_integrity(struct env_t *envh, struct mg_connection *conn, 
                const struct mg_request_info *ri,
                Ham__DbCheckIntegrityRequest *request)
{
    Ham__DbCheckIntegrityReply reply;
    Ham__Wrapper wrapper;
    ham_txn_t *txn=0;
    ham_db_t *db;

    ham_assert(request!=0, (""));

    ham__db_check_integrity_reply__init(&reply);
    ham__wrapper__init(&wrapper);
    reply.status=0;
    wrapper.db_check_integrity_reply=&reply;
    wrapper.type=HAM__WRAPPER__TYPE__DB_CHECK_INTEGRITY_REPLY;

    if (request->txn_handle) {
        txn=__get_handle(envh, request->txn_handle);
        if (!txn) {
            reply.status=HAM_INV_PARAMETER;
        }
    }

    if (reply.status==0) {
        db=__get_handle(envh, request->db_handle);
        if (!db) {
            reply.status=HAM_INV_PARAMETER;
        }
        else {
            reply.status=ham_check_integrity(db, txn);
        }
    }

    send_wrapper(envh->env, conn, &wrapper);
}

static void
handle_db_get_key_count(struct env_t *envh, struct mg_connection *conn, 
                const struct mg_request_info *ri,
                Ham__DbGetKeyCountRequest *request)
{
    Ham__DbGetKeyCountReply reply;
    Ham__Wrapper wrapper;
    ham_txn_t *txn=0;
    ham_db_t *db;

    ham_assert(request!=0, (""));

    ham__db_get_key_count_reply__init(&reply);
    ham__wrapper__init(&wrapper);
    reply.status=0;
    wrapper.db_get_key_count_reply=&reply;
    wrapper.type=HAM__WRAPPER__TYPE__DB_GET_KEY_COUNT_REPLY;

    if (request->txn_handle) {
        txn=__get_handle(envh, request->txn_handle);
        if (!txn) {
            reply.status=HAM_INV_PARAMETER;
        }
    }

    if (reply.status==0) {
        db=__get_handle(envh, request->db_handle);
        if (!db) {
            reply.status=HAM_INV_PARAMETER;
        }
        else {
            reply.status=ham_get_key_count(db, txn, request->flags, 
                    &reply.keycount);
        }
    }

    send_wrapper(envh->env, conn, &wrapper);
}

static void
handle_db_insert(struct env_t *envh, struct mg_connection *conn, 
                const struct mg_request_info *ri,
                Ham__DbInsertRequest *request)
{
    Ham__DbInsertReply reply;
    Ham__Wrapper wrapper;
    Ham__Key replykey;
    ham_txn_t *txn=0;
    ham_db_t *db;

    ham_assert(request!=0, (""));

    ham__db_insert_reply__init(&reply);
    ham__key__init(&replykey);
    ham__wrapper__init(&wrapper);
    reply.status=0;
    wrapper.db_insert_reply=&reply;
    wrapper.type=HAM__WRAPPER__TYPE__DB_INSERT_REPLY;

    if (request->txn_handle) {
        txn=__get_handle(envh, request->txn_handle);
        if (!txn) {
            reply.status=HAM_INV_PARAMETER;
        }
    }

    if (reply.status==0) {
        db=__get_handle(envh, request->db_handle);
        if (!db) {
            reply.status=HAM_INV_PARAMETER;
        }
        else {
            ham_key_t key;
            ham_record_t rec;

            memset(&key, 0, sizeof(key));
            key.data=request->key->data.data;
            key.size=request->key->data.len;
            key.flags=request->key->flags & (~HAM_KEY_USER_ALLOC);

            memset(&rec, 0, sizeof(rec));
            rec.data=request->record->data.data;
            rec.size=request->record->data.len;
            rec.partial_size=request->record->partial_size;
            rec.partial_offset=request->record->partial_offset;
            rec.flags=request->record->flags & (~HAM_RECORD_USER_ALLOC);
            reply.status=ham_insert(db, txn, &key, &rec, request->flags);

            /* recno: return the modified key */
            if ((reply.status==0) && (ham_get_flags(db)&HAM_RECORD_NUMBER)) {
                ham_assert(key.size==sizeof(ham_offset_t), (""));
                reply.key=&replykey;
                replykey.data.data=key.data;
                replykey.data.len=key.size;
            }
        }
    }

    send_wrapper(envh->env, conn, &wrapper);
}

static void
handle_db_find(struct env_t *envh, struct mg_connection *conn, 
                const struct mg_request_info *ri,
                Ham__DbFindRequest *request)
{
    Ham__DbFindReply reply;
    Ham__Wrapper wrapper;
    Ham__Key replykey;
    Ham__Record replyrec;
    ham_txn_t *txn=0;
    ham_db_t *db;

    ham_assert(request!=0, (""));

    ham__db_find_reply__init(&reply);
    ham__key__init(&replykey);
    ham__record__init(&replyrec);
    ham__wrapper__init(&wrapper);
    reply.status=0;
    wrapper.db_find_reply=&reply;
    wrapper.type=HAM__WRAPPER__TYPE__DB_FIND_REPLY;

    if (request->txn_handle) {
        txn=__get_handle(envh, request->txn_handle);
        if (!txn) {
            reply.status=HAM_INV_PARAMETER;
        }
    }

    if (reply.status==0) {
        db=__get_handle(envh, request->db_handle);
        if (!db) {
            reply.status=HAM_INV_PARAMETER;
        }
        else {
            ham_key_t key;
            ham_record_t rec;

            memset(&key, 0, sizeof(key));
            key.data=request->key->data.data;
            key.size=request->key->data.len;
            key.flags=request->key->flags & (~HAM_KEY_USER_ALLOC);

            memset(&rec, 0, sizeof(rec));
            rec.data=request->record->data.data;
            rec.size=request->record->data.len;
            rec.flags=request->record->flags & (~HAM_RECORD_USER_ALLOC);
            rec.partial_size=request->record->partial_size;
            rec.partial_offset=request->record->partial_offset;
            reply.status=ham_find(db, txn, &key, &rec, request->flags);

            if (reply.status==0) {
                /* approx matching: key->_flags was modified! */
                if (key._flags) {
                    reply.key=&replykey;
                    replykey.intflags=key._flags;
                }
                /* in any case - return the record */
                reply.record=&replyrec;
                replyrec.data.data=rec.data;
                replyrec.data.len=rec.size;
                replyrec.flags=rec.flags;
            }
        }
    }

    send_wrapper(envh->env, conn, &wrapper);
}

static void
handle_db_erase(struct env_t *envh, struct mg_connection *conn, 
                const struct mg_request_info *ri,
                Ham__DbEraseRequest *request)
{
    Ham__DbEraseReply reply;
    Ham__Wrapper wrapper;
    ham_txn_t *txn=0;
    ham_db_t *db;

    ham_assert(request!=0, (""));

    ham__db_erase_reply__init(&reply);
    ham__wrapper__init(&wrapper);
    reply.status=0;
    wrapper.db_erase_reply=&reply;
    wrapper.type=HAM__WRAPPER__TYPE__DB_ERASE_REPLY;

    if (request->txn_handle) {
        txn=__get_handle(envh, request->txn_handle);
        if (!txn) {
            reply.status=HAM_INV_PARAMETER;
        }
    }

    if (reply.status==0) {
        db=__get_handle(envh, request->db_handle);
        if (!db) {
            reply.status=HAM_INV_PARAMETER;
        }
        else {
            ham_key_t key;

            memset(&key, 0, sizeof(key));
            key.data=request->key->data.data;
            key.size=request->key->data.len;
            key.flags=request->key->flags & (~HAM_KEY_USER_ALLOC);

            reply.status=ham_erase(db, txn, &key, request->flags);
        }
    }

    send_wrapper(envh->env, conn, &wrapper);
}

static void
handle_cursor_create(struct env_t *envh, struct mg_connection *conn, 
                const struct mg_request_info *ri,
                Ham__CursorCreateRequest *request)
{
    Ham__CursorCreateReply reply;
    Ham__Wrapper wrapper;
    ham_txn_t *txn=0;
    ham_db_t *db;
    ham_cursor_t *cursor;

    ham_assert(request!=0, (""));

    ham__cursor_create_reply__init(&reply);
    ham__wrapper__init(&wrapper);
    reply.status=0;
    wrapper.cursor_create_reply=&reply;
    wrapper.type=HAM__WRAPPER__TYPE__CURSOR_CREATE_REPLY;

    if (request->txn_handle) {
        txn=__get_handle(envh, request->txn_handle);
        if (!txn) {
            reply.status=HAM_INV_PARAMETER;
            goto bail;
        }
    }

    db=__get_handle(envh, request->db_handle);
    if (!db) {
        reply.status=HAM_INV_PARAMETER;
        goto bail;
    }

    /* create the cursor */
    reply.status=ham_cursor_create(db, txn, request->flags, &cursor);
    if (reply.status==0) {
        /* allocate a new handle in the Env wrapper structure */
        reply.cursor_handle=__store_handle(envh, cursor, HANDLE_TYPE_CURSOR);
    }

bail:
    send_wrapper(envh->env, conn, &wrapper);
}

static void
handle_cursor_clone(struct env_t *envh, struct mg_connection *conn, 
                const struct mg_request_info *ri,
                Ham__CursorCloneRequest *request)
{
    Ham__CursorCloneReply reply;
    Ham__Wrapper wrapper;
    ham_cursor_t *src;
    ham_cursor_t *dest;

    ham_assert(request!=0, (""));

    ham__cursor_clone_reply__init(&reply);
    ham__wrapper__init(&wrapper);
    reply.status=0;
    wrapper.cursor_clone_reply=&reply;
    wrapper.type=HAM__WRAPPER__TYPE__CURSOR_CLONE_REPLY;

    src=__get_handle(envh, request->cursor_handle);
    if (!src) {
        reply.status=HAM_INV_PARAMETER;
        goto bail;
    }

    /* clone the cursor */
    reply.status=ham_cursor_clone(src, &dest);
    if (reply.status==0) {
        /* allocate a new handle in the Env wrapper structure */
        reply.cursor_handle=__store_handle(envh, dest, HANDLE_TYPE_CURSOR);
    }

bail:
    send_wrapper(envh->env, conn, &wrapper);
}

static void
handle_cursor_close(struct env_t *envh, struct mg_connection *conn, 
                const struct mg_request_info *ri,
                Ham__CursorCloseRequest *request)
{
    Ham__CursorCloseReply reply;
    Ham__Wrapper wrapper;
    ham_cursor_t *cursor;

    ham_assert(request!=0, (""));

    ham__cursor_close_reply__init(&reply);
    ham__wrapper__init(&wrapper);
    reply.status=0;
    wrapper.cursor_close_reply=&reply;
    wrapper.type=HAM__WRAPPER__TYPE__CURSOR_CLOSE_REPLY;

    cursor=__get_handle(envh, request->cursor_handle);
    if (!cursor) {
        reply.status=HAM_INV_PARAMETER;
        goto bail;
    }

    /* close the cursor */
    reply.status=ham_cursor_close(cursor);
    if (reply.status==0) {
        /* remove the handle from the Env wrapper structure */
        __remove_handle(envh, request->cursor_handle);
    }

bail:
    send_wrapper(envh->env, conn, &wrapper);
}

static void
request_handler(struct mg_connection *conn, const struct mg_request_info *ri,
                void *user_data)
{
    Ham__Wrapper *wrapper;
    struct env_t *env=(struct env_t *)user_data;

    os_critsec_enter(&env->cs);

    wrapper=ham__wrapper__unpack(0, ri->post_data_len, 
            (ham_u8_t *)ri->post_data);
    if (!wrapper) {
        printf("failed to unpack wrapper (%d bytes)\n", ri->post_data_len);
        /* TODO send error */
        goto bail;   
    }

    switch (wrapper->type) {
    case HAM__WRAPPER__TYPE__CONNECT_REQUEST:
        ham_trace(("connect request"));
        handle_connect(env->env, conn, ri, wrapper->connect_request);
        break;
    case HAM__WRAPPER__TYPE__ENV_GET_PARAMETERS_REQUEST:
        ham_trace(("env_get_parameters request"));
        handle_env_get_parameters(env->env, conn, ri, 
                    wrapper->env_get_parameters_request);
        break;
    case HAM__WRAPPER__TYPE__ENV_GET_DATABASE_NAMES_REQUEST:
        ham_trace(("env_get_database_names request"));
        handle_env_get_database_names(env->env, conn, ri, 
                    wrapper->env_get_database_names_request);
        break;
    case HAM__WRAPPER__TYPE__ENV_FLUSH_REQUEST:
        ham_trace(("env_flush request"));
        handle_env_flush(env->env, conn, ri, wrapper->env_flush_request);
        break;
    case HAM__WRAPPER__TYPE__ENV_RENAME_REQUEST:
        ham_trace(("env_rename request"));
        handle_env_rename(env->env, conn, ri, wrapper->env_rename_request);
        break;
    case HAM__WRAPPER__TYPE__ENV_CREATE_DB_REQUEST:
        ham_trace(("env_create_db request"));
        handle_env_create_db(env, env->env, conn, ri, 
                            wrapper->env_create_db_request);
        break;
    case HAM__WRAPPER__TYPE__ENV_OPEN_DB_REQUEST:
        ham_trace(("env_open_db request"));
        handle_env_open_db(env, env->env, conn, ri, 
                            wrapper->env_open_db_request);
        break;
    case HAM__WRAPPER__TYPE__ENV_ERASE_DB_REQUEST:
        ham_trace(("env_erase_db request"));
        handle_env_erase_db(env->env, conn, ri, wrapper->env_erase_db_request);
        break;
    case HAM__WRAPPER__TYPE__DB_CLOSE_REQUEST:
        ham_trace(("db_close request"));
        handle_db_close(env, conn, ri, wrapper->db_close_request);
        break;
    case HAM__WRAPPER__TYPE__DB_GET_PARAMETERS_REQUEST:
        ham_trace(("db_get_parameters request"));
        handle_db_get_parameters(env, conn, ri, 
                            wrapper->db_get_parameters_request);
        break;
    case HAM__WRAPPER__TYPE__DB_FLUSH_REQUEST:
        ham_trace(("db_flush request"));
        handle_db_flush(env, conn, ri, wrapper->db_flush_request);
        break;
    case HAM__WRAPPER__TYPE__TXN_BEGIN_REQUEST:
        ham_trace(("txn_begin request"));
        handle_txn_begin(env, conn, ri, wrapper->txn_begin_request);
        break;
    case HAM__WRAPPER__TYPE__TXN_COMMIT_REQUEST:
        ham_trace(("txn_commit request"));
        handle_txn_commit(env, conn, ri, wrapper->txn_commit_request);
        break;
    case HAM__WRAPPER__TYPE__TXN_ABORT_REQUEST:
        ham_trace(("txn_abort request"));
        handle_txn_abort(env, conn, ri, wrapper->txn_abort_request);
        break;
    case HAM__WRAPPER__TYPE__DB_CHECK_INTEGRITY_REQUEST:
        ham_trace(("db_check_integrity request"));
        handle_db_check_integrity(env, conn, ri, 
                wrapper->db_check_integrity_request);
        break;
    case HAM__WRAPPER__TYPE__DB_GET_KEY_COUNT_REQUEST:
        ham_trace(("db_get_key_count request"));
        handle_db_get_key_count(env, conn, ri, 
                wrapper->db_get_key_count_request);
        break;
    case HAM__WRAPPER__TYPE__DB_INSERT_REQUEST:
        ham_trace(("db_insert request"));
        handle_db_insert(env, conn, ri, 
                wrapper->db_insert_request);
        break;
    case HAM__WRAPPER__TYPE__DB_FIND_REQUEST:
        ham_trace(("db_find request"));
        handle_db_find(env, conn, ri, 
                wrapper->db_find_request);
        break;
    case HAM__WRAPPER__TYPE__DB_ERASE_REQUEST:
        ham_trace(("db_erase request"));
        handle_db_erase(env, conn, ri, 
                wrapper->db_erase_request);
        break;
    case HAM__WRAPPER__TYPE__CURSOR_CREATE_REQUEST:
        ham_trace(("cursor_create request"));
        handle_cursor_create(env, conn, ri, 
                wrapper->cursor_create_request);
        break;
    case HAM__WRAPPER__TYPE__CURSOR_CLONE_REQUEST:
        ham_trace(("cursor_clone request"));
        handle_cursor_clone(env, conn, ri, 
                wrapper->cursor_clone_request);
        break;
    case HAM__WRAPPER__TYPE__CURSOR_CLOSE_REQUEST:
        ham_trace(("cursor_close request"));
        handle_cursor_close(env, conn, ri, 
                wrapper->cursor_close_request);
        break;
    default:
        /* TODO send error */
        ham_trace(("unknown request"));
        goto bail;   
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
        ham__wrapper__free_unpacked(wrapper, 0);

    os_critsec_leave(&env->cs);
}

ham_bool_t 
hamserver_init(hamserver_config_t *config, hamserver_t **psrv)
{
    hamserver_t *srv;
    char buf[32];
    sprintf(buf, "%d", (int)config->port);

    srv=(hamserver_t *)malloc(sizeof(hamserver_t));
    if (!srv)
        return (HAM_FALSE);
    memset(srv, 0, sizeof(*srv));

    srv->mg_ctxt=mg_start();
    mg_set_option(srv->mg_ctxt, "ports", buf);

    *psrv=srv;
    return (HAM_TRUE);
}

ham_bool_t 
hamserver_add_env(hamserver_t *srv, ham_env_t *env, const char *urlname)
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
        return (HAM_FALSE);
    
    mg_set_uri_callback(srv->mg_ctxt, urlname, 
                        request_handler, &srv->environments[i]);
    return (HAM_TRUE);
}

void
hamserver_close(hamserver_t *srv)
{
    int i;

    /* clean up Environment handlers */
    for (i=0; i<MAX_ENVIRONMENTS; i++) {
        if (srv->environments[i].env) {
            os_critsec_close(&srv->environments[i].cs);
            free(srv->environments[i].urlname);
            /* env will be closed by the caller */
            srv->environments[i].env=0;
        }
    }

    mg_stop(srv->mg_ctxt);
    free(srv);
}

