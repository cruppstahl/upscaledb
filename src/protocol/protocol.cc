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

#include "protocol.h"
#include "../error.h"
#include "../mem.h"
#include "../endian.h"
#include "../db.h"

#include "messages.pb.h"

using namespace ham;

proto_wrapper_t *
proto_unpack(ham_size_t size, const ham_u8_t *buf)
{
    if (*(ham_u32_t *)&buf[0]!=ham_db2h32(HAM_TRANSFER_MAGIC_V1)) {
        ham_trace(("invalid protocol version"));
        return (0);
    }

    Wrapper *w=new Wrapper;
    if (!w->ParseFromArray(buf+8, size-8)) {
        delete w;
        return (0);
    }
    return ((proto_wrapper_t *)w);
}

void
proto_delete(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w)
        delete (w);
}

ham_bool_t
proto_pack(proto_wrapper_t *wrapper, mem_allocator_t *alloc,
            ham_u8_t **data, ham_size_t *size)
{
    Wrapper *w=(Wrapper *)wrapper;
    ham_size_t packed_size=w->ByteSize();
    /* we need 8 more bytes for magic and size */
    ham_u8_t *p=(ham_u8_t *)allocator_alloc(alloc, packed_size+8);
    if (!p)
        return (HAM_FALSE);

    /* write the magic and the payload size of the packed structure */
    *(ham_u32_t *)&p[0]=ham_h2db32(HAM_TRANSFER_MAGIC_V1);
    *(ham_u32_t *)&p[4]=ham_h2db32(packed_size);

    /* now write the packed structure */
    if (!w->SerializeToArray(&p[8], packed_size)) {
        allocator_free(alloc, p);
        return (HAM_FALSE);
    }
    
    *data=p;
    *size=packed_size+8;
    return (HAM_TRUE);
}

ham_u32_t
proto_get_type(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->type());
}

void
proto_shutdown(void)
{
    google::protobuf::ShutdownProtobufLibrary();
}

proto_wrapper_t *
proto_init_connect_request(const char *filename)
{
    Wrapper *w=new Wrapper();
    w->set_type(Wrapper::CONNECT_REQUEST);
    w->mutable_connect_request()->set_path(filename);
    return ((proto_wrapper_t *)w);
}

ham_bool_t
proto_has_connect_request(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->type()==Wrapper::CONNECT_REQUEST) {
        ham_assert(w->has_connect_request()==true, (""));
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_connect_request()==false, (""));
        return (HAM_FALSE);
    }
}

proto_wrapper_t *
proto_init_connect_reply(ham_u32_t status, ham_u32_t env_flags)
{
    Wrapper *w=new Wrapper();
    w->set_type(Wrapper::CONNECT_REPLY);
    w->mutable_connect_reply()->set_status(status);
    w->mutable_connect_reply()->set_env_flags(env_flags);
    return ((proto_wrapper_t *)w);
}

ham_bool_t
proto_has_connect_reply(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->type()==Wrapper::CONNECT_REPLY) {
        ham_assert(w->has_connect_reply()==true, (""));
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_connect_reply()==false, (""));
        return (HAM_FALSE);
    }
}

ham_u32_t
proto_connect_reply_get_status(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->connect_reply().status());
}

ham_u32_t
proto_connect_reply_get_env_flags(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->connect_reply().env_flags());
}

proto_wrapper_t *
proto_init_env_rename_request(ham_u16_t oldname, ham_u16_t newname,     
                ham_u32_t flags)
{
    Wrapper *w=new Wrapper();
    w->set_type(Wrapper::ENV_RENAME_REQUEST);
    w->mutable_env_rename_request()->set_oldname(oldname);
    w->mutable_env_rename_request()->set_newname(newname);
    w->mutable_env_rename_request()->set_flags(flags);
    return ((proto_wrapper_t *)w);
}

ham_u32_t
proto_env_rename_request_get_oldname(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->env_rename_request().oldname());
}

ham_u32_t
proto_env_rename_request_get_newname(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->env_rename_request().newname());
}

ham_u32_t
proto_env_rename_request_get_flags(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->env_rename_request().flags());
}

ham_bool_t
proto_has_env_rename_request(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->type()==Wrapper::ENV_RENAME_REQUEST) {
        ham_assert(w->has_env_rename_request()==true, (""));
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_env_rename_request()==false, (""));
        return (HAM_FALSE);
    }
}

proto_wrapper_t *
proto_init_env_rename_reply(ham_status_t status)
{
    Wrapper *w=new Wrapper();
    w->set_type(Wrapper::ENV_RENAME_REPLY);
    w->mutable_env_rename_reply()->set_status(status);
    return ((proto_wrapper_t *)w);
}

ham_bool_t
proto_has_env_rename_reply(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->type()==Wrapper::ENV_RENAME_REPLY) {
        ham_assert(w->has_env_rename_reply()==true, (""));
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_env_rename_reply()==false, (""));
        return (HAM_FALSE);
    }
}

ham_u32_t
proto_env_rename_reply_get_status(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->env_rename_reply().status());
}

proto_wrapper_t *
proto_init_env_erase_db_request(ham_u16_t name, ham_u32_t flags)
{
    Wrapper *w=new Wrapper();
    w->set_type(Wrapper::ENV_ERASE_DB_REQUEST);
    w->mutable_env_erase_db_request()->set_name(name);
    w->mutable_env_erase_db_request()->set_flags(flags);
    return ((proto_wrapper_t *)w);
}

ham_bool_t
proto_has_env_erase_db_request(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->type()==Wrapper::ENV_ERASE_DB_REQUEST) {
        ham_assert(w->has_env_erase_db_request()==true, (""));
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_env_erase_db_request()==false, (""));
        return (HAM_FALSE);
    }
}

ham_u16_t
proto_env_erase_db_request_get_dbname(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->env_erase_db_request().name());
}

ham_u32_t
proto_env_erase_db_request_get_flags(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->env_erase_db_request().flags());
}

proto_wrapper_t *
proto_init_env_erase_db_reply(ham_status_t status)
{
    Wrapper *w=new Wrapper();
    w->set_type(Wrapper::ENV_ERASE_DB_REPLY);
    w->mutable_env_erase_db_reply()->set_status(status);
    return ((proto_wrapper_t *)w);
}

ham_bool_t
proto_has_env_erase_db_reply(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->type()==Wrapper::ENV_ERASE_DB_REPLY) {
        ham_assert(w->has_env_erase_db_reply()==true, (""));
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_env_erase_db_reply()==false, (""));
        return (HAM_FALSE);
    }
}

ham_u32_t
proto_env_erase_db_reply_get_status(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->env_erase_db_reply().status());
}

proto_wrapper_t *
proto_init_env_get_database_names_request(void)
{
    Wrapper *w=new Wrapper();
    w->set_type(Wrapper::ENV_GET_DATABASE_NAMES_REQUEST);
    w->mutable_env_get_database_names_request(); /* create structure */
    return ((proto_wrapper_t *)w);
}

ham_bool_t
proto_has_env_get_database_names_request(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->type()==Wrapper::ENV_GET_DATABASE_NAMES_REQUEST) {
        ham_assert(w->has_env_get_database_names_request()==true, (""));
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_env_get_database_names_request()==false, (""));
        return (HAM_FALSE);
    }
}

proto_wrapper_t *
proto_init_env_get_database_names_reply(ham_status_t status, ham_u16_t *names,
                ham_size_t num_names)
{
    Wrapper *w=new Wrapper();
    w->set_type(Wrapper::ENV_GET_DATABASE_NAMES_REPLY);
    w->mutable_env_get_database_names_reply()->set_status(status);
    for (ham_u32_t i=0; i<num_names; i++)
        w->mutable_env_get_database_names_reply()->add_names(names[i]);
    return ((proto_wrapper_t *)w);
}

ham_bool_t
proto_has_env_get_database_names_reply(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->type()==Wrapper::ENV_GET_DATABASE_NAMES_REPLY) {
        ham_assert(w->has_env_get_database_names_reply()==true, (""));
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_env_get_database_names_reply()==false, (""));
        return (HAM_FALSE);
    }
}

ham_u32_t
proto_env_get_database_names_reply_get_status(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->env_get_database_names_reply().status());
}

ham_u32_t
proto_env_get_database_names_reply_get_names_size(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->env_get_database_names_reply().names_size());
}

ham_u32_t *
proto_env_get_database_names_reply_get_names(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->mutable_env_get_database_names_reply()->mutable_names()->mutable_data());
}

proto_wrapper_t *
proto_init_env_get_parameters_request(ham_u32_t *names, ham_u32_t names_size)
{
    Wrapper *w=new Wrapper();
    w->set_type(Wrapper::ENV_GET_PARAMETERS_REQUEST);
    for (ham_u32_t i=0; i<names_size; i++)
        w->mutable_env_get_parameters_request()->add_names(names[i]);
    return ((proto_wrapper_t *)w);
}

ham_u32_t *
proto_env_get_parameters_request_get_names(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->mutable_env_get_parameters_request()->mutable_names()->mutable_data());
}

ham_size_t
proto_env_get_parameters_request_get_names_size(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->env_get_parameters_request().names().size());
}

ham_bool_t
proto_has_env_get_parameters_request(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->type()==Wrapper::ENV_GET_PARAMETERS_REQUEST) {
        ham_assert(w->has_env_get_parameters_request()==true, (""));
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_env_get_parameters_request()==false, (""));
        return (HAM_FALSE);
    }
}

proto_wrapper_t *
proto_init_env_get_parameters_reply(ham_u32_t status)
{
    Wrapper *w=new Wrapper();
    w->set_type(Wrapper::ENV_GET_PARAMETERS_REPLY);
    w->mutable_env_get_parameters_reply()->set_status(status);
    return ((proto_wrapper_t *)w);
}

ham_bool_t
proto_has_env_get_parameters_reply(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->type()==Wrapper::ENV_GET_PARAMETERS_REPLY) {
        ham_assert(w->has_env_get_parameters_reply()==true, (""));
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_env_get_parameters_reply()==false, (""));
        return (HAM_FALSE);
    }
}

ham_u32_t
proto_env_get_parameters_reply_get_status(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->env_get_parameters_reply().status());
}

void
proto_env_get_parameters_reply_set_cachesize(proto_wrapper_t *wrapper,
                ham_u32_t cachesize)
{
    Wrapper *w=(Wrapper *)wrapper;
    w->mutable_env_get_parameters_reply()->set_cachesize(cachesize);
}

ham_bool_t
proto_env_get_parameters_reply_has_cachesize(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->env_get_parameters_reply().has_cachesize());
}

ham_u32_t
proto_env_get_parameters_reply_get_cachesize(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->env_get_parameters_reply().cachesize());
}

void
proto_env_get_parameters_reply_set_pagesize(proto_wrapper_t *wrapper,
                ham_u32_t pagesize)
{
    Wrapper *w=(Wrapper *)wrapper;
    w->mutable_env_get_parameters_reply()->set_pagesize(pagesize);
}

ham_bool_t
proto_env_get_parameters_reply_has_pagesize(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->env_get_parameters_reply().has_pagesize());
}

ham_u32_t
proto_env_get_parameters_reply_get_pagesize(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->env_get_parameters_reply().pagesize());
}

void
proto_env_get_parameters_reply_set_max_env_databases(proto_wrapper_t *wrapper,
                ham_u32_t val)
{
    Wrapper *w=(Wrapper *)wrapper;
    w->mutable_env_get_parameters_reply()->set_max_env_databases(val);
}

ham_bool_t
proto_env_get_parameters_reply_has_max_env_databases(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->env_get_parameters_reply().has_max_env_databases());
}

ham_u32_t
proto_env_get_parameters_reply_get_max_env_databases(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->env_get_parameters_reply().max_env_databases());
}

void
proto_env_get_parameters_reply_set_flags(proto_wrapper_t *wrapper,
                ham_u32_t flags)
{
    Wrapper *w=(Wrapper *)wrapper;
    w->mutable_env_get_parameters_reply()->set_flags(flags);
}

ham_bool_t
proto_env_get_parameters_reply_has_flags(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->env_get_parameters_reply().has_flags());
}

ham_u32_t
proto_env_get_parameters_reply_get_flags(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->env_get_parameters_reply().flags());
}

void
proto_env_get_parameters_reply_set_filemode(proto_wrapper_t *wrapper,
                ham_u32_t filemode)
{
    Wrapper *w=(Wrapper *)wrapper;
    w->mutable_env_get_parameters_reply()->set_filemode(filemode);
}

ham_bool_t
proto_env_get_parameters_reply_has_filemode(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->env_get_parameters_reply().has_filemode());
}

ham_u32_t
proto_env_get_parameters_reply_get_filemode(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->env_get_parameters_reply().filemode());
}

void
proto_env_get_parameters_reply_set_filename(proto_wrapper_t *wrapper,
                const char *filename)
{
    Wrapper *w=(Wrapper *)wrapper;
    w->mutable_env_get_parameters_reply()->set_filename(filename);
}

ham_bool_t
proto_env_get_parameters_reply_has_filename(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->env_get_parameters_reply().has_filename());
}

const char *
proto_env_get_parameters_reply_get_filename(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->env_get_parameters_reply().filename().c_str());
}

proto_wrapper_t *
proto_init_env_flush_request(ham_u32_t flags)
{
    Wrapper *w=new Wrapper();
    w->set_type(Wrapper::ENV_FLUSH_REQUEST);
    w->mutable_env_flush_request()->set_flags(flags);
    return ((proto_wrapper_t *)w);
}

ham_u32_t
proto_env_flush_request_get_flags(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->env_flush_request().flags());
}

ham_bool_t
proto_has_env_flush_request(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->type()==Wrapper::ENV_FLUSH_REQUEST) {
        ham_assert(w->has_env_flush_request()==true, (""));
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_env_flush_request()==false, (""));
        return (HAM_FALSE);
    }
}

proto_wrapper_t *
proto_init_env_flush_reply(ham_status_t status)
{
    Wrapper *w=new Wrapper();
    w->set_type(Wrapper::ENV_FLUSH_REPLY);
    w->mutable_env_flush_reply()->set_status(status);
    return ((proto_wrapper_t *)w);
}

ham_bool_t
proto_has_env_flush_reply(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->type()==Wrapper::ENV_FLUSH_REPLY) {
        ham_assert(w->has_env_flush_reply()==true, (""));
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_env_flush_reply()==false, (""));
        return (HAM_FALSE);
    }
}

ham_u32_t
proto_env_flush_reply_get_status(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->env_flush_reply().status());
}

proto_wrapper_t *
proto_init_env_create_db_request(ham_u16_t dbname, ham_u32_t flags, 
                ham_u32_t *names, ham_u64_t *values, ham_u32_t num_params)
{
    Wrapper *w=new Wrapper();
    w->set_type(Wrapper::ENV_CREATE_DB_REQUEST);
    w->mutable_env_create_db_request()->set_dbname(dbname);
    w->mutable_env_create_db_request()->set_flags(flags);
    for (ham_u32_t i=0; i<num_params; i++) {
        w->mutable_env_create_db_request()->add_param_names(names[i]);
        w->mutable_env_create_db_request()->add_param_values(values[i]);
    }
    return ((proto_wrapper_t *)w);
}

ham_bool_t
proto_has_env_create_db_request(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->type()==Wrapper::ENV_CREATE_DB_REQUEST) {
        ham_assert(w->has_env_create_db_request()==true, (""));
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_env_create_db_request()==false, (""));
        return (HAM_FALSE);
    }
}

ham_size_t
proto_env_create_db_request_get_num_params(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->env_create_db_request().param_names().size());
}

ham_u32_t *
proto_env_create_db_request_get_param_names(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return ((ham_u32_t *)w->mutable_env_create_db_request()->mutable_param_names()->data());
}

ham_u32_t *
proto_env_create_db_request_get_param_values(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return ((ham_u32_t *)w->mutable_env_create_db_request()->mutable_param_values()->data());
}

ham_u32_t
proto_env_create_db_request_get_dbname(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->env_create_db_request().dbname());
}

ham_u32_t
proto_env_create_db_request_get_flags(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->env_create_db_request().flags());
}

proto_wrapper_t *
proto_init_env_create_db_reply(ham_status_t status, ham_u64_t db_handle,
                ham_u32_t db_flags)
{
    Wrapper *w=new Wrapper();
    w->set_type(Wrapper::ENV_CREATE_DB_REPLY);
    w->mutable_env_create_db_reply()->set_status(status);
    w->mutable_env_create_db_reply()->set_db_handle(db_handle);
    w->mutable_env_create_db_reply()->set_db_flags(db_flags);
    return ((proto_wrapper_t *)w);
}

ham_bool_t
proto_has_env_create_db_reply(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->type()==Wrapper::ENV_CREATE_DB_REPLY) {
        ham_assert(w->has_env_create_db_reply()==true, (""));
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_env_create_db_reply()==false, (""));
        return (HAM_FALSE);
    }
}

ham_u32_t
proto_env_create_db_reply_get_status(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->env_create_db_reply().status());
}

ham_u32_t
proto_env_create_db_reply_get_flags(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->env_create_db_reply().db_flags());
}

ham_u64_t
proto_env_create_db_reply_get_db_handle(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->env_create_db_reply().db_handle());
}

proto_wrapper_t *
proto_init_env_open_db_request(ham_u16_t dbname, ham_u32_t flags, 
                ham_u32_t *names, ham_u64_t *values, ham_u32_t num_params)
{
    Wrapper *w=new Wrapper();
    w->set_type(Wrapper::ENV_OPEN_DB_REQUEST);
    w->mutable_env_open_db_request()->set_dbname(dbname);
    w->mutable_env_open_db_request()->set_flags(flags);
    for (ham_u32_t i=0; i<num_params; i++) {
        w->mutable_env_open_db_request()->add_param_names(names[i]);
        w->mutable_env_open_db_request()->add_param_values(values[i]);
    }
    return ((proto_wrapper_t *)w);
}

ham_bool_t
proto_has_env_open_db_request(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->type()==Wrapper::ENV_OPEN_DB_REQUEST) {
        ham_assert(w->has_env_open_db_request()==true, (""));
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_env_open_db_request()==false, (""));
        return (HAM_FALSE);
    }
}

ham_size_t
proto_env_open_db_request_get_num_params(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->env_open_db_request().param_names().size());
}

ham_u32_t *
proto_env_open_db_request_get_param_names(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return ((ham_u32_t *)w->mutable_env_open_db_request()->mutable_param_names()->data());
}

ham_u32_t *
proto_env_open_db_request_get_param_values(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return ((ham_u32_t *)w->mutable_env_open_db_request()->mutable_param_values()->data());
}

ham_u32_t
proto_env_open_db_request_get_dbname(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->env_open_db_request().dbname());
}

ham_u32_t
proto_env_open_db_request_get_flags(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->env_open_db_request().flags());
}

proto_wrapper_t *
proto_init_env_open_db_reply(ham_status_t status, ham_u64_t dbhandle,
                ham_u32_t flags)
{
    Wrapper *w=new Wrapper();
    w->set_type(Wrapper::ENV_OPEN_DB_REPLY);
    w->mutable_env_open_db_reply()->set_status(status);
    w->mutable_env_open_db_reply()->set_db_handle(dbhandle);
    w->mutable_env_open_db_reply()->set_db_flags(flags);
    return ((proto_wrapper_t *)w);
}

ham_bool_t
proto_has_env_open_db_reply(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->type()==Wrapper::ENV_OPEN_DB_REPLY) {
        ham_assert(w->has_env_open_db_reply()==true, (""));
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_env_open_db_reply()==false, (""));
        return (HAM_FALSE);
    }
}

ham_u32_t
proto_env_open_db_reply_get_status(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->env_open_db_reply().status());
}

ham_u32_t
proto_env_open_db_reply_get_flags(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->env_open_db_reply().db_flags());
}

ham_u64_t
proto_env_open_db_reply_get_db_handle(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->env_open_db_reply().db_handle());
}

proto_wrapper_t *
proto_init_txn_begin_request(ham_u64_t dbhandle, ham_u32_t flags)
{
    Wrapper *w=new Wrapper();
    w->set_type(Wrapper::TXN_BEGIN_REQUEST);
    w->mutable_txn_begin_request()->set_db_handle(dbhandle);
    w->mutable_txn_begin_request()->set_flags(flags);
    return ((proto_wrapper_t *)w);
}

ham_bool_t
proto_has_txn_begin_request(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->type()==Wrapper::TXN_BEGIN_REQUEST) {
        ham_assert(w->has_txn_begin_request()==true, (""));
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_txn_begin_request()==false, (""));
        return (HAM_FALSE);
    }
}

ham_u32_t
proto_txn_begin_request_get_flags(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->txn_begin_request().flags());
}

ham_u64_t
proto_txn_begin_request_get_db_handle(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->txn_begin_request().db_handle());
}

proto_wrapper_t *
proto_init_txn_begin_reply(ham_status_t status, ham_u64_t txnhandle)
{
    Wrapper *w=new Wrapper();
    w->set_type(Wrapper::TXN_BEGIN_REPLY);
    w->mutable_txn_begin_reply()->set_status(status);
    w->mutable_txn_begin_reply()->set_txn_handle(txnhandle);
    return ((proto_wrapper_t *)w);
}

ham_bool_t
proto_has_txn_begin_reply(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->type()==Wrapper::TXN_BEGIN_REPLY) {
        ham_assert(w->has_txn_begin_reply()==true, (""));
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_txn_begin_reply()==false, (""));
        return (HAM_FALSE);
    }
}

ham_u32_t
proto_txn_begin_reply_get_status(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->txn_begin_reply().status());
}

ham_u64_t
proto_txn_begin_reply_get_txn_handle(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->txn_begin_reply().txn_handle());
}

proto_wrapper_t *
proto_init_txn_commit_request(ham_u64_t txnhandle, ham_u32_t flags)
{
    Wrapper *w=new Wrapper();
    w->set_type(Wrapper::TXN_COMMIT_REQUEST);
    w->mutable_txn_commit_request()->set_txn_handle(txnhandle);
    w->mutable_txn_commit_request()->set_flags(flags);
    return ((proto_wrapper_t *)w);
}

ham_bool_t
proto_has_txn_commit_request(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->type()==Wrapper::TXN_COMMIT_REQUEST) {
        ham_assert(w->has_txn_commit_request()==true, (""));
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_txn_commit_request()==false, (""));
        return (HAM_FALSE);
    }
}

ham_u32_t
proto_txn_commit_request_get_flags(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->txn_commit_request().flags());
}

ham_u64_t
proto_txn_commit_request_get_txn_handle(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->txn_commit_request().txn_handle());
}

proto_wrapper_t *
proto_init_txn_commit_reply(ham_status_t status)
{
    Wrapper *w=new Wrapper();
    w->set_type(Wrapper::TXN_COMMIT_REPLY);
    w->mutable_txn_commit_reply()->set_status(status);
    return ((proto_wrapper_t *)w);
}

ham_bool_t
proto_has_txn_commit_reply(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->type()==Wrapper::TXN_COMMIT_REPLY) {
        ham_assert(w->has_txn_commit_reply()==true, (""));
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_txn_commit_reply()==false, (""));
        return (HAM_FALSE);
    }
}

ham_u32_t
proto_txn_commit_reply_get_status(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->txn_commit_reply().status());
}

proto_wrapper_t *
proto_init_txn_abort_request(ham_u64_t txnhandle, ham_u32_t flags)
{
    Wrapper *w=new Wrapper();
    w->set_type(Wrapper::TXN_ABORT_REQUEST);
    w->mutable_txn_abort_request()->set_txn_handle(txnhandle);
    w->mutable_txn_abort_request()->set_flags(flags);
    return ((proto_wrapper_t *)w);
}

ham_bool_t
proto_has_txn_abort_request(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->type()==Wrapper::TXN_ABORT_REQUEST) {
        ham_assert(w->has_txn_abort_request()==true, (""));
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_txn_abort_request()==false, (""));
        return (HAM_FALSE);
    }
}

ham_u32_t
proto_txn_abort_request_get_flags(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->txn_abort_request().flags());
}

ham_u64_t
proto_txn_abort_request_get_txn_handle(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->txn_abort_request().txn_handle());
}

proto_wrapper_t *
proto_init_txn_abort_reply(ham_status_t status)
{
    Wrapper *w=new Wrapper();
    w->set_type(Wrapper::TXN_ABORT_REPLY);
    w->mutable_txn_abort_reply()->set_status(status);
    return ((proto_wrapper_t *)w);
}

ham_bool_t
proto_has_txn_abort_reply(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->type()==Wrapper::TXN_ABORT_REPLY) {
        ham_assert(w->has_txn_abort_reply()==true, (""));
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_txn_abort_reply()==false, (""));
        return (HAM_FALSE);
    }
}

ham_u32_t
proto_txn_abort_reply_get_status(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->txn_abort_reply().status());
}

proto_wrapper_t *
proto_init_db_close_request(ham_u64_t dbhandle, ham_u32_t flags)
{
    Wrapper *w=new Wrapper();
    w->set_type(Wrapper::DB_CLOSE_REQUEST);
    w->mutable_db_close_request()->set_db_handle(dbhandle);
    w->mutable_db_close_request()->set_flags(flags);
    return ((proto_wrapper_t *)w);
}

ham_bool_t
proto_has_db_close_request(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->type()==Wrapper::DB_CLOSE_REQUEST) {
        ham_assert(w->has_db_close_request()==true, (""));
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_db_close_request()==false, (""));
        return (HAM_FALSE);
    }
}

ham_u64_t
proto_db_close_request_get_db_handle(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_close_request().db_handle());
}

ham_u32_t
proto_db_close_request_get_flags(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_close_request().flags());
}

proto_wrapper_t *
proto_init_db_close_reply(ham_u32_t status)
{
    Wrapper *w=new Wrapper();
    w->set_type(Wrapper::DB_CLOSE_REPLY);
    w->mutable_db_close_reply()->set_status(status);
    return ((proto_wrapper_t *)w);
}

ham_bool_t
proto_has_db_close_reply(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->type()==Wrapper::DB_CLOSE_REPLY) {
        ham_assert(w->has_db_close_reply()==true, (""));
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_db_close_reply()==false, (""));
        return (HAM_FALSE);
    }
}

ham_u32_t
proto_db_close_reply_get_status(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_close_reply().status());
}

proto_wrapper_t *
proto_init_db_get_parameters_request(ham_u64_t dbhandle, ham_u32_t *names, 
                ham_u32_t names_size)
{
    Wrapper *w=new Wrapper();
    w->set_type(Wrapper::DB_GET_PARAMETERS_REQUEST);
    w->mutable_db_get_parameters_request()->set_db_handle(dbhandle);
    for (ham_u32_t i=0; i<names_size; i++)
        w->mutable_db_get_parameters_request()->add_names(names[i]);
    return ((proto_wrapper_t *)w);
}

ham_u64_t
proto_db_get_parameters_request_get_db_handle(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_get_parameters_request().db_handle());
}

ham_bool_t
proto_has_db_get_parameters_request(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->type()==Wrapper::DB_GET_PARAMETERS_REQUEST) {
        ham_assert(w->has_db_get_parameters_request()==true, (""));
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_db_get_parameters_request()==false, (""));
        return (HAM_FALSE);
    }
}

ham_size_t
proto_db_get_parameters_request_get_names_size(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_get_parameters_request().names().size());
}

ham_u32_t *
proto_db_get_parameters_request_get_names(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->mutable_db_get_parameters_request()->mutable_names()->mutable_data());
}

proto_wrapper_t *
proto_init_db_get_parameters_reply(ham_status_t status)
{
    Wrapper *w=new Wrapper();
    w->set_type(Wrapper::DB_GET_PARAMETERS_REPLY);
    w->mutable_db_get_parameters_reply()->set_status(status);
    return ((proto_wrapper_t *)w);
}

ham_bool_t
proto_has_db_get_parameters_reply(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->type()==Wrapper::DB_GET_PARAMETERS_REPLY) {
        ham_assert(w->has_db_get_parameters_reply()==true, (""));
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_db_get_parameters_reply()==false, (""));
        return (HAM_FALSE);
    }
}

ham_u32_t
proto_db_get_parameters_reply_get_status(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_get_parameters_reply().status());
}

void
proto_db_get_parameters_reply_set_cachesize(proto_wrapper_t *wrapper,
                ham_u32_t cachesize)
{
    Wrapper *w=(Wrapper *)wrapper;
    w->mutable_db_get_parameters_reply()->set_cachesize(cachesize);
}

ham_bool_t
proto_db_get_parameters_reply_has_cachesize(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_get_parameters_reply().has_cachesize());
}

ham_u32_t
proto_db_get_parameters_reply_get_cachesize(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_get_parameters_reply().cachesize());
}

void
proto_db_get_parameters_reply_set_pagesize(proto_wrapper_t *wrapper,
                ham_u32_t pagesize)
{
    Wrapper *w=(Wrapper *)wrapper;
    w->mutable_db_get_parameters_reply()->set_pagesize(pagesize);
}

ham_bool_t
proto_db_get_parameters_reply_has_pagesize(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_get_parameters_reply().has_pagesize());
}

ham_u32_t
proto_db_get_parameters_reply_get_pagesize(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_get_parameters_reply().pagesize());
}

void
proto_db_get_parameters_reply_set_max_env_databases(proto_wrapper_t *wrapper,
                ham_u32_t med)
{
    Wrapper *w=(Wrapper *)wrapper;
    w->mutable_db_get_parameters_reply()->set_max_env_databases(med);
}

ham_bool_t
proto_db_get_parameters_reply_has_max_env_databases(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_get_parameters_reply().has_max_env_databases());
}

ham_u32_t
proto_db_get_parameters_reply_get_max_env_databases(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_get_parameters_reply().max_env_databases());
}

void
proto_db_get_parameters_reply_set_flags(proto_wrapper_t *wrapper, 
                ham_u32_t flags)
{
    Wrapper *w=(Wrapper *)wrapper;
    w->mutable_db_get_parameters_reply()->set_flags(flags);
}

ham_bool_t
proto_db_get_parameters_reply_has_flags(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_get_parameters_reply().has_flags());
}

ham_u32_t
proto_db_get_parameters_reply_get_flags(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_get_parameters_reply().flags());
}

void
proto_db_get_parameters_reply_set_filemode(proto_wrapper_t *wrapper, 
                ham_u32_t filemode)
{
    Wrapper *w=(Wrapper *)wrapper;
    w->mutable_db_get_parameters_reply()->set_filemode(filemode);
}

ham_bool_t
proto_db_get_parameters_reply_has_filemode(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_get_parameters_reply().has_filemode());
}

ham_u32_t
proto_db_get_parameters_reply_get_filemode(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_get_parameters_reply().filemode());
}

void
proto_db_get_parameters_reply_set_filename(proto_wrapper_t *wrapper,
                const char *filename)
{
    Wrapper *w=(Wrapper *)wrapper;
    w->mutable_db_get_parameters_reply()->set_filename(filename);
}

ham_bool_t
proto_db_get_parameters_reply_has_filename(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_get_parameters_reply().has_filename());
}

const char *
proto_db_get_parameters_reply_get_filename(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_get_parameters_reply().filename().c_str());
}

void
proto_db_get_parameters_reply_set_keysize(proto_wrapper_t *wrapper,
                ham_u32_t keysize)
{
    Wrapper *w=(Wrapper *)wrapper;
    w->mutable_db_get_parameters_reply()->set_keysize(keysize);
}

ham_bool_t
proto_db_get_parameters_reply_has_keysize(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_get_parameters_reply().has_keysize());
}

ham_u32_t
proto_db_get_parameters_reply_get_keysize(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_get_parameters_reply().keysize());
}

void
proto_db_get_parameters_reply_set_dbname(proto_wrapper_t *wrapper,
                ham_u32_t dbname)
{
    Wrapper *w=(Wrapper *)wrapper;
    w->mutable_db_get_parameters_reply()->set_dbname(dbname);
}

ham_bool_t
proto_db_get_parameters_reply_has_dbname(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_get_parameters_reply().has_dbname());
}

ham_u32_t
proto_db_get_parameters_reply_get_dbname(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_get_parameters_reply().dbname());
}

void
proto_db_get_parameters_reply_set_keys_per_page(proto_wrapper_t *wrapper,
                ham_u32_t kpp)
{
    Wrapper *w=(Wrapper *)wrapper;
    w->mutable_db_get_parameters_reply()->set_keys_per_page(kpp);
}

ham_bool_t
proto_db_get_parameters_reply_has_keys_per_page(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_get_parameters_reply().has_keys_per_page());
}

ham_u32_t
proto_db_get_parameters_reply_get_keys_per_page(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_get_parameters_reply().keys_per_page());
}

void
proto_db_get_parameters_reply_set_dam(proto_wrapper_t *wrapper,
                ham_u32_t dam)
{
    Wrapper *w=(Wrapper *)wrapper;
    w->mutable_db_get_parameters_reply()->set_dam(dam);
}

ham_bool_t
proto_db_get_parameters_reply_has_dam(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_get_parameters_reply().has_dam());
}

ham_u32_t
proto_db_get_parameters_reply_get_dam(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_get_parameters_reply().dam());
}

proto_wrapper_t *
proto_init_db_flush_request(ham_u64_t dbhandle, ham_u32_t flags)
{
    Wrapper *w=new Wrapper();
    w->set_type(Wrapper::DB_FLUSH_REQUEST);
    w->mutable_db_flush_request()->set_db_handle(dbhandle);
    w->mutable_db_flush_request()->set_flags(flags);
    return ((proto_wrapper_t *)w);
}

ham_u64_t
proto_db_flush_request_get_db_handle(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_flush_request().db_handle());
}

ham_u32_t
proto_db_flush_request_get_flags(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_flush_request().flags());
}

ham_bool_t
proto_has_db_flush_request(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->type()==Wrapper::DB_FLUSH_REQUEST) {
        ham_assert(w->has_db_flush_request()==true, (""));
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_db_flush_request()==false, (""));
        return (HAM_FALSE);
    }
}

proto_wrapper_t *
proto_init_db_flush_reply(ham_u32_t status)
{
    Wrapper *w=new Wrapper();
    w->set_type(Wrapper::DB_FLUSH_REPLY);
    w->mutable_db_flush_reply()->set_status(status);
    return ((proto_wrapper_t *)w);
}

ham_bool_t
proto_has_db_flush_reply(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->type()==Wrapper::DB_FLUSH_REPLY) {
        ham_assert(w->has_db_flush_reply()==true, (""));
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_db_flush_reply()==false, (""));
        return (HAM_FALSE);
    }
}

ham_u32_t
proto_db_flush_reply_get_status(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_flush_reply().status());
}

proto_wrapper_t *
proto_init_check_integrity_request(ham_u64_t dbhandle, ham_u64_t txnhandle)
{
    Wrapper *w=new Wrapper();
    w->set_type(Wrapper::DB_CHECK_INTEGRITY_REQUEST);
    w->mutable_db_check_integrity_request()->set_db_handle(dbhandle);
    w->mutable_db_check_integrity_request()->set_txn_handle(txnhandle);
    return ((proto_wrapper_t *)w);
}

ham_bool_t
proto_has_check_integrity_request(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->type()==Wrapper::DB_CHECK_INTEGRITY_REQUEST) {
        ham_assert(w->has_db_check_integrity_request()==true, (""));
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_db_check_integrity_request()==false, (""));
        return (HAM_FALSE);
    }
}

ham_u64_t
proto_check_integrity_request_get_db_handle(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_check_integrity_request().db_handle());
}

ham_u64_t
proto_check_integrity_request_get_txn_handle(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_check_integrity_request().txn_handle());
}

proto_wrapper_t *
proto_init_check_integrity_reply(ham_status_t status)
{
    Wrapper *w=new Wrapper();
    w->set_type(Wrapper::DB_CHECK_INTEGRITY_REPLY);
    w->mutable_db_check_integrity_reply()->set_status(status);
    return ((proto_wrapper_t *)w);
}

ham_bool_t
proto_has_check_integrity_reply(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->type()==Wrapper::DB_CHECK_INTEGRITY_REPLY) {
        ham_assert(w->has_db_check_integrity_reply()==true, (""));
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_db_check_integrity_reply()==false, (""));
        return (HAM_FALSE);
    }
}

ham_u32_t
proto_check_integrity_reply_get_status(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_check_integrity_reply().status());
}

proto_wrapper_t *
proto_init_db_get_key_count_request(ham_u64_t dbhandle, ham_u64_t txnhandle, 
                ham_u32_t flags)
{
    Wrapper *w=new Wrapper();
    w->set_type(Wrapper::DB_GET_KEY_COUNT_REQUEST);
    w->mutable_db_get_key_count_request()->set_db_handle(dbhandle);
    w->mutable_db_get_key_count_request()->set_txn_handle(txnhandle);
    w->mutable_db_get_key_count_request()->set_flags(flags);
    return ((proto_wrapper_t *)w);
}

ham_bool_t
proto_has_db_get_key_count_request(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->type()==Wrapper::DB_GET_KEY_COUNT_REQUEST) {
        ham_assert(w->has_db_get_key_count_request()==true, (""));
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_db_get_key_count_request()==false, (""));
        return (HAM_FALSE);
    }
}

ham_u64_t
proto_db_get_key_count_request_get_db_handle(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_get_key_count_request().db_handle());
}

ham_u64_t
proto_db_get_key_count_request_get_txn_handle(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_get_key_count_request().txn_handle());
}

ham_u32_t
proto_db_get_key_count_request_get_flags(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_get_key_count_request().flags());
}

proto_wrapper_t *
proto_init_db_get_key_count_reply(ham_status_t status, ham_u64_t keycount)
{
    Wrapper *w=new Wrapper();
    w->set_type(Wrapper::DB_GET_KEY_COUNT_REPLY);
    w->mutable_db_get_key_count_reply()->set_status(status);
    w->mutable_db_get_key_count_reply()->set_keycount(keycount);
    return ((proto_wrapper_t *)w);
}

ham_bool_t
proto_has_db_get_key_count_reply(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->type()==Wrapper::DB_GET_KEY_COUNT_REPLY) {
        ham_assert(w->has_db_get_key_count_reply()==true, (""));
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_db_get_key_count_reply()==false, (""));
        return (HAM_FALSE);
    }
}

ham_u32_t
proto_db_get_key_count_reply_get_status(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_get_key_count_reply().status());
}

ham_u64_t
proto_db_get_key_count_reply_get_key_count(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_get_key_count_reply().keycount());
}

static void
assign_key(Key *protokey, ham_key_t *hamkey)
{
    protokey->set_data(hamkey->data, hamkey->size);
    protokey->set_flags(hamkey->flags);
    protokey->set_intflags(hamkey->_flags);
}

static void
assign_record(Record *protorec, ham_record_t *hamrec)
{
    protorec->set_data(hamrec->data, hamrec->size);
    protorec->set_flags(hamrec->flags);
    protorec->set_partial_offset(hamrec->partial_offset);
    protorec->set_partial_size(hamrec->partial_size);
}

proto_wrapper_t *
proto_init_db_insert_request(ham_u64_t dbhandle, ham_u64_t txnhandle, 
                ham_key_t *key, ham_record_t *record, ham_u32_t flags)
{
    Wrapper *w=new Wrapper();
    w->set_type(Wrapper::DB_INSERT_REQUEST);
    w->mutable_db_insert_request()->set_db_handle(dbhandle);
    w->mutable_db_insert_request()->set_txn_handle(txnhandle);
    w->mutable_db_insert_request()->set_flags(flags);
    if (key)
        assign_key(w->mutable_db_insert_request()->mutable_key(), key);
    if (record)
        assign_record(w->mutable_db_insert_request()->mutable_record(), record);
    return ((proto_wrapper_t *)w);
}

ham_bool_t
proto_has_db_insert_request(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->type()==Wrapper::DB_INSERT_REQUEST) {
        ham_assert(w->has_db_insert_request()==true, (""));
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_db_insert_request()==false, (""));
        return (HAM_FALSE);
    }
}

ham_u64_t
proto_db_insert_request_get_db_handle(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_insert_request().db_handle());
}

ham_u64_t
proto_db_insert_request_get_txn_handle(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_insert_request().txn_handle());
}

ham_u32_t
proto_db_insert_request_get_flags(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_insert_request().flags());
}

ham_bool_t
proto_db_insert_request_has_key(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_insert_request().has_key());
}

ham_u32_t
proto_db_insert_request_get_key_flags(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_insert_request().key().flags());
}

void *
proto_db_insert_request_get_key_data(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->db_insert_request().key().has_data())
        return ((void *)&w->db_insert_request().key().data()[0]);
    else
        return (0);
}

ham_size_t
proto_db_insert_request_get_key_size(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_insert_request().key().data().size());
}

ham_bool_t
proto_db_insert_request_has_record(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_insert_request().has_record());
}

ham_u32_t
proto_db_insert_request_get_record_flags(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_insert_request().record().flags());
}

void *
proto_db_insert_request_get_record_data(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->db_insert_request().record().has_data())
        return ((void *)&w->db_insert_request().record().data()[0]);
    else
        return (0);
}

ham_size_t
proto_db_insert_request_get_record_size(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_insert_request().record().data().size());
}

ham_offset_t
proto_db_insert_request_get_record_partial_offset(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_insert_request().record().partial_offset());
}

ham_offset_t
proto_db_insert_request_get_record_partial_size(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_insert_request().record().partial_size());
}

proto_wrapper_t *
proto_init_db_insert_reply(ham_status_t status, ham_key_t *key)
{
    Wrapper *w=new Wrapper();
    w->set_type(Wrapper::DB_INSERT_REPLY);
    w->mutable_db_insert_reply()->set_status(status);
    if (key)
        assign_key(w->mutable_db_insert_reply()->mutable_key(), key);
    return ((proto_wrapper_t *)w);
}

ham_bool_t
proto_has_db_insert_reply(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->type()==Wrapper::DB_INSERT_REPLY) {
        ham_assert(w->has_db_insert_reply()==true, (""));
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_db_insert_reply()==false, (""));
        return (HAM_FALSE);
    }
}

ham_u32_t
proto_db_insert_reply_get_status(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_insert_reply().status());
}

ham_bool_t
proto_db_insert_reply_has_key(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_insert_reply().has_key());
}

void *
proto_db_insert_reply_get_key_data(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->db_insert_reply().key().has_data())
        return ((void *)&w->db_insert_reply().key().data()[0]);
    else
        return (0);
}

ham_size_t
proto_db_insert_reply_get_key_size(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_insert_reply().key().data().size());
}

proto_wrapper_t *
proto_init_db_find_request(ham_u64_t dbhandle, ham_u64_t txnhandle, 
                ham_key_t *key, ham_record_t *record, ham_u32_t flags)
{
    Wrapper *w=new Wrapper();
    w->set_type(Wrapper::DB_FIND_REQUEST);
    w->mutable_db_find_request()->set_db_handle(dbhandle);
    w->mutable_db_find_request()->set_txn_handle(txnhandle);
    w->mutable_db_find_request()->set_flags(flags);
    if (key)
        assign_key(w->mutable_db_find_request()->mutable_key(), key);
    if (record)
        assign_record(w->mutable_db_find_request()->mutable_record(), record);
    return ((proto_wrapper_t *)w);
}

ham_bool_t
proto_has_db_find_request(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->type()==Wrapper::DB_FIND_REQUEST) {
        ham_assert(w->has_db_find_request()==true, (""));
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_db_find_request()==false, (""));
        return (HAM_FALSE);
    }
}

ham_u64_t
proto_db_find_request_get_db_handle(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_find_request().db_handle());
}

ham_u64_t
proto_db_find_request_get_txn_handle(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_find_request().txn_handle());
}

ham_u32_t
proto_db_find_request_get_flags(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_find_request().flags());
}

ham_u32_t
proto_db_find_request_get_key_flags(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_find_request().key().flags());
}

void *
proto_db_find_request_get_key_data(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->db_find_request().key().has_data())
        return ((void *)&w->db_find_request().key().data()[0]);
    else
        return (0);
}

ham_size_t
proto_db_find_request_get_key_size(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_find_request().key().data().size());
}

ham_u32_t
proto_db_find_request_get_record_flags(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_find_request().record().flags());
}

void *
proto_db_find_request_get_record_data(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->db_find_request().record().has_data())
        return ((void *)&w->db_find_request().record().data()[0]);
    else
        return (0);
}

ham_size_t
proto_db_find_request_get_record_size(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_find_request().record().data().size());
}

ham_offset_t
proto_db_find_request_get_record_partial_offset(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_find_request().record().partial_offset());
}

ham_offset_t
proto_db_find_request_get_record_partial_size(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_find_request().record().partial_size());
}

proto_wrapper_t *
proto_init_db_find_reply(ham_status_t status,
                ham_key_t *key, ham_record_t *record)
{
    Wrapper *w=new Wrapper();
    w->set_type(Wrapper::DB_FIND_REPLY);
    w->mutable_db_find_reply()->set_status(status);
    if (key)
        assign_key(w->mutable_db_find_reply()->mutable_key(), key);
    if (record)
        assign_record(w->mutable_db_find_reply()->mutable_record(), record);
    return ((proto_wrapper_t *)w);
}

ham_bool_t
proto_has_db_find_reply(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->type()==Wrapper::DB_FIND_REPLY) {
        ham_assert(w->has_db_find_reply()==true, (""));
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_db_find_reply()==false, (""));
        return (HAM_FALSE);
    }
}

ham_u32_t
proto_db_find_reply_get_status(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_find_reply().status());
}

ham_bool_t
proto_db_find_reply_has_key(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_find_reply().has_key());
}

void *
proto_db_find_reply_get_key_data(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->db_find_reply().key().has_data())
        return ((void *)&w->db_find_reply().key().data()[0]);
    else
        return (0);
}

ham_u32_t
proto_db_find_reply_get_key_intflags(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_find_reply().key().intflags());
}

ham_size_t
proto_db_find_reply_get_key_size(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_find_reply().key().data().size());
}

ham_bool_t
proto_db_find_reply_has_record(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_find_reply().has_record());
}

void *
proto_db_find_reply_get_record_data(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->db_find_reply().record().has_data())
        return ((void *)&w->db_find_reply().record().data()[0]);
    else
        return (0);
}

ham_size_t
proto_db_find_reply_get_record_size(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_find_reply().record().data().size());
}

proto_wrapper_t *
proto_init_db_erase_request(ham_u64_t dbhandle, ham_u64_t txnhandle, 
                ham_key_t *key, ham_u32_t flags)
{
    Wrapper *w=new Wrapper();
    w->set_type(Wrapper::DB_ERASE_REQUEST);
    w->mutable_db_erase_request()->set_db_handle(dbhandle);
    w->mutable_db_erase_request()->set_txn_handle(txnhandle);
    w->mutable_db_erase_request()->set_flags(flags);
    if (key)
        assign_key(w->mutable_db_erase_request()->mutable_key(), key);
    return ((proto_wrapper_t *)w);
}

ham_bool_t
proto_has_db_erase_request(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->type()==Wrapper::DB_ERASE_REQUEST) {
        ham_assert(w->has_db_erase_request()==true, (""));
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_db_erase_request()==false, (""));
        return (HAM_FALSE);
    }
}

ham_u64_t
proto_db_erase_request_get_db_handle(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_erase_request().db_handle());
}

ham_u64_t
proto_db_erase_request_get_txn_handle(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_erase_request().txn_handle());
}

ham_u32_t
proto_db_erase_request_get_flags(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_erase_request().flags());
}

ham_u32_t
proto_db_erase_request_get_key_flags(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_erase_request().key().flags());
}

void *
proto_db_erase_request_get_key_data(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->db_erase_request().key().has_data())
        return ((void *)&w->db_erase_request().key().data()[0]);
    else
        return (0);
}

ham_size_t
proto_db_erase_request_get_key_size(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_erase_request().key().data().size());
}

proto_wrapper_t *
proto_init_db_erase_reply(ham_status_t status)
{
    Wrapper *w=new Wrapper();
    w->set_type(Wrapper::DB_ERASE_REPLY);
    w->mutable_db_erase_reply()->set_status(status);
    return ((proto_wrapper_t *)w);
}

ham_bool_t
proto_has_db_erase_reply(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->type()==Wrapper::DB_ERASE_REPLY) {
        ham_assert(w->has_db_erase_reply()==true, (""));
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_db_erase_reply()==false, (""));
        return (HAM_FALSE);
    }
}

ham_u32_t
proto_db_erase_reply_get_status(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->db_erase_reply().status());
}

proto_wrapper_t *
proto_init_cursor_create_request(ham_u64_t dbhandle, ham_u64_t txnhandle, 
                ham_u32_t flags)
{
    Wrapper *w=new Wrapper();
    w->set_type(Wrapper::CURSOR_CREATE_REQUEST);
    w->mutable_cursor_create_request()->set_db_handle(dbhandle);
    w->mutable_cursor_create_request()->set_txn_handle(txnhandle);
    w->mutable_cursor_create_request()->set_flags(flags);
    return ((proto_wrapper_t *)w);
}

ham_bool_t
proto_has_cursor_create_request(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->type()==Wrapper::CURSOR_CREATE_REQUEST) {
        ham_assert(w->has_cursor_create_request()==true, (""));
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_cursor_create_request()==false, (""));
        return (HAM_FALSE);
    }
}

ham_u64_t
proto_cursor_create_request_get_db_handle(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->cursor_create_request().db_handle());
}

ham_u64_t
proto_cursor_create_request_get_txn_handle(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->cursor_create_request().txn_handle());
}

ham_u32_t
proto_cursor_create_request_get_flags(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->cursor_create_request().flags());
}

proto_wrapper_t *
proto_init_cursor_create_reply(ham_status_t status, ham_u64_t handle)
{
    Wrapper *w=new Wrapper();
    w->set_type(Wrapper::CURSOR_CREATE_REPLY);
    w->mutable_cursor_create_reply()->set_status(status);
    w->mutable_cursor_create_reply()->set_cursor_handle(handle);
    return ((proto_wrapper_t *)w);
}

ham_bool_t
proto_has_cursor_create_reply(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->type()==Wrapper::CURSOR_CREATE_REPLY) {
        ham_assert(w->has_cursor_create_reply()==true, (""));
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_cursor_create_reply()==false, (""));
        return (HAM_FALSE);
    }
}

ham_u32_t
proto_cursor_create_reply_get_status(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->cursor_create_reply().status());
}

ham_u64_t
proto_cursor_create_reply_get_cursor_handle(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->cursor_create_reply().cursor_handle());
}

proto_wrapper_t *
proto_init_cursor_clone_request(ham_u64_t cursorhandle)
{
    Wrapper *w=new Wrapper();
    w->set_type(Wrapper::CURSOR_CLONE_REQUEST);
    w->mutable_cursor_clone_request()->set_cursor_handle(cursorhandle);
    return ((proto_wrapper_t *)w);
}

ham_bool_t
proto_has_cursor_clone_request(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->type()==Wrapper::CURSOR_CLONE_REQUEST) {
        ham_assert(w->has_cursor_clone_request()==true, (""));
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_cursor_clone_request()==false, (""));
        return (HAM_FALSE);
    }
}

ham_u64_t
proto_cursor_clone_request_get_cursor_handle(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->cursor_clone_request().cursor_handle());
}

proto_wrapper_t *
proto_init_cursor_clone_reply(ham_status_t status, ham_u64_t cursorhandle)
{
    Wrapper *w=new Wrapper();
    w->set_type(Wrapper::CURSOR_CLONE_REPLY);
    w->mutable_cursor_clone_reply()->set_status(status);
    w->mutable_cursor_clone_reply()->set_cursor_handle(cursorhandle);
    return ((proto_wrapper_t *)w);
}

ham_bool_t
proto_has_cursor_clone_reply(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->type()==Wrapper::CURSOR_CLONE_REPLY) {
        ham_assert(w->has_cursor_clone_reply()==true, (""));
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_cursor_clone_reply()==false, (""));
        return (HAM_FALSE);
    }
}

ham_u32_t
proto_cursor_clone_reply_get_status(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->cursor_clone_reply().status());
}

ham_u64_t
proto_cursor_clone_reply_get_cursor_handle(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->cursor_clone_reply().cursor_handle());
}

proto_wrapper_t *
proto_init_cursor_close_request(ham_u64_t cursorhandle)
{
    Wrapper *w=new Wrapper();
    w->set_type(Wrapper::CURSOR_CLOSE_REQUEST);
    w->mutable_cursor_close_request()->set_cursor_handle(cursorhandle);
    return ((proto_wrapper_t *)w);
}

ham_bool_t
proto_has_cursor_close_request(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->type()==Wrapper::CURSOR_CLOSE_REQUEST) {
        ham_assert(w->has_cursor_close_request()==true, (""));
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_cursor_close_request()==false, (""));
        return (HAM_FALSE);
    }
}

ham_u64_t
proto_cursor_close_request_get_cursor_handle(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->cursor_close_request().cursor_handle());
}

proto_wrapper_t *
proto_init_cursor_close_reply(ham_status_t status)
{
    Wrapper *w=new Wrapper();
    w->set_type(Wrapper::CURSOR_CLOSE_REPLY);
    w->mutable_cursor_close_reply()->set_status(status);
    return ((proto_wrapper_t *)w);
}

ham_bool_t
proto_has_cursor_close_reply(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->type()==Wrapper::CURSOR_CLOSE_REPLY) {
        ham_assert(w->has_cursor_close_reply()==true, (""));
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_cursor_close_reply()==false, (""));
        return (HAM_FALSE);
    }
}

ham_u32_t
proto_cursor_close_reply_get_status(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->cursor_close_reply().status());
}

proto_wrapper_t *
proto_init_cursor_insert_request(ham_u64_t cursorhandle, ham_key_t *key,
        ham_record_t *record, ham_u32_t flags)
{
    Wrapper *w=new Wrapper();
    w->set_type(Wrapper::CURSOR_INSERT_REQUEST);
    w->mutable_cursor_insert_request()->set_cursor_handle(cursorhandle);
    w->mutable_cursor_insert_request()->set_flags(flags);
    if (key)
        assign_key(w->mutable_cursor_insert_request()->mutable_key(), 
                key);
    if (record)
        assign_record(w->mutable_cursor_insert_request()->mutable_record(), 
                record);
    return ((proto_wrapper_t *)w);
}

ham_bool_t
proto_has_cursor_insert_request(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->type()==Wrapper::CURSOR_INSERT_REQUEST) {
        ham_assert(w->has_cursor_insert_request()==true, (""));
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_cursor_insert_request()==false, (""));
        return (HAM_FALSE);
    }
}

ham_u64_t
proto_cursor_insert_request_get_cursor_handle(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->cursor_insert_request().cursor_handle());
}

ham_u32_t
proto_cursor_insert_request_get_flags(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->cursor_insert_request().flags());
}

ham_bool_t
proto_cursor_insert_request_has_key(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->cursor_insert_request().has_key());
}

ham_u32_t
proto_cursor_insert_request_get_key_flags(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->cursor_insert_request().key().flags());
}

void *
proto_cursor_insert_request_get_key_data(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->cursor_insert_request().key().has_data())
        return ((void *)&w->cursor_insert_request().key().data()[0]);
    else
        return (0);
}

ham_size_t
proto_cursor_insert_request_get_key_size(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->cursor_insert_request().key().data().size());
}

ham_bool_t
proto_cursor_insert_request_has_record(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->cursor_insert_request().has_record());
}

ham_u32_t
proto_cursor_insert_request_get_record_flags(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->cursor_insert_request().record().flags());
}

void *
proto_cursor_insert_request_get_record_data(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->cursor_insert_request().record().has_data())
        return ((void *)&w->cursor_insert_request().record().data()[0]);
    else
        return (0);
}

ham_size_t
proto_cursor_insert_request_get_record_size(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->cursor_insert_request().record().data().size());
}

ham_offset_t
proto_cursor_insert_request_get_record_partial_offset(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->cursor_insert_request().record().partial_offset());
}

ham_offset_t
proto_cursor_insert_request_get_record_partial_size(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->cursor_insert_request().record().partial_size());
}

proto_wrapper_t *
proto_init_cursor_insert_reply(ham_status_t status, ham_key_t *key)
{
    Wrapper *w=new Wrapper();
    w->set_type(Wrapper::CURSOR_INSERT_REPLY);
    w->mutable_cursor_insert_reply()->set_status(status);
    if (key)
        assign_key(w->mutable_cursor_insert_reply()->mutable_key(), key);
    return ((proto_wrapper_t *)w);
}

ham_bool_t
proto_has_cursor_insert_reply(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->type()==Wrapper::CURSOR_INSERT_REPLY) {
        ham_assert(w->has_cursor_insert_reply()==true, (""));
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_cursor_insert_reply()==false, (""));
        return (HAM_FALSE);
    }
}

ham_u32_t
proto_cursor_insert_reply_get_status(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->cursor_insert_reply().status());
}

ham_bool_t
proto_cursor_insert_reply_has_key(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->cursor_insert_reply().has_key());
}

void *
proto_cursor_insert_reply_get_key_data(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->cursor_insert_reply().key().has_data())
        return ((void *)&w->cursor_insert_reply().key().data()[0]);
    else
        return (0);
}

ham_size_t
proto_cursor_insert_reply_get_key_size(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->cursor_insert_reply().key().data().size());
}

proto_wrapper_t *
proto_init_cursor_erase_request(ham_u64_t cursorhandle, ham_u32_t flags)
{
    Wrapper *w=new Wrapper();
    w->set_type(Wrapper::CURSOR_ERASE_REQUEST);
    w->mutable_cursor_erase_request()->set_cursor_handle(cursorhandle);
    w->mutable_cursor_erase_request()->set_flags(flags);
    return ((proto_wrapper_t *)w);
}

ham_bool_t
proto_has_cursor_erase_request(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->type()==Wrapper::CURSOR_ERASE_REQUEST) {
        ham_assert(w->has_cursor_erase_request()==true, (""));
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_cursor_erase_request()==false, (""));
        return (HAM_FALSE);
    }
}

ham_u64_t
proto_cursor_erase_request_get_cursor_handle(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->cursor_erase_request().cursor_handle());
}

ham_u32_t
proto_cursor_erase_request_get_flags(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->cursor_erase_request().flags());
}

proto_wrapper_t *
proto_init_cursor_erase_reply(ham_status_t status)
{
    Wrapper *w=new Wrapper();
    w->set_type(Wrapper::CURSOR_ERASE_REPLY);
    w->mutable_cursor_erase_reply()->set_status(status);
    return ((proto_wrapper_t *)w);
}

ham_bool_t
proto_has_cursor_erase_reply(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->type()==Wrapper::CURSOR_ERASE_REPLY) {
        ham_assert(w->has_cursor_erase_reply()==true, (""));
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_cursor_erase_reply()==false, (""));
        return (HAM_FALSE);
    }
}

ham_u32_t
proto_cursor_erase_reply_get_status(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->cursor_erase_reply().status());
}

proto_wrapper_t *
proto_init_cursor_find_request(ham_u64_t cursorhandle, ham_key_t *key,
        ham_record_t *record, ham_u32_t flags)
{
    Wrapper *w=new Wrapper();
    w->set_type(Wrapper::CURSOR_FIND_REQUEST);
    w->mutable_cursor_find_request()->set_cursor_handle(cursorhandle);
    w->mutable_cursor_find_request()->set_flags(flags);
    if (key)
        assign_key(w->mutable_cursor_find_request()->mutable_key(), 
                key);
    if (record)
        assign_record(w->mutable_cursor_find_request()->mutable_record(), 
                record);
    return ((proto_wrapper_t *)w);
}

ham_bool_t
proto_has_cursor_find_request(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->type()==Wrapper::CURSOR_FIND_REQUEST) {
        ham_assert(w->has_cursor_find_request()==true, (""));
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_cursor_find_request()==false, (""));
        return (HAM_FALSE);
    }
}

ham_u64_t
proto_cursor_find_request_get_cursor_handle(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->cursor_find_request().cursor_handle());
}

ham_u32_t
proto_cursor_find_request_get_flags(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->cursor_find_request().flags());
}

void *
proto_cursor_find_request_get_key_data(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->cursor_find_request().key().has_data())
        return ((void *)&w->cursor_find_request().key().data()[0]);
    else
        return (0);
}

ham_size_t
proto_cursor_find_request_get_key_size(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->cursor_find_request().key().data().size());
}

ham_bool_t
proto_cursor_find_request_has_record(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->cursor_find_request().has_record());
}

ham_size_t
proto_cursor_find_request_get_key_flags(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->cursor_find_request().key().flags());
}

ham_u32_t
proto_cursor_find_request_get_record_flags(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->cursor_find_request().record().flags());
}

void *
proto_cursor_find_request_get_record_data(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->cursor_find_request().record().has_data())
        return ((void *)&w->cursor_find_request().record().data()[0]);
    else
        return (0);
}

ham_size_t
proto_cursor_find_request_get_record_size(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->cursor_find_request().record().data().size());
}

ham_offset_t
proto_cursor_find_request_get_record_partial_offset(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->cursor_find_request().record().partial_offset());
}

ham_offset_t
proto_cursor_find_request_get_record_partial_size(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->cursor_find_request().record().partial_size());
}

proto_wrapper_t *
proto_init_cursor_find_reply(ham_status_t status, ham_key_t *key,
        ham_record_t *record)
{
    Wrapper *w=new Wrapper();
    w->set_type(Wrapper::CURSOR_FIND_REPLY);
    w->mutable_cursor_find_reply()->set_status(status);
    if (key)
        assign_key(w->mutable_cursor_find_reply()->mutable_key(), key);
    if (record)
        assign_record(w->mutable_cursor_find_reply()->mutable_record(), record);
    return ((proto_wrapper_t *)w);
}

ham_bool_t
proto_has_cursor_find_reply(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->type()==Wrapper::CURSOR_FIND_REPLY) {
        ham_assert(w->has_cursor_find_reply()==true, (""));
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_cursor_find_reply()==false, (""));
        return (HAM_FALSE);
    }
}

ham_u32_t
proto_cursor_find_reply_get_status(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->cursor_find_reply().status());
}

ham_bool_t
proto_cursor_find_reply_has_key(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->cursor_find_reply().has_key());
}

void *
proto_cursor_find_reply_get_key_data(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->cursor_find_reply().key().has_data())
        return ((void *)&w->cursor_find_reply().key().data()[0]);
    else
        return (0);
}

ham_u32_t
proto_cursor_find_reply_get_key_intflags(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->cursor_find_reply().key().intflags());
}

ham_size_t
proto_cursor_find_reply_get_key_size(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->cursor_find_reply().key().data().size());
}

ham_bool_t
proto_cursor_find_reply_has_record(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->cursor_find_reply().has_record());
}

void *
proto_cursor_find_reply_get_record_data(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->cursor_find_reply().record().has_data())
        return ((void *)&w->cursor_find_reply().record().data()[0]);
    else
        return (0);
}

ham_size_t
proto_cursor_find_reply_get_record_size(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->cursor_find_reply().record().data().size());
}

proto_wrapper_t *
proto_init_cursor_get_duplicate_count_request(ham_u64_t cursorhandle, 
                ham_u32_t flags)
{
    Wrapper *w=new Wrapper();
    w->set_type(Wrapper::CURSOR_GET_DUPLICATE_COUNT_REQUEST);
    w->mutable_cursor_get_duplicate_count_request()->set_cursor_handle(
                    cursorhandle);
    w->mutable_cursor_get_duplicate_count_request()->set_flags(flags);
    return ((proto_wrapper_t *)w);
}

ham_bool_t
proto_has_cursor_get_duplicate_count_request(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->type()==Wrapper::CURSOR_GET_DUPLICATE_COUNT_REQUEST) {
        ham_assert(w->has_cursor_get_duplicate_count_request()==true, (""));
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_cursor_get_duplicate_count_request()==false, (""));
        return (HAM_FALSE);
    }
}

ham_u64_t
proto_cursor_get_duplicate_count_request_get_cursor_handle(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->cursor_get_duplicate_count_request().cursor_handle());
}

ham_u32_t
proto_cursor_get_duplicate_count_request_get_flags(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->cursor_get_duplicate_count_request().flags());
}

proto_wrapper_t *
proto_init_cursor_get_duplicate_count_reply(ham_status_t status,
                ham_u64_t count)
{
    Wrapper *w=new Wrapper();
    w->set_type(Wrapper::CURSOR_GET_DUPLICATE_COUNT_REPLY);
    w->mutable_cursor_get_duplicate_count_reply()->set_status(status);
    w->mutable_cursor_get_duplicate_count_reply()->set_count(count);
    return ((proto_wrapper_t *)w);
}

ham_bool_t
proto_has_cursor_get_duplicate_count_reply(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->type()==Wrapper::CURSOR_GET_DUPLICATE_COUNT_REPLY) {
        ham_assert(w->has_cursor_get_duplicate_count_reply()==true, (""));
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_cursor_get_duplicate_count_reply()==false, (""));
        return (HAM_FALSE);
    }
}

ham_u32_t
proto_cursor_get_duplicate_count_reply_get_status(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->cursor_get_duplicate_count_reply().status());
}

ham_u32_t
proto_cursor_get_duplicate_count_reply_get_count(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->cursor_get_duplicate_count_reply().count());
}

proto_wrapper_t *
proto_init_cursor_overwrite_request(ham_u64_t cursorhandle, 
                ham_record_t *record, ham_u32_t flags)
{
    Wrapper *w=new Wrapper();
    w->set_type(Wrapper::CURSOR_OVERWRITE_REQUEST);
    w->mutable_cursor_overwrite_request()->set_cursor_handle(cursorhandle);
    w->mutable_cursor_overwrite_request()->set_flags(flags);
    if (record)
        assign_record(w->mutable_cursor_overwrite_request()->mutable_record(),
                    record);
    return ((proto_wrapper_t *)w);
}

ham_bool_t
proto_has_cursor_overwrite_request(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->type()==Wrapper::CURSOR_OVERWRITE_REQUEST) {
        ham_assert(w->has_cursor_overwrite_request()==true, (""));
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_cursor_overwrite_request()==false, (""));
        return (HAM_FALSE);
    }
}

ham_u64_t
proto_cursor_overwrite_request_get_cursor_handle(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->cursor_overwrite_request().cursor_handle());
}

ham_u32_t
proto_cursor_overwrite_request_get_flags(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->cursor_overwrite_request().flags());
}

ham_u32_t
proto_cursor_overwrite_request_get_record_flags(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->cursor_overwrite_request().record().flags());
}

void *
proto_cursor_overwrite_request_get_record_data(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->cursor_overwrite_request().record().has_data())
        return ((void *)&w->cursor_overwrite_request().record().data()[0]);
    else
        return (0);
}

ham_size_t
proto_cursor_overwrite_request_get_record_size(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->cursor_overwrite_request().record().data().size());
}

ham_offset_t
proto_cursor_overwrite_request_get_record_partial_offset(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->cursor_overwrite_request().record().partial_offset());
}

ham_offset_t
proto_cursor_overwrite_request_get_record_partial_size(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->cursor_overwrite_request().record().partial_size());
}

proto_wrapper_t *
proto_init_cursor_overwrite_reply(ham_status_t status)
{
    Wrapper *w=new Wrapper();
    w->set_type(Wrapper::CURSOR_OVERWRITE_REPLY);
    w->mutable_cursor_overwrite_reply()->set_status(status);
    return ((proto_wrapper_t *)w);
}

ham_bool_t
proto_has_cursor_overwrite_reply(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->type()==Wrapper::CURSOR_OVERWRITE_REPLY) {
        ham_assert(w->has_cursor_overwrite_reply()==true, (""));
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_cursor_overwrite_reply()==false, (""));
        return (HAM_FALSE);
    }
}

ham_u32_t
proto_cursor_overwrite_reply_get_status(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->cursor_overwrite_reply().status());
}

proto_wrapper_t *
proto_init_cursor_move_request(ham_u64_t cursorhandle, ham_key_t *key,
        ham_record_t *record, ham_u32_t flags)
{
    Wrapper *w=new Wrapper();
    w->set_type(Wrapper::CURSOR_MOVE_REQUEST);
    w->mutable_cursor_move_request()->set_cursor_handle(cursorhandle);
    w->mutable_cursor_move_request()->set_flags(flags);
    if (key)
        assign_key(w->mutable_cursor_move_request()->mutable_key(),
                    key);
    if (record)
        assign_record(w->mutable_cursor_move_request()->mutable_record(),
                    record);
    return ((proto_wrapper_t *)w);
}

ham_bool_t
proto_has_cursor_move_request(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->type()==Wrapper::CURSOR_MOVE_REQUEST) {
        ham_assert(w->has_cursor_move_request()==true, (""));
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_cursor_move_request()==false, (""));
        return (HAM_FALSE);
    }
}

ham_u64_t
proto_cursor_move_request_get_cursor_handle(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->cursor_move_request().cursor_handle());
}

ham_u32_t
proto_cursor_move_request_get_flags(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->cursor_move_request().flags());
}

ham_bool_t
proto_cursor_move_request_has_key(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->cursor_move_request().has_key());
}

void *
proto_cursor_move_request_get_key_data(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->cursor_move_request().key().has_data())
        return ((void *)&w->cursor_move_request().key().data()[0]);
    else
        return (0);
}

ham_u32_t
proto_cursor_move_request_get_key_flags(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->cursor_move_request().key().flags());
}

ham_size_t
proto_cursor_move_request_get_key_size(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->cursor_move_request().key().data().size());
}

ham_bool_t
proto_cursor_move_request_has_record(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->cursor_move_request().has_record());
}

void *
proto_cursor_move_request_get_record_data(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->cursor_move_request().record().has_data())
        return ((void *)&w->cursor_move_request().record().data()[0]);
    else
        return (0);
}

ham_size_t
proto_cursor_move_request_get_record_size(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->cursor_move_request().record().data().size());
}

ham_u32_t
proto_cursor_move_request_get_record_flags(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->cursor_move_request().record().flags());
}

ham_offset_t
proto_cursor_move_request_get_record_partial_offset(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->cursor_move_request().record().partial_offset());
}

ham_offset_t
proto_cursor_move_request_get_record_partial_size(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->cursor_move_request().record().partial_size());
}

proto_wrapper_t *
proto_init_cursor_move_reply(ham_status_t status, ham_key_t *key,
        ham_record_t *record)
{
    Wrapper *w=new Wrapper();
    w->set_type(Wrapper::CURSOR_MOVE_REPLY);
    w->mutable_cursor_move_reply()->set_status(status);
    if (key)
        assign_key(w->mutable_cursor_move_reply()->mutable_key(), key);
    if (record)
        assign_record(w->mutable_cursor_move_reply()->mutable_record(), record);
    return ((proto_wrapper_t *)w);
}

ham_bool_t
proto_has_cursor_move_reply(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->type()==Wrapper::CURSOR_MOVE_REPLY) {
        ham_assert(w->has_cursor_move_reply()==true, (""));
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_cursor_move_reply()==false, (""));
        return (HAM_FALSE);
    }
}

ham_u32_t
proto_cursor_move_reply_get_status(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->cursor_move_reply().status());
}

ham_bool_t
proto_cursor_move_reply_has_key(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->cursor_move_reply().has_key());
}

void *
proto_cursor_move_reply_get_key_data(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->cursor_move_reply().key().has_data())
        return ((void *)&w->cursor_move_reply().key().data()[0]);
    else
        return (0);
}

ham_u32_t
proto_cursor_move_reply_get_key_intflags(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->cursor_move_reply().key().intflags());
}

ham_size_t
proto_cursor_move_reply_get_key_size(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->cursor_move_reply().key().data().size());
}

ham_bool_t
proto_cursor_move_reply_has_record(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->cursor_move_reply().has_record());
}

void *
proto_cursor_move_reply_get_record_data(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->cursor_move_reply().record().has_data())
        return ((void *)&w->cursor_move_reply().record().data()[0]);
    else
        return (0);
}

ham_size_t
proto_cursor_move_reply_get_record_size(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    return (w->cursor_move_reply().record().data().size());
}

