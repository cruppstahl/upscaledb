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
    Wrapper *w=new Wrapper;
    if (!w->ParseFromArray(buf, size)) {
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
    
    return (HAM_TRUE);
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
    if (w->type()==Wrapper::ENV_GET_DATABASE_NAMES_REPLY) {
        ham_assert(w->has_env_get_database_names_reply()==true, (""));
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_env_get_database_names_reply()==false, (""));
        return (HAM_FALSE);
    }
}

ham_bool_t
proto_has_env_get_database_names_reply(proto_wrapper_t *wrapper)
{
    Wrapper *w=(Wrapper *)wrapper;
    if (w->type()==Wrapper::ENV_GET_DATABASE_NAMES_REPLY) {
        ham_assert(w->has_env_get_database_names_request()==true, (""));
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_env_get_database_names_request()==false, (""));
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
    return (0);
}

ham_bool_t
proto_env_get_parameters_reply_has_cachesize(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_u32_t
proto_env_get_parameters_reply_get_cachesize(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_bool_t
proto_env_get_parameters_reply_has_pagesize(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_u32_t
proto_env_get_parameters_reply_get_pagesize(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_bool_t
proto_env_get_parameters_reply_has_max_env_databases(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_u32_t
proto_env_get_parameters_reply_get_max_env_databases(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_bool_t
proto_env_get_parameters_reply_has_flags(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_u32_t
proto_env_get_parameters_reply_get_flags(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_bool_t
proto_env_get_parameters_reply_has_filemode(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_u32_t
proto_env_get_parameters_reply_get_filemode(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_bool_t
proto_env_get_parameters_reply_has_filename(proto_wrapper_t *wrapper)
{
    return (0);
}

const char *
proto_env_get_parameters_reply_get_filename(proto_wrapper_t *wrapper)
{
    return (0);
}

proto_wrapper_t *
proto_init_env_flush_request(ham_u32_t flags)
{
    return (0);
}

ham_bool_t
proto_has_env_flush_request(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_bool_t
proto_has_env_flush_reply(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_u32_t
proto_env_flush_reply_get_status(proto_wrapper_t *wrapper)
{
    return (0);
}

proto_wrapper_t *
proto_init_env_create_db_request(ham_u16_t dbname, ham_u32_t flags, 
                ham_u32_t *names, ham_u64_t *values, ham_u32_t num_params)
{
    return (0);
}

ham_bool_t
proto_has_env_create_db_request(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_bool_t
proto_has_env_create_db_reply(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_u32_t
proto_env_create_db_reply_get_status(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_u32_t
proto_env_create_db_reply_get_flags(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_u64_t
proto_env_create_db_reply_get_db_handle(proto_wrapper_t *wrapper)
{
    return (0);
}

proto_wrapper_t *
proto_init_env_open_db_request(ham_u16_t dbname, ham_u32_t flags, 
                ham_u32_t *names, ham_u64_t *values, ham_u32_t num_params)
{
    return (0);
}

ham_bool_t
proto_has_env_open_db_request(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_bool_t
proto_has_env_open_db_reply(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_u32_t
proto_env_open_db_reply_get_status(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_u32_t
proto_env_open_db_reply_get_flags(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_u64_t
proto_env_open_db_reply_get_db_handle(proto_wrapper_t *wrapper)
{
    return (0);
}

proto_wrapper_t *
proto_init_txn_begin_request(ham_u64_t dbhandle, ham_u32_t flags)
{
    return (0);
}

ham_bool_t
proto_has_txn_begin_request(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_bool_t
proto_has_txn_begin_reply(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_u32_t
proto_txn_begin_reply_get_status(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_u64_t
proto_txn_begin_reply_get_txn_handle(proto_wrapper_t *wrapper)
{
    return (0);
}

proto_wrapper_t *
proto_init_txn_commit_request(ham_u64_t txnhandle, ham_u32_t flags)
{
    return (0);
}

ham_bool_t
proto_has_txn_commit_request(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_bool_t
proto_has_txn_commit_reply(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_u32_t
proto_txn_commit_reply_get_status(proto_wrapper_t *wrapper)
{
    return (0);
}

proto_wrapper_t *
proto_init_txn_abort_request(ham_u64_t txnhandle, ham_u32_t flags)
{
    return (0);
}

ham_bool_t
proto_has_txn_abort_request(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_bool_t
proto_has_txn_abort_reply(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_u32_t
proto_txn_abort_reply_get_status(proto_wrapper_t *wrapper)
{
    return (0);
}

proto_wrapper_t *
proto_init_db_close_request(ham_u64_t dbhandle, ham_u32_t flags)
{
    return (0);
}

ham_bool_t
proto_has_db_close_request(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_bool_t
proto_has_db_close_reply(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_u32_t
proto_db_close_reply_get_status(proto_wrapper_t *wrapper)
{
    return (0);
}

proto_wrapper_t *
proto_init_db_get_parameters_request(ham_u64_t dbhandle, ham_u32_t *names, 
                ham_u32_t names_size)
{
    return (0);
}

ham_bool_t
proto_has_db_get_parameters_request(proto_wrapper_t *wrapper)
{
    return (0);
}


ham_bool_t
proto_has_db_get_parameters_reply(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_u32_t
proto_db_get_parameters_reply_get_status(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_bool_t
proto_db_get_parameters_reply_has_cachesize(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_u32_t
proto_db_get_parameters_reply_get_cachesize(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_bool_t
proto_db_get_parameters_reply_has_pagesize(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_u32_t
proto_db_get_parameters_reply_get_pagesize(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_bool_t
proto_db_get_parameters_reply_has_max_env_databases(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_u32_t
proto_db_get_parameters_reply_get_max_env_databases(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_bool_t
proto_db_get_parameters_reply_has_flags(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_u32_t
proto_db_get_parameters_reply_get_flags(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_bool_t
proto_db_get_parameters_reply_has_filemode(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_u32_t
proto_db_get_parameters_reply_get_filemode(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_bool_t
proto_db_get_parameters_reply_has_filename(proto_wrapper_t *wrapper)
{
    return (0);
}

const char *
proto_db_get_parameters_reply_get_filename(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_bool_t
proto_db_get_parameters_reply_has_keysize(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_u32_t
proto_db_get_parameters_reply_get_keysize(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_bool_t
proto_db_get_parameters_reply_has_dbname(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_u32_t
proto_db_get_parameters_reply_get_dbname(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_bool_t
proto_db_get_parameters_reply_has_keys_per_page(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_u32_t
proto_db_get_parameters_reply_get_keys_per_page(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_bool_t
proto_db_get_parameters_reply_has_dam(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_u32_t
proto_db_get_parameters_reply_get_dam(proto_wrapper_t *wrapper)
{
    return (0);
}

proto_wrapper_t *
proto_init_db_flush_request(ham_u64_t dbhandle, ham_u32_t flags)
{
    return (0);
}

ham_bool_t
proto_has_db_flush_request(proto_wrapper_t *wrapper)
{
    return (0);
}


ham_bool_t
proto_has_db_flush_reply(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_u32_t
proto_db_flush_reply_get_status(proto_wrapper_t *wrapper)
{
    return (0);
}

proto_wrapper_t *
proto_init_check_integrity_request(ham_u64_t dbhandle, ham_u64_t txnhandle)
{
    return (0);
}

ham_bool_t
proto_has_check_integrity_request(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_bool_t
proto_has_check_integrity_reply(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_u32_t
proto_check_integrity_reply_get_status(proto_wrapper_t *wrapper)
{
    return (0);
}

proto_wrapper_t *
proto_init_db_get_key_count_request(ham_u64_t dbhandle, ham_u64_t txnhandle, 
                ham_u32_t flags)
{
    return (0);
}

ham_bool_t
proto_has_db_get_key_count_request(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_bool_t
proto_has_db_get_key_count_reply(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_u32_t
proto_db_get_key_count_reply_get_status(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_u64_t
proto_db_get_key_count_reply_get_key_count(proto_wrapper_t *wrapper)
{
    return (0);
}

proto_wrapper_t *
proto_init_db_insert_request(ham_u64_t dbhandle, ham_u64_t txnhandle, 
                ham_key_t *key, ham_record_t *record, ham_u32_t flags)
{
    return (0);
}

ham_bool_t
proto_has_db_insert_request(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_bool_t
proto_has_db_insert_reply(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_u32_t
proto_db_insert_reply_get_status(proto_wrapper_t *wrapper)
{
    return (0);
}

void *
proto_db_insert_reply_get_key_data(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_size_t
proto_db_insert_reply_get_key_size(proto_wrapper_t *wrapper)
{
    return (0);
}

proto_wrapper_t *
proto_init_db_find_request(ham_u64_t dbhandle, ham_u64_t txnhandle, 
                ham_key_t *key, ham_record_t *record, ham_u32_t flags)
{
    return (0);
}

ham_bool_t
proto_has_db_find_request(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_bool_t
proto_has_db_find_reply(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_u32_t
proto_db_find_reply_get_status(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_bool_t
proto_db_find_reply_has_key(proto_wrapper_t *wrapper)
{
    return (0);
}

void *
proto_db_find_reply_get_key_data(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_u32_t
proto_db_find_reply_get_key_intflags(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_size_t
proto_db_find_reply_get_key_size(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_bool_t
proto_db_find_reply_has_record(proto_wrapper_t *wrapper)
{
    return (0);
}

void *
proto_db_find_reply_get_record_data(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_size_t
proto_db_find_reply_get_record_size(proto_wrapper_t *wrapper)
{
    return (0);
}

proto_wrapper_t *
proto_init_db_erase_request(ham_u64_t dbhandle, ham_u64_t txnhandle, 
                ham_key_t *key, ham_u32_t flags)
{
    return (0);
}

ham_bool_t
proto_has_db_erase_request(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_bool_t
proto_has_db_erase_reply(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_u32_t
proto_db_erase_reply_get_status(proto_wrapper_t *wrapper)
{
    return (0);
}

proto_wrapper_t *
proto_init_cursor_create_request(ham_u64_t dbhandle, ham_u64_t txnhandle, 
                ham_u32_t flags)
{
    return (0);
}

ham_bool_t
proto_has_cursor_create_request(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_bool_t
proto_has_cursor_create_reply(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_u32_t
proto_cursor_create_reply_get_status(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_u64_t
proto_cursor_create_reply_get_cursor_handle(proto_wrapper_t *wrapper)
{
    return (0);
}

proto_wrapper_t *
proto_init_cursor_clone_request(ham_u64_t cursorhandle)
{
    return (0);
}

ham_bool_t
proto_has_cursor_clone_request(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_bool_t
proto_has_cursor_clone_reply(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_u32_t
proto_cursor_clone_reply_get_status(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_u64_t
proto_cursor_clone_reply_get_cursor_handle(proto_wrapper_t *wrapper)
{
    return (0);
}

proto_wrapper_t *
proto_init_cursor_close_request(ham_u64_t cursorhandle)
{
    return (0);
}

ham_bool_t
proto_has_cursor_close_request(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_bool_t
proto_has_cursor_close_reply(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_u32_t
proto_cursor_close_reply_get_status(proto_wrapper_t *wrapper)
{
    return (0);
}

proto_wrapper_t *
proto_init_cursor_insert_request(ham_u64_t cursorhandle, ham_key_t *key,
        ham_record_t *record, ham_u32_t flags)
{
    return (0);
}

ham_bool_t
proto_has_cursor_insert_request(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_bool_t
proto_has_cursor_insert_reply(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_u32_t
proto_cursor_insert_reply_get_status(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_bool_t
proto_cursor_insert_reply_has_key(proto_wrapper_t *wrapper)
{
    return (0);
}

void *
proto_cursor_insert_reply_get_key_data(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_size_t
proto_cursor_insert_reply_get_key_size(proto_wrapper_t *wrapper)
{
    return (0);
}

proto_wrapper_t *
proto_init_cursor_erase_request(ham_u64_t cursorhandle, ham_u32_t flags)
{
    return (0);
}

ham_bool_t
proto_has_cursor_erase_request(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_bool_t
proto_has_cursor_erase_reply(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_u32_t
proto_cursor_erase_reply_get_status(proto_wrapper_t *wrapper)
{
    return (0);
}

proto_wrapper_t *
proto_init_cursor_find_request(ham_u64_t cursorhandle, ham_key_t *key,
        ham_record_t *record, ham_u32_t flags)
{
    return (0);
}

ham_bool_t
proto_has_cursor_find_request(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_bool_t
proto_has_cursor_find_reply(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_u32_t
proto_cursor_find_reply_get_status(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_bool_t
proto_cursor_find_reply_has_key(proto_wrapper_t *wrapper)
{
    return (0);
}

void *
proto_cursor_find_reply_get_key_data(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_u32_t
proto_cursor_find_reply_get_key_intflags(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_size_t
proto_cursor_find_reply_get_key_size(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_bool_t
proto_cursor_find_reply_has_record(proto_wrapper_t *wrapper)
{
    return (0);
}

void *
proto_cursor_find_reply_get_record_data(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_size_t
proto_cursor_find_reply_get_record_size(proto_wrapper_t *wrapper)
{
    return (0);
}

proto_wrapper_t *
proto_init_cursor_get_duplicate_count_request(ham_u64_t cursorhandle, 
                ham_u32_t flags)
{
    return (0);
}

ham_bool_t
proto_has_cursor_get_duplicate_count_request(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_bool_t
proto_has_cursor_get_duplicate_count_reply(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_u32_t
proto_cursor_get_duplicate_count_reply_get_status(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_u32_t
proto_cursor_get_duplicate_count_reply_get_count(proto_wrapper_t *wrapper)
{
    return (0);
}

proto_wrapper_t *
proto_init_cursor_overwrite_request(ham_u64_t cursorhandle, 
                ham_record_t *record, ham_u32_t flags)
{
    return (0);
}

ham_bool_t
proto_has_cursor_overwrite_request(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_bool_t
proto_has_cursor_overwrite_reply(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_u32_t
proto_cursor_overwrite_reply_get_status(proto_wrapper_t *wrapper)
{
    return (0);
}

proto_wrapper_t *
proto_init_cursor_move_request(ham_u64_t cursorhandle, ham_key_t *key,
        ham_record_t *record, ham_u32_t flags)
{
    return (0);
}

ham_bool_t
proto_has_cursor_move_request(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_bool_t
proto_has_cursor_move_reply(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_u32_t
proto_cursor_move_reply_get_status(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_bool_t
proto_cursor_move_reply_has_key(proto_wrapper_t *wrapper)
{
    return (0);
}

void *
proto_cursor_move_reply_get_key_data(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_u32_t
proto_cursor_move_reply_get_key_intflags(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_size_t
proto_cursor_move_reply_get_key_size(proto_wrapper_t *wrapper)
{
    return (0);
}

void *
proto_cursor_move_reply_get_record_data(proto_wrapper_t *wrapper)
{
    return (0);
}

ham_size_t
proto_cursor_move_reply_get_record_size(proto_wrapper_t *wrapper)
{
    return (0);
}

