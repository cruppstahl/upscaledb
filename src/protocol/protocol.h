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

/**
 * @brief Abstraction layer for the remote protocol
 *
 * Yes, this abstraction layer is ugly. Google's protobuf is based on data
 * interfaces, whereas this layer provides a functional interface. There's
 * no way to avoid ugliness when translating one to the other.
 *
 * The abstraction is necessary because google protobuf uses C++, whereas 
 * hamsterdb is plain C. If someone has an idea how to avoid this mess pls
 * tell me.
 */

#ifndef HAM_PROTOCOL_H__
#define HAM_PROTOCOL_H__

#include <ham/hamsterdb.h>
#include "../mem.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * The different package types; this is copy-pasted from messages.pb.h
 */
enum {
  HAM__WRAPPER__TYPE__CONNECT_REQUEST = 10,
  HAM__WRAPPER__TYPE__CONNECT_REPLY = 11,
  HAM__WRAPPER__TYPE__ENV_RENAME_REQUEST = 20,
  HAM__WRAPPER__TYPE__ENV_RENAME_REPLY = 21,
  HAM__WRAPPER__TYPE__ENV_GET_PARAMETERS_REQUEST = 30,
  HAM__WRAPPER__TYPE__ENV_GET_PARAMETERS_REPLY = 31,
  HAM__WRAPPER__TYPE__ENV_GET_DATABASE_NAMES_REQUEST = 40,
  HAM__WRAPPER__TYPE__ENV_GET_DATABASE_NAMES_REPLY = 41,
  HAM__WRAPPER__TYPE__ENV_FLUSH_REQUEST = 50,
  HAM__WRAPPER__TYPE__ENV_FLUSH_REPLY = 51,
  HAM__WRAPPER__TYPE__ENV_CREATE_DB_REQUEST = 60,
  HAM__WRAPPER__TYPE__ENV_CREATE_DB_REPLY = 61,
  HAM__WRAPPER__TYPE__ENV_OPEN_DB_REQUEST = 70,
  HAM__WRAPPER__TYPE__ENV_OPEN_DB_REPLY = 71,
  HAM__WRAPPER__TYPE__ENV_ERASE_DB_REQUEST = 80,
  HAM__WRAPPER__TYPE__ENV_ERASE_DB_REPLY = 81,
  HAM__WRAPPER__TYPE__DB_CLOSE_REQUEST = 90,
  HAM__WRAPPER__TYPE__DB_CLOSE_REPLY = 91,
  HAM__WRAPPER__TYPE__DB_GET_PARAMETERS_REQUEST = 100,
  HAM__WRAPPER__TYPE__DB_GET_PARAMETERS_REPLY = 101,
  // HAM__WRAPPER__TYPE__DB_FLUSH_REQUEST = 110,
  // HAM__WRAPPER__TYPE__DB_FLUSH_REPLY = 111,
  HAM__WRAPPER__TYPE__TXN_BEGIN_REQUEST = 120,
  HAM__WRAPPER__TYPE__TXN_BEGIN_REPLY = 121,
  HAM__WRAPPER__TYPE__TXN_COMMIT_REQUEST = 130,
  HAM__WRAPPER__TYPE__TXN_COMMIT_REPLY = 131,
  HAM__WRAPPER__TYPE__TXN_ABORT_REQUEST = 140,
  HAM__WRAPPER__TYPE__TXN_ABORT_REPLY = 141,
  HAM__WRAPPER__TYPE__DB_CHECK_INTEGRITY_REQUEST = 150,
  HAM__WRAPPER__TYPE__DB_CHECK_INTEGRITY_REPLY = 151,
  HAM__WRAPPER__TYPE__DB_GET_KEY_COUNT_REQUEST = 160,
  HAM__WRAPPER__TYPE__DB_GET_KEY_COUNT_REPLY = 161,
  HAM__WRAPPER__TYPE__DB_INSERT_REQUEST = 170,
  HAM__WRAPPER__TYPE__DB_INSERT_REPLY = 171,
  HAM__WRAPPER__TYPE__DB_ERASE_REQUEST = 180,
  HAM__WRAPPER__TYPE__DB_ERASE_REPLY = 181,
  HAM__WRAPPER__TYPE__DB_FIND_REQUEST = 190,
  HAM__WRAPPER__TYPE__DB_FIND_REPLY = 191,
  HAM__WRAPPER__TYPE__CURSOR_CREATE_REQUEST = 200,
  HAM__WRAPPER__TYPE__CURSOR_CREATE_REPLY = 201,
  HAM__WRAPPER__TYPE__CURSOR_CLONE_REQUEST = 210,
  HAM__WRAPPER__TYPE__CURSOR_CLONE_REPLY = 211,
  HAM__WRAPPER__TYPE__CURSOR_CLOSE_REQUEST = 220,
  HAM__WRAPPER__TYPE__CURSOR_CLOSE_REPLY = 221,
  HAM__WRAPPER__TYPE__CURSOR_INSERT_REQUEST = 230,
  HAM__WRAPPER__TYPE__CURSOR_INSERT_REPLY = 231,
  HAM__WRAPPER__TYPE__CURSOR_ERASE_REQUEST = 240,
  HAM__WRAPPER__TYPE__CURSOR_ERASE_REPLY = 241,
  HAM__WRAPPER__TYPE__CURSOR_FIND_REQUEST = 250,
  HAM__WRAPPER__TYPE__CURSOR_FIND_REPLY = 251,
  HAM__WRAPPER__TYPE__CURSOR_GET_DUPLICATE_COUNT_REQUEST = 260,
  HAM__WRAPPER__TYPE__CURSOR_GET_DUPLICATE_COUNT_REPLY = 261,
  HAM__WRAPPER__TYPE__CURSOR_OVERWRITE_REQUEST = 270,
  HAM__WRAPPER__TYPE__CURSOR_OVERWRITE_REPLY = 271,
  HAM__WRAPPER__TYPE__CURSOR_MOVE_REQUEST = 280,
  HAM__WRAPPER__TYPE__CURSOR_MOVE_REPLY = 281
};

/* This is a typedef for our internal C++ Wrapper class, which is defined
 * in messages.pb.h */
typedef void proto_wrapper_t;

/* 
 * unpack a buffer into a proto_wrapper_t structure 
 */
extern proto_wrapper_t *
proto_unpack(ham_size_t size, const ham_u8_t *buf);

/* 
 * delete an unpacked proto_wrapper_t structure 
 */
extern void
proto_delete(proto_wrapper_t *wrapper);

/* 
 * packs the proto_wrapper_t structure into a memory buffer and returns
 * a pointer to the buffer and the buffer size 
 */
extern ham_bool_t
proto_pack(proto_wrapper_t *wrapper, mem_allocator_t *alloc, 
            ham_u8_t **data, ham_size_t *size);

/* 
 * get the type of the Wrapper structure
 */
extern ham_u32_t
proto_get_type(proto_wrapper_t *wrapper);

/*
 * shutdown/free globally allocated memory
 */
extern void
proto_shutdown(void);

/*
 * connect request
 */
extern proto_wrapper_t *
proto_init_connect_request(const char *filename);

extern ham_bool_t
proto_has_connect_request(proto_wrapper_t *wrapper);

/*
 * connect reply
 */
extern proto_wrapper_t *
proto_init_connect_reply(ham_u32_t status, ham_u32_t env_flags);

extern ham_bool_t
proto_has_connect_reply(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_connect_reply_get_status(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_connect_reply_get_env_flags(proto_wrapper_t *wrapper);

/*
 * env_rename request
 */
extern proto_wrapper_t *
proto_init_env_rename_request(ham_u16_t oldname, ham_u16_t newname, 
                ham_u32_t flags);

extern ham_u32_t
proto_env_rename_request_get_oldname(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_env_rename_request_get_newname(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_env_rename_request_get_flags(proto_wrapper_t *wrapper);

extern ham_bool_t
proto_has_env_rename_request(proto_wrapper_t *wrapper);

/*
 * env_rename reply
 */
extern proto_wrapper_t *
proto_init_env_rename_reply(ham_status_t status);

extern ham_bool_t
proto_has_env_rename_reply(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_env_rename_reply_get_status(proto_wrapper_t *wrapper);

/*
 * env_erase_db request
 */
extern proto_wrapper_t *
proto_init_env_erase_db_request(ham_u16_t name, ham_u32_t flags);

extern ham_bool_t
proto_has_env_erase_db_request(proto_wrapper_t *wrapper);

extern ham_u16_t
proto_env_erase_db_request_get_dbname(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_env_erase_db_request_get_flags(proto_wrapper_t *wrapper);

/*
 * env_erase_db reply
 */
extern proto_wrapper_t *
proto_init_env_erase_db_reply(ham_status_t status);

extern ham_bool_t
proto_has_env_erase_db_reply(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_env_erase_db_reply_get_status(proto_wrapper_t *wrapper);

/*
 * env_get_database_names request
 */
extern proto_wrapper_t *
proto_init_env_get_database_names_request();

extern ham_bool_t
proto_has_env_get_database_names_request(proto_wrapper_t *wrapper);

/*
 * env_get_database_names reply
 */
extern proto_wrapper_t *
proto_init_env_get_database_names_reply(ham_status_t status, ham_u16_t *names,
                ham_size_t num_names);

extern ham_bool_t
proto_has_env_get_database_names_reply(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_env_get_database_names_reply_get_status(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_env_get_database_names_reply_get_names_size(proto_wrapper_t *wrapper);

extern ham_u32_t *
proto_env_get_database_names_reply_get_names(proto_wrapper_t *wrapper);

/*
 * env_get_parameters request
 */
extern proto_wrapper_t *
proto_init_env_get_parameters_request(ham_u32_t *names, ham_u32_t names_size);

extern ham_u32_t *
proto_env_get_parameters_request_get_names(proto_wrapper_t *wrapper);

extern ham_size_t
proto_env_get_parameters_request_get_names_size(proto_wrapper_t *wrapper);

extern ham_bool_t
proto_has_env_get_parameters_request(proto_wrapper_t *wrapper);

/*
 * env_get_parameters reply
 */
extern proto_wrapper_t *
proto_init_env_get_parameters_reply(ham_u32_t status);

extern ham_bool_t
proto_has_env_get_parameters_reply(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_env_get_parameters_reply_get_status(proto_wrapper_t *wrapper);

extern void
proto_env_get_parameters_reply_set_cachesize(proto_wrapper_t *wrapper,
                ham_u32_t cachesize);

extern ham_bool_t
proto_env_get_parameters_reply_has_cachesize(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_env_get_parameters_reply_get_cachesize(proto_wrapper_t *wrapper);

extern void
proto_env_get_parameters_reply_set_pagesize(proto_wrapper_t *wrapper,
                ham_u32_t pagesize);

extern ham_bool_t
proto_env_get_parameters_reply_has_pagesize(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_env_get_parameters_reply_get_pagesize(proto_wrapper_t *wrapper);

extern void
proto_env_get_parameters_reply_set_max_env_databases(proto_wrapper_t *wrapper,
                ham_u32_t max_env_databases);

extern ham_bool_t
proto_env_get_parameters_reply_has_max_env_databases(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_env_get_parameters_reply_get_max_env_databases(proto_wrapper_t *wrapper);

extern void
proto_env_get_parameters_reply_set_flags(proto_wrapper_t *wrapper,
                ham_u32_t flags);

extern ham_bool_t
proto_env_get_parameters_reply_has_flags(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_env_get_parameters_reply_get_flags(proto_wrapper_t *wrapper);

extern void
proto_env_get_parameters_reply_set_filemode(proto_wrapper_t *wrapper,
                ham_u32_t filemode);

extern ham_bool_t
proto_env_get_parameters_reply_has_filemode(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_env_get_parameters_reply_get_filemode(proto_wrapper_t *wrapper);

extern void
proto_env_get_parameters_reply_set_filename(proto_wrapper_t *wrapper,
                const char *filename);

extern ham_bool_t
proto_env_get_parameters_reply_has_filename(proto_wrapper_t *wrapper);

extern const char *
proto_env_get_parameters_reply_get_filename(proto_wrapper_t *wrapper);

/*
 * env_flush request
 */
extern proto_wrapper_t *
proto_init_env_flush_request(ham_u32_t flags);

extern ham_u32_t
proto_env_flush_request_get_flags(proto_wrapper_t *wrapper);

extern ham_bool_t
proto_has_env_flush_request(proto_wrapper_t *wrapper);

/*
 * env_flush reply
 */
extern proto_wrapper_t *
proto_init_env_flush_reply(ham_status_t status);

extern ham_bool_t
proto_has_env_flush_reply(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_env_flush_reply_get_status(proto_wrapper_t *wrapper);

/*
 * env_create_db request
 */
extern proto_wrapper_t *
proto_init_env_create_db_request(ham_u16_t dbname, ham_u32_t flags, 
                ham_u32_t *names, ham_u64_t *values, ham_u32_t num_params);

extern ham_bool_t
proto_has_env_create_db_request(proto_wrapper_t *wrapper);

extern ham_size_t
proto_env_create_db_request_get_num_params(proto_wrapper_t *wrapper);

extern ham_u32_t *
proto_env_create_db_request_get_param_names(proto_wrapper_t *wrapper);

extern ham_u32_t *
proto_env_create_db_request_get_param_values(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_env_create_db_request_get_dbname(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_env_create_db_request_get_flags(proto_wrapper_t *wrapper);

/*
 * env_create_db reply
 */
extern proto_wrapper_t *
proto_init_env_create_db_reply(ham_status_t status, ham_u64_t db_handle,
                ham_u32_t db_flags);

extern ham_bool_t
proto_has_env_create_db_reply(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_env_create_db_reply_get_status(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_env_create_db_reply_get_flags(proto_wrapper_t *wrapper);

extern ham_u64_t
proto_env_create_db_reply_get_db_handle(proto_wrapper_t *wrapper);

/*
 * env_open_db request
 */
extern proto_wrapper_t *
proto_init_env_open_db_request(ham_u16_t dbname,
            ham_u32_t flags, ham_u32_t *names, ham_u64_t *values,
            ham_u32_t num_params);

extern ham_bool_t
proto_has_env_open_db_request(proto_wrapper_t *wrapper);

extern ham_size_t
proto_env_open_db_request_get_num_params(proto_wrapper_t *wrapper);

extern ham_u32_t *
proto_env_open_db_request_get_param_names(proto_wrapper_t *wrapper);

extern ham_u32_t *
proto_env_open_db_request_get_param_values(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_env_open_db_request_get_dbname(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_env_open_db_request_get_flags(proto_wrapper_t *wrapper);

/*
 * env_open_db reply
 */
extern proto_wrapper_t *
proto_init_env_open_db_reply(ham_status_t status, ham_u64_t dbhandle,
                ham_u32_t flags);

extern ham_bool_t
proto_has_env_open_db_reply(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_env_open_db_reply_get_status(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_env_open_db_reply_get_flags(proto_wrapper_t *wrapper);

extern ham_u64_t
proto_env_open_db_reply_get_db_handle(proto_wrapper_t *wrapper);

/*
 * txn_begin request
 */
extern proto_wrapper_t *
proto_init_txn_begin_request(ham_u64_t dbhandle, ham_u32_t flags);

extern ham_bool_t
proto_has_txn_begin_request(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_txn_begin_request_get_flags(proto_wrapper_t *wrapper);

extern ham_u64_t
proto_txn_begin_request_get_db_handle(proto_wrapper_t *wrapper);

/*
 * txn_begin reply
 */
extern proto_wrapper_t *
proto_init_txn_begin_reply(ham_status_t status, ham_u64_t txnhandle);

extern ham_bool_t
proto_has_txn_begin_reply(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_txn_begin_reply_get_status(proto_wrapper_t *wrapper);

extern ham_u64_t
proto_txn_begin_reply_get_txn_handle(proto_wrapper_t *wrapper);

/*
 * txn_commit request
 */
extern proto_wrapper_t *
proto_init_txn_commit_request(ham_u64_t txnhandle, ham_u32_t flags);

extern ham_bool_t
proto_has_txn_commit_request(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_txn_commit_request_get_flags(proto_wrapper_t *wrapper);

extern ham_u64_t
proto_txn_commit_request_get_txn_handle(proto_wrapper_t *wrapper);

/*
 * txn_commit reply
 */
extern proto_wrapper_t *
proto_init_txn_commit_reply(ham_status_t status);

extern ham_bool_t
proto_has_txn_commit_reply(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_txn_commit_reply_get_status(proto_wrapper_t *wrapper);

/*
 * txn_abort request
 */
extern proto_wrapper_t *
proto_init_txn_abort_request(ham_u64_t txnhandle, ham_u32_t flags);

extern ham_bool_t
proto_has_txn_abort_request(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_txn_abort_request_get_flags(proto_wrapper_t *wrapper);

extern ham_u64_t
proto_txn_abort_request_get_txn_handle(proto_wrapper_t *wrapper);

/*
 * txn_abort reply
 */
extern proto_wrapper_t *
proto_init_txn_abort_reply(ham_status_t status);

extern ham_bool_t
proto_has_txn_abort_reply(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_txn_abort_reply_get_status(proto_wrapper_t *wrapper);

/*
 * db_close request
 */
extern proto_wrapper_t *
proto_init_db_close_request(ham_u64_t dbhandle, ham_u32_t flags);

extern ham_bool_t
proto_has_db_close_request(proto_wrapper_t *wrapper);

extern ham_u64_t
proto_db_close_request_get_db_handle(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_db_close_request_get_flags(proto_wrapper_t *wrapper);

/*
 * db_close reply
 */
extern proto_wrapper_t *
proto_init_db_close_reply(ham_u32_t status);

extern ham_bool_t
proto_has_db_close_reply(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_db_close_reply_get_status(proto_wrapper_t *wrapper);

/*
 * db_get_parameters request
 */
extern proto_wrapper_t *
proto_init_db_get_parameters_request(ham_u64_t dbhandle, ham_u32_t *names, 
                ham_u32_t names_size);

extern ham_bool_t
proto_has_db_get_parameters_request(proto_wrapper_t *wrapper);

extern ham_size_t
proto_db_get_parameters_request_get_names_size(proto_wrapper_t *wrapper);

extern ham_u32_t *
proto_db_get_parameters_request_get_names(proto_wrapper_t *wrapper);

extern ham_u64_t
proto_db_get_parameters_request_get_db_handle(proto_wrapper_t *wrapper);

/*
 * db_get_parameters reply
 */
extern proto_wrapper_t *
proto_init_db_get_parameters_reply(ham_status_t status);

extern ham_bool_t
proto_has_db_get_parameters_reply(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_db_get_parameters_reply_get_status(proto_wrapper_t *wrapper);

extern void
proto_db_get_parameters_reply_set_cachesize(proto_wrapper_t *wrapper,
                ham_u32_t cachesize);

extern ham_bool_t
proto_db_get_parameters_reply_has_cachesize(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_db_get_parameters_reply_get_cachesize(proto_wrapper_t *wrapper);

extern void
proto_db_get_parameters_reply_set_pagesize(proto_wrapper_t *wrapper,
                ham_u32_t pagesize);

extern ham_bool_t
proto_db_get_parameters_reply_has_pagesize(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_db_get_parameters_reply_get_pagesize(proto_wrapper_t *wrapper);

extern void
proto_db_get_parameters_reply_set_max_env_databases(proto_wrapper_t *wrapper,
                ham_u32_t med);

extern ham_bool_t
proto_db_get_parameters_reply_has_max_env_databases(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_db_get_parameters_reply_get_max_env_databases(proto_wrapper_t *wrapper);

extern void
proto_db_get_parameters_reply_set_flags(proto_wrapper_t *wrapper, 
                ham_u32_t flags);

extern ham_bool_t
proto_db_get_parameters_reply_has_flags(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_db_get_parameters_reply_get_flags(proto_wrapper_t *wrapper);

extern void
proto_db_get_parameters_reply_set_filemode(proto_wrapper_t *wrapper, 
                ham_u32_t filemode);

extern ham_bool_t
proto_db_get_parameters_reply_has_filemode(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_db_get_parameters_reply_get_filemode(proto_wrapper_t *wrapper);

extern void
proto_db_get_parameters_reply_set_filename(proto_wrapper_t *wrapper,
                const char *filename);

extern ham_bool_t
proto_db_get_parameters_reply_has_filename(proto_wrapper_t *wrapper);

extern const char *
proto_db_get_parameters_reply_get_filename(proto_wrapper_t *wrapper);

extern void
proto_db_get_parameters_reply_set_keysize(proto_wrapper_t *wrapper,
                ham_u32_t keysize);

extern ham_bool_t
proto_db_get_parameters_reply_has_keysize(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_db_get_parameters_reply_get_keysize(proto_wrapper_t *wrapper);

extern void
proto_db_get_parameters_reply_set_dbname(proto_wrapper_t *wrapper,
                ham_u32_t dbname);

extern ham_bool_t
proto_db_get_parameters_reply_has_dbname(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_db_get_parameters_reply_get_dbname(proto_wrapper_t *wrapper);

extern void
proto_db_get_parameters_reply_set_keys_per_page(proto_wrapper_t *wrapper,
                ham_u32_t kpp);

extern ham_bool_t
proto_db_get_parameters_reply_has_keys_per_page(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_db_get_parameters_reply_get_keys_per_page(proto_wrapper_t *wrapper);

extern void
proto_db_get_parameters_reply_set_dam(proto_wrapper_t *wrapper,
                ham_u32_t dam);

extern ham_bool_t
proto_db_get_parameters_reply_has_dam(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_db_get_parameters_reply_get_dam(proto_wrapper_t *wrapper);

/*
 * check_integrity request
 */
extern proto_wrapper_t *
proto_init_check_integrity_request(ham_u64_t dbhandle, ham_u64_t txnhandle);

extern ham_bool_t
proto_has_check_integrity_request(proto_wrapper_t *wrapper);

extern ham_u64_t
proto_check_integrity_request_get_db_handle(proto_wrapper_t *wrapper);

extern ham_u64_t
proto_check_integrity_request_get_txn_handle(proto_wrapper_t *wrapper);

/*
 * check_integrity reply
 */
extern proto_wrapper_t *
proto_init_check_integrity_reply(ham_status_t status);

extern ham_bool_t
proto_has_check_integrity_reply(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_check_integrity_reply_get_status(proto_wrapper_t *wrapper);

/*
 * db_get_key_count request
 */
extern proto_wrapper_t *
proto_init_db_get_key_count_request(ham_u64_t dbhandle, 
                ham_u64_t txnhandle, ham_u32_t flags);

extern ham_bool_t
proto_has_db_get_key_count_request(proto_wrapper_t *wrapper);

extern ham_u64_t
proto_db_get_key_count_request_get_db_handle(proto_wrapper_t *wrapper);

extern ham_u64_t
proto_db_get_key_count_request_get_txn_handle(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_db_get_key_count_request_get_flags(proto_wrapper_t *wrapper);

/*
 * db_get_key_count reply
 */
extern proto_wrapper_t *
proto_init_db_get_key_count_reply(ham_status_t status, ham_u64_t keycount);

extern ham_bool_t
proto_has_db_get_key_count_reply(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_db_get_key_count_reply_get_status(proto_wrapper_t *wrapper);

extern ham_u64_t
proto_db_get_key_count_reply_get_key_count(proto_wrapper_t *wrapper);

/*
 * db_insert request
 */
extern proto_wrapper_t *
proto_init_db_insert_request(ham_u64_t dbhandle, ham_u64_t txnhandle, 
                ham_key_t *key, ham_record_t *record, ham_u32_t flags);

extern ham_bool_t
proto_has_db_insert_request(proto_wrapper_t *wrapper);

extern ham_u64_t
proto_db_insert_request_get_db_handle(proto_wrapper_t *wrapper);

extern ham_u64_t
proto_db_insert_request_get_txn_handle(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_db_insert_request_get_flags(proto_wrapper_t *wrapper);

extern ham_bool_t
proto_db_insert_request_has_key(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_db_insert_request_get_key_flags(proto_wrapper_t *wrapper);

extern void *
proto_db_insert_request_get_key_data(proto_wrapper_t *wrapper);

extern ham_size_t
proto_db_insert_request_get_key_size(proto_wrapper_t *wrapper);

extern ham_bool_t
proto_db_insert_request_has_record(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_db_insert_request_get_record_flags(proto_wrapper_t *wrapper);

extern void *
proto_db_insert_request_get_record_data(proto_wrapper_t *wrapper);

extern ham_size_t
proto_db_insert_request_get_record_size(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_db_insert_request_get_record_partial_offset(proto_wrapper_t *wrapper);

extern ham_size_t
proto_db_insert_request_get_record_partial_size(proto_wrapper_t *wrapper);

/*
 * db_insert reply
 */
extern proto_wrapper_t *
proto_init_db_insert_reply(ham_status_t status, ham_key_t *key);

extern ham_bool_t
proto_has_db_insert_reply(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_db_insert_reply_get_status(proto_wrapper_t *wrapper);

extern ham_bool_t
proto_db_insert_reply_has_key(proto_wrapper_t *wrapper);

extern void *
proto_db_insert_reply_get_key_data(proto_wrapper_t *wrapper);

extern ham_size_t
proto_db_insert_reply_get_key_size(proto_wrapper_t *wrapper);

/*
 * db_find request
 */
extern proto_wrapper_t *
proto_init_db_find_request(ham_u64_t dbhandle, ham_u64_t txnhandle, 
                ham_key_t *key, ham_record_t *record, ham_u32_t flags);

extern ham_bool_t
proto_has_db_find_request(proto_wrapper_t *wrapper);

extern ham_u64_t
proto_db_find_request_get_db_handle(proto_wrapper_t *wrapper);

extern ham_u64_t
proto_db_find_request_get_txn_handle(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_db_find_request_get_flags(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_db_find_request_get_key_flags(proto_wrapper_t *wrapper);

extern void *
proto_db_find_request_get_key_data(proto_wrapper_t *wrapper);

extern ham_size_t
proto_db_find_request_get_key_size(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_db_find_request_get_record_flags(proto_wrapper_t *wrapper);

extern void *
proto_db_find_request_get_record_data(proto_wrapper_t *wrapper);

extern ham_size_t
proto_db_find_request_get_record_size(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_db_find_request_get_record_partial_offset(proto_wrapper_t *wrapper);

extern ham_size_t
proto_db_find_request_get_record_partial_size(proto_wrapper_t *wrapper);

/*
 * db_find reply
 */
extern proto_wrapper_t *
proto_init_db_find_reply(ham_status_t status, ham_key_t *key, 
                ham_record_t *record);

extern ham_bool_t
proto_has_db_find_reply(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_db_find_reply_get_status(proto_wrapper_t *wrapper);

extern ham_bool_t
proto_db_find_reply_has_key(proto_wrapper_t *wrapper);

extern void *
proto_db_find_reply_get_key_data(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_db_find_reply_get_key_intflags(proto_wrapper_t *wrapper);

extern ham_size_t
proto_db_find_reply_get_key_size(proto_wrapper_t *wrapper);

extern ham_bool_t
proto_db_find_reply_has_record(proto_wrapper_t *wrapper);

extern void *
proto_db_find_reply_get_record_data(proto_wrapper_t *wrapper);

extern ham_size_t
proto_db_find_reply_get_record_size(proto_wrapper_t *wrapper);

/*
 * db_erase request
 */
extern proto_wrapper_t *
proto_init_db_erase_request(ham_u64_t dbhandle, ham_u64_t txnhandle, 
                ham_key_t *key, ham_u32_t flags);

extern ham_bool_t
proto_has_db_erase_request(proto_wrapper_t *wrapper);

extern ham_u64_t
proto_db_erase_request_get_db_handle(proto_wrapper_t *wrapper);

extern ham_u64_t
proto_db_erase_request_get_txn_handle(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_db_erase_request_get_flags(proto_wrapper_t *wrapper);

extern void *
proto_db_erase_request_get_key_data(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_db_erase_request_get_key_flags(proto_wrapper_t *wrapper);

extern ham_size_t
proto_db_erase_request_get_key_size(proto_wrapper_t *wrapper);

/*
 * db_erase reply
 */
extern proto_wrapper_t *
proto_init_db_erase_reply(ham_status_t status);

extern ham_bool_t
proto_has_db_erase_reply(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_db_erase_reply_get_status(proto_wrapper_t *wrapper);

/*
 * cursor_create request
 */
extern proto_wrapper_t *
proto_init_cursor_create_request(ham_u64_t dbhandle, ham_u64_t txnhandle, 
                ham_u32_t flags);

extern ham_bool_t
proto_has_cursor_create_request(proto_wrapper_t *wrapper);

extern ham_u64_t
proto_cursor_create_request_get_db_handle(proto_wrapper_t *wrapper);

extern ham_u64_t
proto_cursor_create_request_get_txn_handle(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_cursor_create_request_get_flags(proto_wrapper_t *wrapper);

/*
 * cursor_create reply
 */
extern proto_wrapper_t *
proto_init_cursor_create_reply(ham_status_t status, ham_u64_t handle);

extern ham_bool_t
proto_has_cursor_create_reply(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_cursor_create_reply_get_status(proto_wrapper_t *wrapper);

extern ham_u64_t
proto_cursor_create_reply_get_cursor_handle(proto_wrapper_t *wrapper);

/*
 * cursor_clone request
 */
extern proto_wrapper_t *
proto_init_cursor_clone_request(ham_u64_t cursorhandle);

extern ham_bool_t
proto_has_cursor_clone_request(proto_wrapper_t *wrapper);

extern ham_u64_t
proto_cursor_clone_request_get_cursor_handle(proto_wrapper_t *wrapper);

/*
 * cursor_clone reply
 */
extern proto_wrapper_t *
proto_init_cursor_clone_reply(ham_status_t status, ham_u64_t cursorhandle);

extern ham_bool_t
proto_has_cursor_clone_reply(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_cursor_clone_reply_get_status(proto_wrapper_t *wrapper);

extern ham_u64_t
proto_cursor_clone_reply_get_cursor_handle(proto_wrapper_t *wrapper);

/*
 * cursor_close request
 */
extern proto_wrapper_t *
proto_init_cursor_close_request(ham_u64_t cursorhandle);

extern ham_bool_t
proto_has_cursor_close_request(proto_wrapper_t *wrapper);

extern ham_u64_t
proto_cursor_close_request_get_cursor_handle(proto_wrapper_t *wrapper);

/*
 * cursor_close reply
 */
extern proto_wrapper_t *
proto_init_cursor_close_reply(ham_status_t status);

extern ham_bool_t
proto_has_cursor_close_reply(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_cursor_close_reply_get_status(proto_wrapper_t *wrapper);

/*
 * cursor_insert request
 */
extern proto_wrapper_t *
proto_init_cursor_insert_request(ham_u64_t cursorhandle, ham_key_t *key,
        ham_record_t *record, ham_u32_t flags);

extern ham_bool_t
proto_has_cursor_insert_request(proto_wrapper_t *wrapper);

extern ham_u64_t
proto_cursor_insert_request_get_cursor_handle(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_cursor_insert_request_get_flags(proto_wrapper_t *wrapper);

extern ham_bool_t
proto_cursor_insert_request_has_key(proto_wrapper_t *wrapper);

extern void *
proto_cursor_insert_request_get_key_data(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_cursor_insert_request_get_key_flags(proto_wrapper_t *wrapper);

extern ham_size_t
proto_cursor_insert_request_get_key_size(proto_wrapper_t *wrapper);

extern ham_bool_t
proto_cursor_insert_request_has_record(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_cursor_insert_request_get_record_flags(proto_wrapper_t *wrapper);

extern void *
proto_cursor_insert_request_get_record_data(proto_wrapper_t *wrapper);

extern ham_size_t
proto_cursor_insert_request_get_record_size(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_cursor_insert_request_get_record_partial_offset(proto_wrapper_t *wrapper);

extern ham_size_t
proto_cursor_insert_request_get_record_partial_size(proto_wrapper_t *wrapper);

/*
 * cursor_insert reply
 */
extern proto_wrapper_t *
proto_init_cursor_insert_reply(ham_status_t status, ham_key_t *key);

extern ham_bool_t
proto_has_cursor_insert_reply(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_cursor_insert_reply_get_status(proto_wrapper_t *wrapper);

extern ham_bool_t
proto_cursor_insert_reply_has_key(proto_wrapper_t *wrapper);

extern void *
proto_cursor_insert_reply_get_key_data(proto_wrapper_t *wrapper);

extern ham_size_t
proto_cursor_insert_reply_get_key_size(proto_wrapper_t *wrapper);

/*
 * cursor_erase request
 */
extern proto_wrapper_t *
proto_init_cursor_erase_request(ham_u64_t cursorhandle, ham_u32_t flags);

extern ham_bool_t
proto_has_cursor_erase_request(proto_wrapper_t *wrapper);

extern ham_u64_t
proto_cursor_erase_request_get_cursor_handle(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_cursor_erase_request_get_flags(proto_wrapper_t *wrapper);

/*
 * cursor_erase reply
 */
extern proto_wrapper_t *
proto_init_cursor_erase_reply(ham_status_t status);

extern ham_bool_t
proto_has_cursor_erase_reply(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_cursor_erase_reply_get_status(proto_wrapper_t *wrapper);

/*
 * cursor_find request
 */
extern proto_wrapper_t *
proto_init_cursor_find_request(ham_u64_t cursorhandle, ham_key_t *key,
        ham_record_t *record, ham_u32_t flags);

extern ham_bool_t
proto_has_cursor_find_request(proto_wrapper_t *wrapper);

extern ham_u64_t
proto_cursor_find_request_get_cursor_handle(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_cursor_find_request_get_flags(proto_wrapper_t *wrapper);

extern void *
proto_cursor_find_request_get_key_data(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_cursor_find_request_get_key_flags(proto_wrapper_t *wrapper);

extern ham_size_t
proto_cursor_find_request_get_key_size(proto_wrapper_t *wrapper);

extern ham_bool_t
proto_cursor_find_request_has_record(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_cursor_find_request_get_record_flags(proto_wrapper_t *wrapper);

extern void *
proto_cursor_find_request_get_record_data(proto_wrapper_t *wrapper);

extern ham_size_t
proto_cursor_find_request_get_record_size(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_cursor_find_request_get_record_partial_offset(proto_wrapper_t *wrapper);

extern ham_size_t
proto_cursor_find_request_get_record_partial_size(proto_wrapper_t *wrapper);

/*
 * cursor_find reply
 */
extern proto_wrapper_t *
proto_init_cursor_find_reply(ham_status_t status, ham_key_t *key,
        ham_record_t *record);

extern ham_bool_t
proto_has_cursor_find_reply(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_cursor_find_reply_get_status(proto_wrapper_t *wrapper);

extern ham_bool_t
proto_cursor_find_reply_has_key(proto_wrapper_t *wrapper);

extern void *
proto_cursor_find_reply_get_key_data(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_cursor_find_reply_get_key_intflags(proto_wrapper_t *wrapper);

extern ham_size_t
proto_cursor_find_reply_get_key_size(proto_wrapper_t *wrapper);

extern ham_bool_t
proto_cursor_find_reply_has_record(proto_wrapper_t *wrapper);

extern void *
proto_cursor_find_reply_get_record_data(proto_wrapper_t *wrapper);

extern ham_size_t
proto_cursor_find_reply_get_record_size(proto_wrapper_t *wrapper);

/*
 * cursor_get_duplicate_count request
 */
extern proto_wrapper_t *
proto_init_cursor_get_duplicate_count_request(ham_u64_t cursorhandle, 
                ham_u32_t flags);

extern ham_bool_t
proto_has_cursor_get_duplicate_count_request(proto_wrapper_t *wrapper);

extern ham_u64_t
proto_cursor_get_duplicate_count_request_get_cursor_handle(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_cursor_get_duplicate_count_request_get_flags(proto_wrapper_t *wrapper);

/*
 * cursor_get_duplicate_count reply
 */
extern proto_wrapper_t *
proto_init_cursor_get_duplicate_count_reply(ham_status_t status,
                ham_u32_t count);

extern ham_bool_t
proto_has_cursor_get_duplicate_count_reply(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_cursor_get_duplicate_count_reply_get_status(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_cursor_get_duplicate_count_reply_get_count(proto_wrapper_t *wrapper);

/*
 * cursor_overwrite request
 */
extern proto_wrapper_t *
proto_init_cursor_overwrite_request(ham_u64_t cursorhandle, 
                ham_record_t *record, ham_u32_t flags);

extern ham_bool_t
proto_has_cursor_overwrite_request(proto_wrapper_t *wrapper);

extern ham_u64_t
proto_cursor_overwrite_request_get_cursor_handle(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_cursor_overwrite_request_get_flags(proto_wrapper_t *wrapper);

extern void *
proto_cursor_overwrite_request_get_record_data(proto_wrapper_t *wrapper);

extern ham_size_t
proto_cursor_overwrite_request_get_record_size(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_cursor_overwrite_request_get_record_flags(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_cursor_overwrite_request_get_record_partial_offset(proto_wrapper_t *wrapper);

extern ham_size_t
proto_cursor_overwrite_request_get_record_partial_size(proto_wrapper_t *wrapper);

/*
 * cursor_overwrite reply
 */
extern proto_wrapper_t *
proto_init_cursor_overwrite_reply(ham_status_t status);

extern ham_bool_t
proto_has_cursor_overwrite_reply(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_cursor_overwrite_reply_get_status(proto_wrapper_t *wrapper);

/*
 * cursor_move request
 */
extern proto_wrapper_t *
proto_init_cursor_move_request(ham_u64_t cursorhandle, ham_key_t *key,
        ham_record_t *record, ham_u32_t flags);

extern ham_bool_t
proto_has_cursor_move_request(proto_wrapper_t *wrapper);

extern ham_u64_t
proto_cursor_move_request_get_cursor_handle(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_cursor_move_request_get_flags(proto_wrapper_t *wrapper);

extern ham_bool_t
proto_cursor_move_request_has_key(proto_wrapper_t *wrapper);

extern void *
proto_cursor_move_request_get_key_data(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_cursor_move_request_get_key_flags(proto_wrapper_t *wrapper);

extern ham_size_t
proto_cursor_move_request_get_key_size(proto_wrapper_t *wrapper);

extern ham_bool_t
proto_cursor_move_request_has_record(proto_wrapper_t *wrapper);

extern void *
proto_cursor_move_request_get_record_data(proto_wrapper_t *wrapper);

extern ham_size_t
proto_cursor_move_request_get_record_size(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_cursor_move_request_get_record_flags(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_cursor_move_request_get_record_partial_offset(proto_wrapper_t *wrapper);

extern ham_size_t
proto_cursor_move_request_get_record_partial_size(proto_wrapper_t *wrapper);

/*
 * cursor_move reply
 */
extern proto_wrapper_t *
proto_init_cursor_move_reply(ham_status_t status, ham_key_t *key,
        ham_record_t *record);

extern ham_bool_t
proto_has_cursor_move_reply(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_cursor_move_reply_get_status(proto_wrapper_t *wrapper);

extern ham_bool_t
proto_cursor_move_reply_has_key(proto_wrapper_t *wrapper);

extern void *
proto_cursor_move_reply_get_key_data(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_cursor_move_reply_get_key_intflags(proto_wrapper_t *wrapper);

extern ham_size_t
proto_cursor_move_reply_get_key_size(proto_wrapper_t *wrapper);

extern ham_bool_t
proto_cursor_move_reply_has_record(proto_wrapper_t *wrapper);

extern void *
proto_cursor_move_reply_get_record_data(proto_wrapper_t *wrapper);

extern ham_size_t
proto_cursor_move_reply_get_record_size(proto_wrapper_t *wrapper);

#ifdef __cplusplus
} // extern "C"
#endif

#endif /* HAM_PROTOCOL_H__ */
