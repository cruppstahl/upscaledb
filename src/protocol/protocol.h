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
 * connect request
 */
extern proto_wrapper_t *
proto_init_connect_request(const char *filename);

extern ham_bool_t
proto_has_connect_request(proto_wrapper_t *wrapper);

/*
 * connect reply
 */

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

extern ham_bool_t
proto_has_env_rename_request(proto_wrapper_t *wrapper);

/*
 * env_rename reply
 */

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

/*
 * env_erase_db reply
 */

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

extern ham_bool_t
proto_has_env_get_parameters_request(proto_wrapper_t *wrapper);

/*
 * env_get_parameters reply
 */

extern ham_bool_t
proto_has_env_get_parameters_reply(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_env_get_parameters_reply_get_status(proto_wrapper_t *wrapper);

extern ham_bool_t
proto_env_get_parameters_reply_has_cachesize(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_env_get_parameters_reply_get_cachesize(proto_wrapper_t *wrapper);

extern ham_bool_t
proto_env_get_parameters_reply_has_pagesize(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_env_get_parameters_reply_get_pagesize(proto_wrapper_t *wrapper);

extern ham_bool_t
proto_env_get_parameters_reply_has_max_env_databases(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_env_get_parameters_reply_get_max_env_databases(proto_wrapper_t *wrapper);

extern ham_bool_t
proto_env_get_parameters_reply_has_flags(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_env_get_parameters_reply_get_flags(proto_wrapper_t *wrapper);

extern ham_bool_t
proto_env_get_parameters_reply_has_filemode(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_env_get_parameters_reply_get_filemode(proto_wrapper_t *wrapper);

extern ham_bool_t
proto_env_get_parameters_reply_has_filename(proto_wrapper_t *wrapper);

extern const char *
proto_env_get_parameters_reply_get_filename(proto_wrapper_t *wrapper);

/*
 * env_flush request
 */
extern proto_wrapper_t *
proto_init_env_flush_request(ham_u32_t flags);

extern ham_bool_t
proto_has_env_flush_request(proto_wrapper_t *wrapper);

/*
 * env_flush reply
 */

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

/*
 * env_create_db reply
 */
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

/*
 * env_open_db reply
 */
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

/*
 * txn_begin reply
 */
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

/*
 * txn_commit reply
 */
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

/*
 * txn_abort reply
 */
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

/*
 * db_close reply
 */
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

/*
 * db_get_parameters reply
 */

extern ham_bool_t
proto_has_db_get_parameters_reply(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_db_get_parameters_reply_get_status(proto_wrapper_t *wrapper);

extern ham_bool_t
proto_db_get_parameters_reply_has_cachesize(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_db_get_parameters_reply_get_cachesize(proto_wrapper_t *wrapper);

extern ham_bool_t
proto_db_get_parameters_reply_has_pagesize(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_db_get_parameters_reply_get_pagesize(proto_wrapper_t *wrapper);

extern ham_bool_t
proto_db_get_parameters_reply_has_max_env_databases(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_db_get_parameters_reply_get_max_env_databases(proto_wrapper_t *wrapper);

extern ham_bool_t
proto_db_get_parameters_reply_has_flags(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_db_get_parameters_reply_get_flags(proto_wrapper_t *wrapper);

extern ham_bool_t
proto_db_get_parameters_reply_has_filemode(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_db_get_parameters_reply_get_filemode(proto_wrapper_t *wrapper);

extern ham_bool_t
proto_db_get_parameters_reply_has_filename(proto_wrapper_t *wrapper);

extern const char *
proto_db_get_parameters_reply_get_filename(proto_wrapper_t *wrapper);

extern ham_bool_t
proto_db_get_parameters_reply_has_keysize(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_db_get_parameters_reply_get_keysize(proto_wrapper_t *wrapper);

extern ham_bool_t
proto_db_get_parameters_reply_has_dbname(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_db_get_parameters_reply_get_dbname(proto_wrapper_t *wrapper);

extern ham_bool_t
proto_db_get_parameters_reply_has_keys_per_page(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_db_get_parameters_reply_get_keys_per_page(proto_wrapper_t *wrapper);

extern ham_bool_t
proto_db_get_parameters_reply_has_dam(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_db_get_parameters_reply_get_dam(proto_wrapper_t *wrapper);

/*
 * db_flush request
 */
extern proto_wrapper_t *
proto_init_db_flush_request(ham_u64_t dbhandle, ham_u32_t flags);

extern ham_bool_t
proto_has_db_flush_request(proto_wrapper_t *wrapper);

/*
 * db_flush reply
 */

extern ham_bool_t
proto_has_db_flush_reply(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_db_flush_reply_get_status(proto_wrapper_t *wrapper);

/*
 * check_integrity request
 */
extern proto_wrapper_t *
proto_init_check_integrity_request(ham_u64_t dbhandle, ham_u64_t txnhandle);

extern ham_bool_t
proto_has_check_integrity_request(proto_wrapper_t *wrapper);

/*
 * check_integrity reply
 */
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

/*
 * db_get_key_count reply
 */
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

/*
 * db_insert reply
 */
extern ham_bool_t
proto_has_db_insert_reply(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_db_insert_reply_get_status(proto_wrapper_t *wrapper);

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

/*
 * db_find reply
 */
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

/*
 * db_erase reply
 */
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

/*
 * cursor_create reply
 */
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

/*
 * cursor_clone reply
 */
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

/*
 * cursor_close reply
 */
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

/*
 * cursor_insert reply
 */
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

/*
 * cursor_erase reply
 */
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

/*
 * cursor_find reply
 */
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

/*
 * cursor_get_duplicate_count reply
 */
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

/*
 * cursor_overwrite reply
 */
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

/*
 * cursor_move reply
 */
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
