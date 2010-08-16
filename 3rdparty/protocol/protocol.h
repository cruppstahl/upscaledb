
typedef int proto_bool_t;
typedef unsigned proto_size_t;
typedef unsigned char proto_data_t;

typedef struct proto_wrapper_t
{
    proto_data_t *buf;
    proto_size_t size;
} proto_wrapper_t;

extern proto_wrapper_t *
proto_unpack(proto_size_t size, const proto_data_t *buf);

extern void
proto_free_unpacked(proto_wrapper_t *wrapper);

/* TODO also writes the magic + size at wrapper->buf[0]! */
extern proto_data_t *
proto_get_packed_data(proto_wrapper_t *wrapper);
    //*(ham_u32_t *)&rbuf.packed_data[0]=ham_h2db32(HAM_TRANSFER_MAGIC_V1);
    //*(ham_u32_t *)&rbuf.packed_data[4]=ham_h2db32(payload_size);

extern proto_size_t
proto_get_packed_size(proto_wrapper_t *wrapper);

/*
 * connect request
 */

extern void
proto_init_connect_request(proto_wrapper_t *wrapper, 
        const char *filename);

extern proto_bool_t
proto_has_connect_request(proto_wrapper_t *wrapper);

/*
 * connect reply
 */

extern proto_bool_t
proto_has_connect_reply(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_connect_reply_get_status(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_connect_reply_get_env_flags(proto_wrapper_t *wrapper);

/*
 * env_rename request
 */
extern void
proto_init_env_rename_request(proto_wrapper_t *wrapper, 
        ham_u16_t oldname, ham_u16_t newname, ham_u32_t flags);

extern proto_bool_t
proto_has_env_rename_request(proto_wrapper_t *wrapper);

/*
 * env_rename reply
 */

extern proto_bool_t
proto_has_env_rename_reply(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_env_rename_reply_get_status(proto_wrapper_t *wrapper);

/*
 * env_erase_db request
 */
extern void
proto_init_env_erase_db_request(proto_wrapper_t *wrapper, 
        ham_u16_t name, ham_u32_t flags);

extern proto_bool_t
proto_has_env_erase_db_request(proto_wrapper_t *wrapper);

/*
 * env_erase_db reply
 */

extern proto_bool_t
proto_has_env_erase_db_reply(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_env_erase_db_reply_get_status(proto_wrapper_t *wrapper);

/*
 * env_get_database_names request
 */
extern void
proto_init_env_get_database_names_request(proto_wrapper_t *wrapper);

extern proto_bool_t
proto_has_env_get_database_names_request(proto_wrapper_t *wrapper);

/*
 * env_get_database_names reply
 */

extern proto_bool_t
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
extern void
proto_init_env_get_parameters_request(proto_wrapper_t *wrapper,
        ham_u32_t *names, ham_u32_t names_size);

extern proto_bool_t
proto_has_env_get_parameters_request(proto_wrapper_t *wrapper);

/*
 * env_get_parameters reply
 */

extern proto_bool_t
proto_has_env_get_parameters_reply(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_env_get_parameters_reply_get_status(proto_wrapper_t *wrapper);

extern proto_bool_t
proto_env_get_parameters_reply_has_cachesize(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_env_get_parameters_reply_get_cachesize(proto_wrapper_t *wrapper);

extern proto_bool_t
proto_env_get_parameters_reply_has_pagesize(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_env_get_parameters_reply_get_pagesize(proto_wrapper_t *wrapper);

extern proto_bool_t
proto_env_get_parameters_reply_has_max_env_databases(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_env_get_parameters_reply_get_max_env_databases(proto_wrapper_t *wrapper);

extern proto_bool_t
proto_env_get_parameters_reply_has_flags(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_env_get_parameters_reply_get_flags(proto_wrapper_t *wrapper);

extern proto_bool_t
proto_env_get_parameters_reply_has_filemode(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_env_get_parameters_reply_get_filemode(proto_wrapper_t *wrapper);

extern proto_bool_t
proto_env_get_parameters_reply_has_filename(proto_wrapper_t *wrapper);

extern const char *
proto_env_get_parameters_reply_get_filename(proto_wrapper_t *wrapper);

/*
 * env_flush request
 */
extern void
proto_init_env_flush_request(proto_wrapper_t *wrapper, ham_u32_t flags);

extern proto_bool_t
proto_has_env_flush_request(proto_wrapper_t *wrapper);

/*
 * env_flush reply
 */

extern proto_bool_t
proto_has_env_flush_reply(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_env_flush_reply_get_status(proto_wrapper_t *wrapper);

/*
 * env_create_db request
 */
extern void
proto_init_env_create_db_request(proto_wrapper_t *wrapper, ham_u16_t dbname,
            ham_u32_t flags, ham_u32_t *names, ham_u64_t *values,
            ham_u32_t num_params);

extern proto_bool_t
proto_has_env_create_db_request(proto_wrapper_t *wrapper);

/*
 * env_create_db reply
 */
extern proto_bool_t
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
extern void
proto_init_env_open_db_request(proto_wrapper_t *wrapper, ham_u16_t dbname,
            ham_u32_t flags, ham_u32_t *names, ham_u64_t *values,
            ham_u32_t num_params);

extern proto_bool_t
proto_has_env_open_db_request(proto_wrapper_t *wrapper);

/*
 * env_open_db reply
 */
extern proto_bool_t
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
extern void
proto_init_txn_begin_request(proto_wrapper_t *wrapper, ham_u64_t dbhandle,
            ham_u32_t flags);

extern proto_bool_t
proto_has_txn_begin_request(proto_wrapper_t *wrapper);

/*
 * txn_begin reply
 */
extern proto_bool_t
proto_has_txn_begin_reply(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_txn_begin_reply_get_status(proto_wrapper_t *wrapper);

extern ham_u64_t
proto_txn_begin_reply_get_txn_handle(proto_wrapper_t *wrapper);

/*
 * txn_commit request
 */
extern void
proto_init_txn_commit_request(proto_wrapper_t *wrapper, ham_u64_t txnhandle,
            ham_u32_t flags);

extern proto_bool_t
proto_has_txn_commit_request(proto_wrapper_t *wrapper);

/*
 * txn_commit reply
 */
extern proto_bool_t
proto_has_txn_commit_reply(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_txn_commit_reply_get_status(proto_wrapper_t *wrapper);

/*
 * txn_abort request
 */
extern void
proto_init_txn_abort_request(proto_wrapper_t *wrapper, ham_u64_t txnhandle,
            ham_u32_t flags);

extern proto_bool_t
proto_has_txn_abort_request(proto_wrapper_t *wrapper);

/*
 * txn_abort reply
 */
extern proto_bool_t
proto_has_txn_abort_reply(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_txn_abort_reply_get_status(proto_wrapper_t *wrapper);

/*
 * db_close request
 */
extern void
proto_init_db_close_request(proto_wrapper_t *wrapper, ham_u64_t dbhandle,
            ham_u32_t flags);

extern proto_bool_t
proto_has_db_close_request(proto_wrapper_t *wrapper);

/*
 * db_close reply
 */
extern proto_bool_t
proto_has_db_close_reply(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_db_close_reply_get_status(proto_wrapper_t *wrapper);

/*
 * db_get_parameters request
 */
extern void
proto_init_db_get_parameters_request(proto_wrapper_t *wrapper,
        ham_u64_t dbhandle, ham_u32_t *names, ham_u32_t names_size);

extern proto_bool_t
proto_has_db_get_parameters_request(proto_wrapper_t *wrapper);

/*
 * db_get_parameters reply
 */

extern proto_bool_t
proto_has_db_get_parameters_reply(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_db_get_parameters_reply_get_status(proto_wrapper_t *wrapper);

extern proto_bool_t
proto_db_get_parameters_reply_has_cachesize(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_db_get_parameters_reply_get_cachesize(proto_wrapper_t *wrapper);

extern proto_bool_t
proto_db_get_parameters_reply_has_pagesize(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_db_get_parameters_reply_get_pagesize(proto_wrapper_t *wrapper);

extern proto_bool_t
proto_db_get_parameters_reply_has_max_env_databases(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_db_get_parameters_reply_get_max_env_databases(proto_wrapper_t *wrapper);

extern proto_bool_t
proto_db_get_parameters_reply_has_flags(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_db_get_parameters_reply_get_flags(proto_wrapper_t *wrapper);

extern proto_bool_t
proto_db_get_parameters_reply_has_filemode(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_db_get_parameters_reply_get_filemode(proto_wrapper_t *wrapper);

extern proto_bool_t
proto_db_get_parameters_reply_has_filename(proto_wrapper_t *wrapper);

extern const char *
proto_db_get_parameters_reply_get_filename(proto_wrapper_t *wrapper);

extern proto_bool_t
proto_db_get_parameters_reply_has_keysize(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_db_get_parameters_reply_get_keysize(proto_wrapper_t *wrapper);

extern proto_bool_t
proto_db_get_parameters_reply_has_dbname(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_db_get_parameters_reply_get_dbname(proto_wrapper_t *wrapper);

extern proto_bool_t
proto_db_get_parameters_reply_has_keys_per_page(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_db_get_parameters_reply_get_keys_per_page(proto_wrapper_t *wrapper);

extern proto_bool_t
proto_db_get_parameters_reply_has_dam(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_db_get_parameters_reply_get_dam(proto_wrapper_t *wrapper);

/*
 * db_flush request
 */
extern void
proto_init_db_flush_request(proto_wrapper_t *wrapper,
        ham_u64_t dbhandle, ham_u32_t flags);

extern proto_bool_t
proto_has_db_flush_request(proto_wrapper_t *wrapper);

/*
 * db_flush reply
 */

extern proto_bool_t
proto_has_db_flush_reply(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_db_flush_reply_get_status(proto_wrapper_t *wrapper);

/*
 * check_integrity request
 */
extern void
proto_init_check_integrity_request(proto_wrapper_t *wrapper,
        ham_u64_t dbhandle, ham_u64_t txnhandle);

extern proto_bool_t
proto_has_check_integrity_request(proto_wrapper_t *wrapper);

/*
 * check_integrity reply
 */
extern proto_bool_t
proto_has_check_integrity_reply(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_check_integrity_reply_get_status(proto_wrapper_t *wrapper);

/*
 * db_get_key_count request
 */
extern void
proto_init_db_get_key_count_request(proto_wrapper_t *wrapper,
        ham_u64_t dbhandle, ham_u64_t txnhandle, ham_u32_t flags);

extern proto_bool_t
proto_has_db_get_key_count_request(proto_wrapper_t *wrapper);

/*
 * db_get_key_count reply
 */
extern proto_bool_t
proto_has_db_get_key_count_reply(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_db_get_key_count_reply_get_status(proto_wrapper_t *wrapper);

extern ham_u64_t
proto_db_get_key_count_reply_get_key_count(proto_wrapper_t *wrapper);

/*
 * db_insert request
 */
extern void
proto_init_db_insert_request(proto_wrapper_t *wrapper,
        ham_u64_t dbhandle, ham_u64_t txnhandle, ham_key_t *key,
        ham_record_t *record, ham_u32_t flags);

extern proto_bool_t
proto_has_db_insert_request(proto_wrapper_t *wrapper);

/*
 * db_insert reply
 */
extern proto_bool_t
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
extern void
proto_init_db_find_request(proto_wrapper_t *wrapper,
        ham_u64_t dbhandle, ham_u64_t txnhandle, ham_key_t *key,
        ham_record_t *record, ham_u32_t flags);

extern proto_bool_t
proto_has_db_find_request(proto_wrapper_t *wrapper);

/*
 * db_find reply
 */
extern proto_bool_t
proto_has_db_find_reply(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_db_find_reply_get_status(proto_wrapper_t *wrapper);

extern proto_bool_t
proto_db_find_reply_has_key(proto_wrapper_t *wrapper);

extern void *
proto_db_find_reply_get_key_data(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_db_find_reply_get_key_intflags(proto_wrapper_t *wrapper);

extern ham_size_t
proto_db_find_reply_get_key_size(proto_wrapper_t *wrapper);

extern proto_bool_t
proto_db_find_reply_has_record(proto_wrapper_t *wrapper);

extern void *
proto_db_find_reply_get_record_data(proto_wrapper_t *wrapper);

extern ham_size_t
proto_db_find_reply_get_record_size(proto_wrapper_t *wrapper);

/*
 * db_erase request
 */
extern void
proto_init_db_erase_request(proto_wrapper_t *wrapper,
        ham_u64_t dbhandle, ham_u64_t txnhandle, ham_key_t *key,
        ham_u32_t flags);

extern proto_bool_t
proto_has_db_erase_request(proto_wrapper_t *wrapper);

/*
 * db_erase reply
 */
extern proto_bool_t
proto_has_db_erase_reply(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_db_erase_reply_get_status(proto_wrapper_t *wrapper);

