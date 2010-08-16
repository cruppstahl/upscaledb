
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
 * env_get_database_names reply
 */

extern proto_bool_t
proto_has_env_flush_reply(proto_wrapper_t *wrapper);

extern ham_u32_t
proto_env_flush_reply_get_status(proto_wrapper_t *wrapper);

