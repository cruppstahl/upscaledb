/*
 * Copyright (C) 2005-2012 Christoph Rupp (chris@crupp.de).
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
 */

#ifndef HAM_PROTOCOL_H__
#define HAM_PROTOCOL_H__

#include <ham/hamsterdb.h>
#include "../mem.h"
#include "messages.pb.h"

/**
 * the Protocol class maps a single message that is exchanged between
 * client and server
 */
class Protocol : public ham::ProtoWrapper
{
  public:
    /** helper function which copies a ham_key_t into a ProtoBuf key */
    static void assign_key(ham::Key *protokey, ham_key_t *hamkey) {
      protokey->set_data(hamkey->data, hamkey->size);
      protokey->set_flags(hamkey->flags);
      protokey->set_intflags(hamkey->_flags);
    }

    /** helper function which copies a ham_record_t into a ProtoBuf record */
    static void assign_record(ham::Record *protorec, ham_record_t *hamrec) {
      protorec->set_data(hamrec->data, hamrec->size);
      protorec->set_flags(hamrec->flags);
      protorec->set_partial_offset(hamrec->partial_offset);
      protorec->set_partial_size(hamrec->partial_size);
    }

    /**
     * Factory function; creates a new Protocol structure from a serialized
     * buffer
     */
    static Protocol *unpack(const ham_u8_t *buf, ham_size_t size);

    /* 
     * Packs the Protocol structure into a memory buffer and returns
     * a pointer to the buffer and the buffer size 
     */
    bool pack(Allocator *alloc, ham_u8_t **data, ham_size_t *size);
};

/**
 * shutdown/free globally allocated memory
 */
extern void
proto_shutdown(void);


/*
 * cursor_insert request
 */
extern Protocol *
proto_init_cursor_insert_request(ham_u64_t cursorhandle, ham_key_t *key,
        ham_record_t *record, ham_u32_t flags);

extern ham_bool_t
proto_has_cursor_insert_request(Protocol *wrapper);

extern ham_u64_t
proto_cursor_insert_request_get_cursor_handle(Protocol *wrapper);

extern ham_u32_t
proto_cursor_insert_request_get_flags(Protocol *wrapper);

extern ham_bool_t
proto_cursor_insert_request_has_key(Protocol *wrapper);

extern void *
proto_cursor_insert_request_get_key_data(Protocol *wrapper);

extern ham_u32_t
proto_cursor_insert_request_get_key_flags(Protocol *wrapper);

extern ham_size_t
proto_cursor_insert_request_get_key_size(Protocol *wrapper);

extern ham_bool_t
proto_cursor_insert_request_has_record(Protocol *wrapper);

extern ham_u32_t
proto_cursor_insert_request_get_record_flags(Protocol *wrapper);

extern void *
proto_cursor_insert_request_get_record_data(Protocol *wrapper);

extern ham_size_t
proto_cursor_insert_request_get_record_size(Protocol *wrapper);

extern ham_u32_t
proto_cursor_insert_request_get_record_partial_offset(Protocol *wrapper);

extern ham_size_t
proto_cursor_insert_request_get_record_partial_size(Protocol *wrapper);

/*
 * cursor_insert reply
 */
extern Protocol *
proto_init_cursor_insert_reply(ham_status_t status, ham_key_t *key);

extern ham_bool_t
proto_has_cursor_insert_reply(Protocol *wrapper);

extern ham_u32_t
proto_cursor_insert_reply_get_status(Protocol *wrapper);

extern ham_bool_t
proto_cursor_insert_reply_has_key(Protocol *wrapper);

extern void *
proto_cursor_insert_reply_get_key_data(Protocol *wrapper);

extern ham_size_t
proto_cursor_insert_reply_get_key_size(Protocol *wrapper);

/*
 * cursor_erase request
 */
extern Protocol *
proto_init_cursor_erase_request(ham_u64_t cursorhandle, ham_u32_t flags);

extern ham_bool_t
proto_has_cursor_erase_request(Protocol *wrapper);

extern ham_u64_t
proto_cursor_erase_request_get_cursor_handle(Protocol *wrapper);

extern ham_u32_t
proto_cursor_erase_request_get_flags(Protocol *wrapper);

/*
 * cursor_erase reply
 */
extern Protocol *
proto_init_cursor_erase_reply(ham_status_t status);

extern ham_bool_t
proto_has_cursor_erase_reply(Protocol *wrapper);

extern ham_u32_t
proto_cursor_erase_reply_get_status(Protocol *wrapper);

/*
 * cursor_find request
 */
extern Protocol *
proto_init_cursor_find_request(ham_u64_t cursorhandle, ham_key_t *key,
        ham_record_t *record, ham_u32_t flags);

extern ham_bool_t
proto_has_cursor_find_request(Protocol *wrapper);

extern ham_u64_t
proto_cursor_find_request_get_cursor_handle(Protocol *wrapper);

extern ham_u32_t
proto_cursor_find_request_get_flags(Protocol *wrapper);

extern void *
proto_cursor_find_request_get_key_data(Protocol *wrapper);

extern ham_u32_t
proto_cursor_find_request_get_key_flags(Protocol *wrapper);

extern ham_size_t
proto_cursor_find_request_get_key_size(Protocol *wrapper);

extern ham_bool_t
proto_cursor_find_request_has_record(Protocol *wrapper);

extern ham_u32_t
proto_cursor_find_request_get_record_flags(Protocol *wrapper);

extern void *
proto_cursor_find_request_get_record_data(Protocol *wrapper);

extern ham_size_t
proto_cursor_find_request_get_record_size(Protocol *wrapper);

extern ham_u32_t
proto_cursor_find_request_get_record_partial_offset(Protocol *wrapper);

extern ham_size_t
proto_cursor_find_request_get_record_partial_size(Protocol *wrapper);

/*
 * cursor_find reply
 */
extern Protocol *
proto_init_cursor_find_reply(ham_status_t status, ham_key_t *key,
        ham_record_t *record);

extern ham_bool_t
proto_has_cursor_find_reply(Protocol *wrapper);

extern ham_u32_t
proto_cursor_find_reply_get_status(Protocol *wrapper);

extern ham_bool_t
proto_cursor_find_reply_has_key(Protocol *wrapper);

extern void *
proto_cursor_find_reply_get_key_data(Protocol *wrapper);

extern ham_u32_t
proto_cursor_find_reply_get_key_intflags(Protocol *wrapper);

extern ham_size_t
proto_cursor_find_reply_get_key_size(Protocol *wrapper);

extern ham_bool_t
proto_cursor_find_reply_has_record(Protocol *wrapper);

extern void *
proto_cursor_find_reply_get_record_data(Protocol *wrapper);

extern ham_size_t
proto_cursor_find_reply_get_record_size(Protocol *wrapper);

/*
 * cursor_get_duplicate_count request
 */
extern Protocol *
proto_init_cursor_get_duplicate_count_request(ham_u64_t cursorhandle, 
                ham_u32_t flags);

extern ham_bool_t
proto_has_cursor_get_duplicate_count_request(Protocol *wrapper);

extern ham_u64_t
proto_cursor_get_duplicate_count_request_get_cursor_handle(Protocol *wrapper);

extern ham_u32_t
proto_cursor_get_duplicate_count_request_get_flags(Protocol *wrapper);

/*
 * cursor_get_duplicate_count reply
 */
extern Protocol *
proto_init_cursor_get_duplicate_count_reply(ham_status_t status,
                ham_u32_t count);

extern ham_bool_t
proto_has_cursor_get_duplicate_count_reply(Protocol *wrapper);

extern ham_u32_t
proto_cursor_get_duplicate_count_reply_get_status(Protocol *wrapper);

extern ham_u32_t
proto_cursor_get_duplicate_count_reply_get_count(Protocol *wrapper);

/*
 * cursor_overwrite request
 */
extern Protocol *
proto_init_cursor_overwrite_request(ham_u64_t cursorhandle, 
                ham_record_t *record, ham_u32_t flags);

extern ham_bool_t
proto_has_cursor_overwrite_request(Protocol *wrapper);

extern ham_u64_t
proto_cursor_overwrite_request_get_cursor_handle(Protocol *wrapper);

extern ham_u32_t
proto_cursor_overwrite_request_get_flags(Protocol *wrapper);

extern void *
proto_cursor_overwrite_request_get_record_data(Protocol *wrapper);

extern ham_size_t
proto_cursor_overwrite_request_get_record_size(Protocol *wrapper);

extern ham_u32_t
proto_cursor_overwrite_request_get_record_flags(Protocol *wrapper);

extern ham_u32_t
proto_cursor_overwrite_request_get_record_partial_offset(Protocol *wrapper);

extern ham_size_t
proto_cursor_overwrite_request_get_record_partial_size(Protocol *wrapper);

/*
 * cursor_overwrite reply
 */
extern Protocol *
proto_init_cursor_overwrite_reply(ham_status_t status);

extern ham_bool_t
proto_has_cursor_overwrite_reply(Protocol *wrapper);

extern ham_u32_t
proto_cursor_overwrite_reply_get_status(Protocol *wrapper);

/*
 * cursor_move request
 */
extern Protocol *
proto_init_cursor_move_request(ham_u64_t cursorhandle, ham_key_t *key,
        ham_record_t *record, ham_u32_t flags);

extern ham_bool_t
proto_has_cursor_move_request(Protocol *wrapper);

extern ham_u64_t
proto_cursor_move_request_get_cursor_handle(Protocol *wrapper);

extern ham_u32_t
proto_cursor_move_request_get_flags(Protocol *wrapper);

extern ham_bool_t
proto_cursor_move_request_has_key(Protocol *wrapper);

extern void *
proto_cursor_move_request_get_key_data(Protocol *wrapper);

extern ham_u32_t
proto_cursor_move_request_get_key_flags(Protocol *wrapper);

extern ham_size_t
proto_cursor_move_request_get_key_size(Protocol *wrapper);

extern ham_bool_t
proto_cursor_move_request_has_record(Protocol *wrapper);

extern void *
proto_cursor_move_request_get_record_data(Protocol *wrapper);

extern ham_size_t
proto_cursor_move_request_get_record_size(Protocol *wrapper);

extern ham_u32_t
proto_cursor_move_request_get_record_flags(Protocol *wrapper);

extern ham_u32_t
proto_cursor_move_request_get_record_partial_offset(Protocol *wrapper);

extern ham_size_t
proto_cursor_move_request_get_record_partial_size(Protocol *wrapper);

/*
 * cursor_move reply
 */
extern Protocol *
proto_init_cursor_move_reply(ham_status_t status, ham_key_t *key,
        ham_record_t *record);

extern ham_bool_t
proto_has_cursor_move_reply(Protocol *wrapper);

extern ham_u32_t
proto_cursor_move_reply_get_status(Protocol *wrapper);

extern ham_bool_t
proto_cursor_move_reply_has_key(Protocol *wrapper);

extern void *
proto_cursor_move_reply_get_key_data(Protocol *wrapper);

extern ham_u32_t
proto_cursor_move_reply_get_key_intflags(Protocol *wrapper);

extern ham_size_t
proto_cursor_move_reply_get_key_size(Protocol *wrapper);

extern ham_bool_t
proto_cursor_move_reply_has_record(Protocol *wrapper);

extern void *
proto_cursor_move_reply_get_record_data(Protocol *wrapper);

extern ham_size_t
proto_cursor_move_reply_get_record_size(Protocol *wrapper);


#endif /* HAM_PROTOCOL_H__ */
