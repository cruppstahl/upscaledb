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

#include "protocol.h"
#include "../error.h"
#include "../mem.h"
#include "../endianswap.h"
#include "../db.h"

#include "messages.pb.h"

using namespace ham;

Protocol *
Protocol::unpack(const ham_u8_t *buf, ham_size_t size)
{
  if (*(ham_u32_t *)&buf[0] != ham_db2h32(HAM_TRANSFER_MAGIC_V1)) {
    ham_trace(("invalid protocol version"));
    return (0);
  }

  Protocol *p = new Protocol;
  if (!p->ParseFromArray(buf + 8, size - 8)) {
    delete p;
    return (0);
  }
  return (p);
}

bool
Protocol::pack(Allocator *alloc, ham_u8_t **data, ham_size_t *size)
{
  ham_size_t packed_size = ByteSize();
  /* we need 8 more bytes for magic and size */
  ham_u8_t *p = (ham_u8_t *)alloc->alloc(packed_size + 8);
  if (!p)
    return (false);

  /* write the magic and the payload size of the packed structure */
  *(ham_u32_t *)&p[0] = ham_h2db32(HAM_TRANSFER_MAGIC_V1);
  *(ham_u32_t *)&p[4] = ham_h2db32(packed_size);

  /* now write the packed structure */
  if (!SerializeToArray(&p[8], packed_size)) {
    alloc->free(p);
    return (false);
  }
    
  *data = p;
  *size = packed_size + 8;
  return (true);
}

void
proto_shutdown(void)
{
    google::protobuf::ShutdownProtobufLibrary();
}





Protocol *
proto_init_cursor_insert_request(ham_u64_t cursorhandle, ham_key_t *key,
        ham_record_t *record, ham_u32_t flags)
{
    Protocol *w=new Protocol();
    w->set_type(Protocol::CURSOR_INSERT_REQUEST);
    w->mutable_cursor_insert_request()->set_cursor_handle(cursorhandle);
    w->mutable_cursor_insert_request()->set_flags(flags);
    if (key)
        Protocol::assign_key(w->mutable_cursor_insert_request()->mutable_key(), 
                key);
    if (record)
        Protocol::assign_record(w->mutable_cursor_insert_request()->mutable_record(), 
                record);
    return ((Protocol *)w);
}

ham_bool_t
proto_has_cursor_insert_request(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    if (w->type()==Protocol::CURSOR_INSERT_REQUEST) {
        ham_assert(w->has_cursor_insert_request()==true);
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_cursor_insert_request()==false);
        return (HAM_FALSE);
    }
}

ham_u64_t
proto_cursor_insert_request_get_cursor_handle(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    return (w->cursor_insert_request().cursor_handle());
}

ham_u32_t
proto_cursor_insert_request_get_flags(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    return (w->cursor_insert_request().flags());
}

ham_bool_t
proto_cursor_insert_request_has_key(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    return (w->cursor_insert_request().has_key());
}

ham_u32_t
proto_cursor_insert_request_get_key_flags(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    return (w->cursor_insert_request().key().flags());
}

void *
proto_cursor_insert_request_get_key_data(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    if (w->cursor_insert_request().key().has_data())
        return ((void *)&w->cursor_insert_request().key().data()[0]);
    else
        return (0);
}

ham_size_t
proto_cursor_insert_request_get_key_size(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    return ((ham_size_t)w->cursor_insert_request().key().data().size());
}

ham_bool_t
proto_cursor_insert_request_has_record(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    return (w->cursor_insert_request().has_record());
}

ham_u32_t
proto_cursor_insert_request_get_record_flags(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    return (w->cursor_insert_request().record().flags());
}

void *
proto_cursor_insert_request_get_record_data(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    if (w->cursor_insert_request().record().has_data())
        return ((void *)&w->cursor_insert_request().record().data()[0]);
    else
        return (0);
}

ham_size_t
proto_cursor_insert_request_get_record_size(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    return ((ham_size_t)w->cursor_insert_request().record().data().size());
}

ham_u32_t
proto_cursor_insert_request_get_record_partial_offset(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    return (w->cursor_insert_request().record().partial_offset());
}

ham_size_t
proto_cursor_insert_request_get_record_partial_size(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    return (w->cursor_insert_request().record().partial_size());
}

Protocol *
proto_init_cursor_insert_reply(ham_status_t status, ham_key_t *key)
{
    Protocol *w=new Protocol();
    w->set_type(Protocol::CURSOR_INSERT_REPLY);
    w->mutable_cursor_insert_reply()->set_status(status);
    if (key)
        Protocol::assign_key(w->mutable_cursor_insert_reply()->mutable_key(), key);
    return ((Protocol *)w);
}

ham_bool_t
proto_has_cursor_insert_reply(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    if (w->type()==Protocol::CURSOR_INSERT_REPLY) {
        ham_assert(w->has_cursor_insert_reply()==true);
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_cursor_insert_reply()==false);
        return (HAM_FALSE);
    }
}

ham_u32_t
proto_cursor_insert_reply_get_status(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    return (w->cursor_insert_reply().status());
}

ham_bool_t
proto_cursor_insert_reply_has_key(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    return (w->cursor_insert_reply().has_key());
}

void *
proto_cursor_insert_reply_get_key_data(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    if (w->cursor_insert_reply().key().has_data())
        return ((void *)&w->cursor_insert_reply().key().data()[0]);
    else
        return (0);
}

ham_size_t
proto_cursor_insert_reply_get_key_size(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    return ((ham_size_t)w->cursor_insert_reply().key().data().size());
}

Protocol *
proto_init_cursor_erase_request(ham_u64_t cursorhandle, ham_u32_t flags)
{
    Protocol *w=new Protocol();
    w->set_type(Protocol::CURSOR_ERASE_REQUEST);
    w->mutable_cursor_erase_request()->set_cursor_handle(cursorhandle);
    w->mutable_cursor_erase_request()->set_flags(flags);
    return ((Protocol *)w);
}

ham_bool_t
proto_has_cursor_erase_request(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    if (w->type()==Protocol::CURSOR_ERASE_REQUEST) {
        ham_assert(w->has_cursor_erase_request()==true);
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_cursor_erase_request()==false);
        return (HAM_FALSE);
    }
}

ham_u64_t
proto_cursor_erase_request_get_cursor_handle(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    return (w->cursor_erase_request().cursor_handle());
}

ham_u32_t
proto_cursor_erase_request_get_flags(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    return (w->cursor_erase_request().flags());
}

Protocol *
proto_init_cursor_erase_reply(ham_status_t status)
{
    Protocol *w=new Protocol();
    w->set_type(Protocol::CURSOR_ERASE_REPLY);
    w->mutable_cursor_erase_reply()->set_status(status);
    return ((Protocol *)w);
}

ham_bool_t
proto_has_cursor_erase_reply(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    if (w->type()==Protocol::CURSOR_ERASE_REPLY) {
        ham_assert(w->has_cursor_erase_reply()==true);
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_cursor_erase_reply()==false);
        return (HAM_FALSE);
    }
}

ham_u32_t
proto_cursor_erase_reply_get_status(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    return (w->cursor_erase_reply().status());
}

Protocol *
proto_init_cursor_find_request(ham_u64_t cursorhandle, ham_key_t *key,
        ham_record_t *record, ham_u32_t flags)
{
    Protocol *w=new Protocol();
    w->set_type(Protocol::CURSOR_FIND_REQUEST);
    w->mutable_cursor_find_request()->set_cursor_handle(cursorhandle);
    w->mutable_cursor_find_request()->set_flags(flags);
    if (key)
        Protocol::assign_key(w->mutable_cursor_find_request()->mutable_key(), 
                key);
    if (record)
        Protocol::assign_record(w->mutable_cursor_find_request()->mutable_record(), 
                record);
    return ((Protocol *)w);
}

ham_bool_t
proto_has_cursor_find_request(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    if (w->type()==Protocol::CURSOR_FIND_REQUEST) {
        ham_assert(w->has_cursor_find_request()==true);
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_cursor_find_request()==false);
        return (HAM_FALSE);
    }
}

ham_u64_t
proto_cursor_find_request_get_cursor_handle(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    return (w->cursor_find_request().cursor_handle());
}

ham_u32_t
proto_cursor_find_request_get_flags(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    return (w->cursor_find_request().flags());
}

void *
proto_cursor_find_request_get_key_data(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    if (w->cursor_find_request().key().has_data())
        return ((void *)&w->cursor_find_request().key().data()[0]);
    else
        return (0);
}

ham_size_t
proto_cursor_find_request_get_key_size(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    return ((ham_size_t)w->cursor_find_request().key().data().size());
}

ham_bool_t
proto_cursor_find_request_has_record(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    return (w->cursor_find_request().has_record());
}

ham_size_t
proto_cursor_find_request_get_key_flags(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    return (w->cursor_find_request().key().flags());
}

ham_u32_t
proto_cursor_find_request_get_record_flags(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    return (w->cursor_find_request().record().flags());
}

void *
proto_cursor_find_request_get_record_data(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    if (w->cursor_find_request().record().has_data())
        return ((void *)&w->cursor_find_request().record().data()[0]);
    else
        return (0);
}

ham_size_t
proto_cursor_find_request_get_record_size(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    return ((ham_size_t)w->cursor_find_request().record().data().size());
}

ham_u32_t
proto_cursor_find_request_get_record_partial_offset(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    return (w->cursor_find_request().record().partial_offset());
}

ham_size_t
proto_cursor_find_request_get_record_partial_size(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    return (w->cursor_find_request().record().partial_size());
}

Protocol *
proto_init_cursor_find_reply(ham_status_t status, ham_key_t *key,
        ham_record_t *record)
{
    Protocol *w=new Protocol();
    w->set_type(Protocol::CURSOR_FIND_REPLY);
    w->mutable_cursor_find_reply()->set_status(status);
    if (key)
        Protocol::assign_key(w->mutable_cursor_find_reply()->mutable_key(), key);
    if (record)
        Protocol::assign_record(w->mutable_cursor_find_reply()->mutable_record(), record);
    return ((Protocol *)w);
}

ham_bool_t
proto_has_cursor_find_reply(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    if (w->type()==Protocol::CURSOR_FIND_REPLY) {
        ham_assert(w->has_cursor_find_reply()==true);
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_cursor_find_reply()==false);
        return (HAM_FALSE);
    }
}

ham_u32_t
proto_cursor_find_reply_get_status(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    return (w->cursor_find_reply().status());
}

ham_bool_t
proto_cursor_find_reply_has_key(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    return (w->cursor_find_reply().has_key());
}

void *
proto_cursor_find_reply_get_key_data(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    if (w->cursor_find_reply().key().has_data())
        return ((void *)&w->cursor_find_reply().key().data()[0]);
    else
        return (0);
}

ham_u32_t
proto_cursor_find_reply_get_key_intflags(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    return (w->cursor_find_reply().key().intflags());
}

ham_size_t
proto_cursor_find_reply_get_key_size(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    return ((ham_size_t)w->cursor_find_reply().key().data().size());
}

ham_bool_t
proto_cursor_find_reply_has_record(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    return (w->cursor_find_reply().has_record());
}

void *
proto_cursor_find_reply_get_record_data(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    if (w->cursor_find_reply().record().has_data())
        return ((void *)&w->cursor_find_reply().record().data()[0]);
    else
        return (0);
}

ham_size_t
proto_cursor_find_reply_get_record_size(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    return ((ham_size_t)w->cursor_find_reply().record().data().size());
}

Protocol *
proto_init_cursor_get_duplicate_count_request(ham_u64_t cursorhandle, 
                ham_u32_t flags)
{
    Protocol *w=new Protocol();
    w->set_type(Protocol::CURSOR_GET_DUPLICATE_COUNT_REQUEST);
    w->mutable_cursor_get_duplicate_count_request()->set_cursor_handle(
                    cursorhandle);
    w->mutable_cursor_get_duplicate_count_request()->set_flags(flags);
    return ((Protocol *)w);
}

ham_bool_t
proto_has_cursor_get_duplicate_count_request(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    if (w->type()==Protocol::CURSOR_GET_DUPLICATE_COUNT_REQUEST) {
        ham_assert(w->has_cursor_get_duplicate_count_request()==true);
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_cursor_get_duplicate_count_request()==false);
        return (HAM_FALSE);
    }
}

ham_u64_t
proto_cursor_get_duplicate_count_request_get_cursor_handle(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    return (w->cursor_get_duplicate_count_request().cursor_handle());
}

ham_u32_t
proto_cursor_get_duplicate_count_request_get_flags(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    return (w->cursor_get_duplicate_count_request().flags());
}

Protocol *
proto_init_cursor_get_duplicate_count_reply(ham_status_t status,
                ham_u32_t count)
{
    Protocol *w=new Protocol();
    w->set_type(Protocol::CURSOR_GET_DUPLICATE_COUNT_REPLY);
    w->mutable_cursor_get_duplicate_count_reply()->set_status(status);
    w->mutable_cursor_get_duplicate_count_reply()->set_count(count);
    return ((Protocol *)w);
}

ham_bool_t
proto_has_cursor_get_duplicate_count_reply(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    if (w->type()==Protocol::CURSOR_GET_DUPLICATE_COUNT_REPLY) {
        ham_assert(w->has_cursor_get_duplicate_count_reply()==true);
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_cursor_get_duplicate_count_reply()==false);
        return (HAM_FALSE);
    }
}

ham_u32_t
proto_cursor_get_duplicate_count_reply_get_status(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    return (w->cursor_get_duplicate_count_reply().status());
}

ham_u32_t
proto_cursor_get_duplicate_count_reply_get_count(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    return (w->cursor_get_duplicate_count_reply().count());
}

Protocol *
proto_init_cursor_overwrite_request(ham_u64_t cursorhandle, 
                ham_record_t *record, ham_u32_t flags)
{
    Protocol *w=new Protocol();
    w->set_type(Protocol::CURSOR_OVERWRITE_REQUEST);
    w->mutable_cursor_overwrite_request()->set_cursor_handle(cursorhandle);
    w->mutable_cursor_overwrite_request()->set_flags(flags);
    if (record)
        Protocol::assign_record(w->mutable_cursor_overwrite_request()->mutable_record(),
                    record);
    return ((Protocol *)w);
}

ham_bool_t
proto_has_cursor_overwrite_request(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    if (w->type()==Protocol::CURSOR_OVERWRITE_REQUEST) {
        ham_assert(w->has_cursor_overwrite_request()==true);
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_cursor_overwrite_request()==false);
        return (HAM_FALSE);
    }
}

ham_u64_t
proto_cursor_overwrite_request_get_cursor_handle(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    return (w->cursor_overwrite_request().cursor_handle());
}

ham_u32_t
proto_cursor_overwrite_request_get_flags(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    return (w->cursor_overwrite_request().flags());
}

ham_u32_t
proto_cursor_overwrite_request_get_record_flags(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    return (w->cursor_overwrite_request().record().flags());
}

void *
proto_cursor_overwrite_request_get_record_data(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    if (w->cursor_overwrite_request().record().has_data())
        return ((void *)&w->cursor_overwrite_request().record().data()[0]);
    else
        return (0);
}

ham_size_t
proto_cursor_overwrite_request_get_record_size(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    return ((ham_size_t)w->cursor_overwrite_request().record().data().size());
}

ham_u32_t
proto_cursor_overwrite_request_get_record_partial_offset(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    return (w->cursor_overwrite_request().record().partial_offset());
}

ham_size_t
proto_cursor_overwrite_request_get_record_partial_size(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    return (w->cursor_overwrite_request().record().partial_size());
}

Protocol *
proto_init_cursor_overwrite_reply(ham_status_t status)
{
    Protocol *w=new Protocol();
    w->set_type(Protocol::CURSOR_OVERWRITE_REPLY);
    w->mutable_cursor_overwrite_reply()->set_status(status);
    return ((Protocol *)w);
}

ham_bool_t
proto_has_cursor_overwrite_reply(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    if (w->type()==Protocol::CURSOR_OVERWRITE_REPLY) {
        ham_assert(w->has_cursor_overwrite_reply()==true);
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_cursor_overwrite_reply()==false);
        return (HAM_FALSE);
    }
}

ham_u32_t
proto_cursor_overwrite_reply_get_status(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    return (w->cursor_overwrite_reply().status());
}

Protocol *
proto_init_cursor_move_request(ham_u64_t cursorhandle, ham_key_t *key,
        ham_record_t *record, ham_u32_t flags)
{
    Protocol *w=new Protocol();
    w->set_type(Protocol::CURSOR_MOVE_REQUEST);
    w->mutable_cursor_move_request()->set_cursor_handle(cursorhandle);
    w->mutable_cursor_move_request()->set_flags(flags);
    if (key)
        Protocol::assign_key(w->mutable_cursor_move_request()->mutable_key(),
                    key);
    if (record)
        Protocol::assign_record(w->mutable_cursor_move_request()->mutable_record(),
                    record);
    return ((Protocol *)w);
}

ham_bool_t
proto_has_cursor_move_request(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    if (w->type()==Protocol::CURSOR_MOVE_REQUEST) {
        ham_assert(w->has_cursor_move_request()==true);
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_cursor_move_request()==false);
        return (HAM_FALSE);
    }
}

ham_u64_t
proto_cursor_move_request_get_cursor_handle(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    return (w->cursor_move_request().cursor_handle());
}

ham_u32_t
proto_cursor_move_request_get_flags(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    return (w->cursor_move_request().flags());
}

ham_bool_t
proto_cursor_move_request_has_key(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    return (w->cursor_move_request().has_key());
}

void *
proto_cursor_move_request_get_key_data(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    if (w->cursor_move_request().key().has_data())
        return ((void *)&w->cursor_move_request().key().data()[0]);
    else
        return (0);
}

ham_u32_t
proto_cursor_move_request_get_key_flags(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    return (w->cursor_move_request().key().flags());
}

ham_size_t
proto_cursor_move_request_get_key_size(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    return ((ham_size_t)w->cursor_move_request().key().data().size());
}

ham_bool_t
proto_cursor_move_request_has_record(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    return (w->cursor_move_request().has_record());
}

void *
proto_cursor_move_request_get_record_data(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    if (w->cursor_move_request().record().has_data())
        return ((void *)&w->cursor_move_request().record().data()[0]);
    else
        return (0);
}

ham_size_t
proto_cursor_move_request_get_record_size(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    return ((ham_size_t)w->cursor_move_request().record().data().size());
}

ham_u32_t
proto_cursor_move_request_get_record_flags(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    return (w->cursor_move_request().record().flags());
}

ham_u32_t
proto_cursor_move_request_get_record_partial_offset(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    return (w->cursor_move_request().record().partial_offset());
}

ham_size_t
proto_cursor_move_request_get_record_partial_size(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    return (w->cursor_move_request().record().partial_size());
}

Protocol *
proto_init_cursor_move_reply(ham_status_t status, ham_key_t *key,
        ham_record_t *record)
{
    Protocol *w=new Protocol();
    w->set_type(Protocol::CURSOR_MOVE_REPLY);
    w->mutable_cursor_move_reply()->set_status(status);
    if (key)
        Protocol::assign_key(w->mutable_cursor_move_reply()->mutable_key(), key);
    if (record)
        Protocol::assign_record(w->mutable_cursor_move_reply()->mutable_record(), record);
    return ((Protocol *)w);
}

ham_bool_t
proto_has_cursor_move_reply(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    if (w->type()==Protocol::CURSOR_MOVE_REPLY) {
        ham_assert(w->has_cursor_move_reply()==true);
        return (HAM_TRUE);
    }
    else {
        ham_assert(w->has_cursor_move_reply()==false);
        return (HAM_FALSE);
    }
}

ham_u32_t
proto_cursor_move_reply_get_status(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    return (w->cursor_move_reply().status());
}

ham_bool_t
proto_cursor_move_reply_has_key(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    return (w->cursor_move_reply().has_key());
}

void *
proto_cursor_move_reply_get_key_data(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    if (w->cursor_move_reply().key().has_data())
        return ((void *)&w->cursor_move_reply().key().data()[0]);
    else
        return (0);
}

ham_u32_t
proto_cursor_move_reply_get_key_intflags(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    return (w->cursor_move_reply().key().intflags());
}

ham_size_t
proto_cursor_move_reply_get_key_size(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    return ((ham_size_t)w->cursor_move_reply().key().data().size());
}

ham_bool_t
proto_cursor_move_reply_has_record(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    return (w->cursor_move_reply().has_record());
}

void *
proto_cursor_move_reply_get_record_data(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    if (w->cursor_move_reply().record().has_data())
        return ((void *)&w->cursor_move_reply().record().data()[0]);
    else
        return (0);
}

ham_size_t
proto_cursor_move_reply_get_record_size(Protocol *wrapper)
{
    Protocol *w=(Protocol *)wrapper;
    return ((ham_size_t)w->cursor_move_reply().record().data().size());
}

