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
#include "../error.h"
#include "../endianswap.h"
#include "messages.pb.h"

/** a magic and version indicator for the remote protocol */
#define HAM_TRANSFER_MAGIC_V1   (('h'<<24)|('a'<<16)|('m'<<8)|'1')

/**
 * the Protocol class maps a single message that is exchanged between
 * client and server
 */
class Protocol : public ham::ProtoWrapper
{
  public:
    Protocol() { }

    /** constructor - assigns a type */
    Protocol(ham::ProtoWrapper_Type type) {
      set_type(type);
    }

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
    static Protocol *unpack(const ham_u8_t *buf, ham_size_t size) {
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

    /* 
     * Packs the Protocol structure into a memory buffer and returns
     * a pointer to the buffer and the buffer size 
     */
    bool pack(Allocator *alloc, ham_u8_t **data, ham_size_t *size) {
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

    /**
     * shutdown/free globally allocated memory
     */
    static void shutdown() {
      google::protobuf::ShutdownProtobufLibrary();
    }
};

#endif /* HAM_PROTOCOL_H__ */
