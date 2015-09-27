/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
 * All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * See the file COPYING for License information.
 */

/*
 * Abstraction layer for the remote protocol
 *
 * @exception_safe: no
 * @thread_safe: no
 */

#ifndef UPS_PROTOCOL_H
#define UPS_PROTOCOL_H

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!
#include "1mem/mem.h"
#include "1base/error.h"
#include "1base/dynamic_array.h"
#include "2protobuf/messages.pb.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

using namespace upscaledb;

/** a magic and version indicator for the remote protocol */
#define UPS_TRANSFER_MAGIC_V1   (('h'<<24)|('a'<<16)|('m'<<8)|'1')

/**
 * the Protocol class maps a single message that is exchanged between
 * client and server
 */
class Protocol : public upscaledb::ProtoWrapper
{
  public:
    Protocol() { }

    /** constructor - assigns a type */
    Protocol(upscaledb::ProtoWrapper_Type type) {
      set_type(type);
    }

    /** helper function which copies a ups_key_t into a ProtoBuf key */
    static void assign_key(upscaledb::Key *protokey, ups_key_t *hamkey,
            bool deep_copy = true) {
      if (deep_copy)
        protokey->set_data(hamkey->data, hamkey->size);
      protokey->set_flags(hamkey->flags);
      protokey->set_intflags(hamkey->_flags);
    }

    /** helper function which copies a ups_record_t into a ProtoBuf record */
    static void assign_record(upscaledb::Record *protorec,
            ups_record_t *hamrec, bool deep_copy = true) {
      if (deep_copy)
        protorec->set_data(hamrec->data, hamrec->size);
      protorec->set_flags(hamrec->flags);
      protorec->set_partial_offset(hamrec->partial_offset);
      protorec->set_partial_size(hamrec->partial_size);
    }

    /**
     * Factory function; creates a new Protocol structure from a serialized
     * buffer
     */
    static Protocol *unpack(const uint8_t *buf, uint32_t size) {
      if (*(uint32_t *)&buf[0] != UPS_TRANSFER_MAGIC_V1) {
        ups_trace(("invalid protocol version"));
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
    bool pack(uint8_t **data, uint32_t *size) {
      uint32_t packed_size = ByteSize();
      /* we need 8 more bytes for magic and size */
      uint8_t *p = Memory::allocate<uint8_t>(packed_size + 8);
      if (!p)
        return (false);

      /* write the magic and the payload size of the packed structure */
      *(uint32_t *)&p[0] = UPS_TRANSFER_MAGIC_V1;
      *(uint32_t *)&p[4] = packed_size;

      /* now write the packed structure */
      if (!SerializeToArray(&p[8], packed_size)) {
        Memory::release(p);
        return (false);
      }

      *data = p;
      *size = packed_size + 8;
      return (true);
    }

    /*
     * Packs the Protocol structure into a ByteArray
     */
    bool pack(ByteArray *barray) {
      uint32_t packed_size = ByteSize();
      /* we need 8 more bytes for magic and size */
      uint8_t *p = (uint8_t *)barray->resize(packed_size + 8);
      if (!p)
        return (false);

      /* write the magic and the payload size of the packed structure */
      *(uint32_t *)&p[0] = UPS_TRANSFER_MAGIC_V1;
      *(uint32_t *)&p[4] = packed_size;

      /* now write the packed structure */
      return (SerializeToArray(&p[8], packed_size));
    }

    /**
     * shutdown/free globally allocated memory
     */
    static void shutdown() {
      google::protobuf::ShutdownProtobufLibrary();
    }
};

#endif /* UPS_PROTOCOL_H */
