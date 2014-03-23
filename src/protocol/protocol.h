/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 */

/**
 * @brief Abstraction layer for the remote protocol
 */

#ifndef HAM_PROTOCOL_H__
#define HAM_PROTOCOL_H__

#include <ham/hamsterdb.h>
#include "../mem.h"
#include "../error.h"
#include "../util.h"
#include "../endianswap.h"
#include "messages.pb.h"

using namespace hamsterdb;

/** a magic and version indicator for the remote protocol */
#define HAM_TRANSFER_MAGIC_V1   (('h'<<24)|('a'<<16)|('m'<<8)|'1')

/**
 * the Protocol class maps a single message that is exchanged between
 * client and server
 */
class Protocol : public hamsterdb::ProtoWrapper
{
  public:
    Protocol() { }

    /** constructor - assigns a type */
    Protocol(hamsterdb::ProtoWrapper_Type type) {
      set_type(type);
    }

    /** helper function which copies a ham_key_t into a ProtoBuf key */
    static void assign_key(hamsterdb::Key *protokey, ham_key_t *hamkey,
            bool deep_copy = true) {
      if (deep_copy)
        protokey->set_data(hamkey->data, hamkey->size);
      protokey->set_flags(hamkey->flags);
      protokey->set_intflags(hamkey->_flags);
    }

    /** helper function which copies a ham_record_t into a ProtoBuf record */
    static void assign_record(hamsterdb::Record *protorec,
            ham_record_t *hamrec, bool deep_copy = true) {
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
    static Protocol *unpack(const ham_u8_t *buf, ham_u32_t size) {
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
    bool pack(ham_u8_t **data, ham_u32_t *size) {
      ham_u32_t packed_size = ByteSize();
      /* we need 8 more bytes for magic and size */
      ham_u8_t *p = Memory::allocate<ham_u8_t>(packed_size + 8);
      if (!p)
        return (false);

      /* write the magic and the payload size of the packed structure */
      *(ham_u32_t *)&p[0] = ham_h2db32(HAM_TRANSFER_MAGIC_V1);
      *(ham_u32_t *)&p[4] = ham_h2db32(packed_size);

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
      ham_u32_t packed_size = ByteSize();
      /* we need 8 more bytes for magic and size */
      ham_u8_t *p = (ham_u8_t *)barray->resize(packed_size + 8);
      if (!p)
        return (false);

      /* write the magic and the payload size of the packed structure */
      *(ham_u32_t *)&p[0] = ham_h2db32(HAM_TRANSFER_MAGIC_V1);
      *(ham_u32_t *)&p[4] = ham_h2db32(packed_size);

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

#endif /* HAM_PROTOCOL_H__ */
