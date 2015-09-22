#ifndef STREAMVARIABLEBYTE_
#define STREAMVARIABLEBYTE_

#include "common.h"
#include "codecs.h"
extern "C" {
uint64_t svb_encode(uint8_t *out, const uint32_t *in, uint32_t count,
                    int delta, int type);
uint8_t *svb_decode_avx_simple(uint32_t *out, uint8_t * keyPtr, uint8_t * dataPtr, uint64_t count);
uint8_t *svb_decode_avx_d1_simple(uint32_t *out, uint8_t * keyPtr, uint8_t * dataPtr, uint64_t count);

}



class SimpleStreamVByteAVX : public IntegerCODEC {
public:
    void encodeArray(uint32_t *in, const size_t count, uint32_t *out,
                     size_t &nvalue) {
        uint64_t bytesWritten = svb_encode((uint8_t *)out, in, count, 0, 1);
        nvalue = (bytesWritten + 3)/4;
    }

    const uint32_t * decodeArray(const uint32_t *in, const size_t /* count */,
                                 uint32_t *out, size_t & nvalue) {
        uint32_t count = *(uint32_t *)in;  // first 4 bytes is number of ints
        nvalue = count;
        if (count == 0) return 0;

        uint8_t *keyPtr = (uint8_t *)in + 4; // full list of keys is next
        uint32_t keyLen = ((count + 3) / 4);   // 2-bits per key (rounded up)
        uint8_t *dataPtr = keyPtr + keyLen;    // data starts at end of keys
        nvalue = count;
        return reinterpret_cast<const uint32_t *>((reinterpret_cast<uintptr_t>(
        		svb_decode_avx_simple(out, keyPtr, dataPtr, count))
                + 3) & ~3);

    }

    std::string name() const {
        return "streamvbyte_avx_simple";
    }

};


class SimpleStreamVByteAVXD1 : public IntegerCODEC {
public:

    void encodeArray(uint32_t *in, const size_t count, uint32_t *out,
                     size_t &nvalue) {
        uint64_t bytesWritten = svb_encode((uint8_t *)out, in, count, 1, 1);
        nvalue = (bytesWritten + 3)/4;
    }

    const uint32_t * decodeArray(const uint32_t *in, const size_t /* count */,
                                 uint32_t *out, size_t & nvalue) {
        uint32_t count = *(uint32_t *)in;  // first 4 bytes is number of ints
        nvalue = count;
        if (count == 0) return 0;

        uint8_t *keyPtr = (uint8_t *)in + 4; // full list of keys is next
        uint32_t keyLen = ((count + 3) / 4);   // 2-bits per key (rounded up)
        uint8_t *dataPtr = keyPtr + keyLen;    // data starts at end of keys
        nvalue = count;
        return reinterpret_cast<const uint32_t *>((reinterpret_cast<uintptr_t>(
        		svb_decode_avx_d1_simple(out, keyPtr, dataPtr, count))
                + 3) & ~3);
    }

    std::string name() const {
        return "streamvbyte_avx_d1_simple";
    }

};


#endif // STREAMVARIABLEBYTE_
