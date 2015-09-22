/**
 * This code is released under a BSD License.
 */
#ifndef SIMDBITPACKING_H_
#define SIMDBITPACKING_H_

#include "portability.h"

/* SSE2 is required */
#include <emmintrin.h>
/* for memset */
#include <string.h>

/* reads 128 values from "in", writes  "bit" 128-bit vectors to "out" */
void simdpack(const uint32_t *  in,__m128i *  out, const uint32_t bit);

/* reads 128 values from "in", writes  "bit" 128-bit vectors to "out" */
void simdpackwithoutmask(const uint32_t *  in,__m128i *  out, const uint32_t bit);

/* reads  "bit" 128-bit vectors from "in", writes  128 values to "out" */
void simdunpack(const __m128i *  in,uint32_t *  out, const uint32_t bit);

/* like simdpack, but supports an undetermined number of inputs. This is useful if you need to pack less than 128 integers. Note that this function is much slower. */
__m128i * simdpack_length(const uint32_t *   in, int length, __m128i *    out, const uint32_t bit);

/* like simdunpack, but supports an undetermined number of inputs. This is useful if you need to unpack less than 128 integers. Note that this function is much slower. */
const __m128i * simdunpack_length(const __m128i *   in, int length, uint32_t * out, const uint32_t bit);

/* given a block of 128 packed values, this function sets the value at index "index" to "value" */
void simdfastset(__m128i * in128, uint32_t b, uint32_t value, size_t index);

#endif /* SIMDBITPACKING_H_ */
