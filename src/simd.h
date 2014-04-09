/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 * All Rights Reserved.
 *
 * NOTICE: All information contained herein is, and remains the property
 * of Christoph Rupp and his suppliers, if any. The intellectual and
 * technical concepts contained herein are proprietary to Christoph Rupp
 * and his suppliers and may be covered by Patents, patents in process,
 * and are protected by trade secret or copyright law. Dissemination of
 * this information or reproduction of this material is strictly forbidden
 * unless prior written permission is obtained from Christoph Rupp.
 */

#ifndef HAM_SIMD_H__
#define HAM_SIMD_H__

#include "error.h"

#ifdef HAM_ENABLE_SIMD

#ifdef WIN32
#  include <xmmintrin.h>
#  include <smmintrin.h>
#  include <immintrin.h>
#else
#  include <x86intrin.h>
#endif

namespace hamsterdb {

template<typename T>
int
linear_search(T *data, int start, int count, T key)
{
  register ham_u32_t c = start;
  ham_u32_t end = start + count;

#undef COMPARE
#define COMPARE(c)      if (key <= data[c]) {                           \
                          if (key < data[c])                            \
                            return (-1);                                \
                          return (c);                                   \
                        }

  while (c + 8 <= end) {
    COMPARE(c)
    COMPARE(c + 1)
    COMPARE(c + 2)
    COMPARE(c + 3)
    COMPARE(c + 4)
    COMPARE(c + 5)
    COMPARE(c + 6)
    COMPARE(c + 7)
    c += 8;
  }

  while (c < end) {
    COMPARE(c)
    c++;
  }

  /* the new key is > the last key in the page */
  return (-1);
}

template<typename T>
int
linear_search_sse(T *data, int start, int count, T key)
{
  return (linear_search(data, start, count, key));
}

template<typename T>
static int
get_sse_threshold()
{
  return (16);
}

template<>
int
get_sse_threshold<ham_u32_t>()
{
  return (16);
}

template<>
int
get_sse_threshold<float>()
{
  return (16);
}

template<>
int
get_sse_threshold<ham_u64_t>()
{
  return (4);
}

template<typename T>
int
find_simd_sse(T *data, ham_u32_t count, ham_key_t *hkey)
{
  ham_assert(hkey->size == sizeof(T));
  T key = *(T *)hkey->data;

  // Run a binary search, but fall back to linear search as soon as
  // the remaining range is too small
  int threshold = get_sse_threshold<T>();
  int i, l = 0, r = count;
  int last = count + 1;

  /* repeat till we found the key or the remaining range is so small that
   * we rather perform a linear search (which is faster for small ranges) */
  while (r - l > threshold) {
    /* get the median item; if it's identical with the "last" item,
     * we've found the slot */
    i = (l + r) / 2;

    if (i == last) {
      ham_assert(i >= 0);
      ham_assert(i < (int)count);
      return (-1);
    }

    /* found it? */
    register T d = data[i];
    /* if the key is < the current item: search "to the left" */
    if (key < d) {
      if (r == 0) {
        ham_assert(i == 0);
        return (-1);
      }
      r = i;
    }
    /* if the key is > the current item: search "to the right" */
    else if (key > d) {
      last = i;
      l = i;
    }
    /* otherwise we found the key */
    else
      return (i);
  }

  // still here? then perform a linear search for the remaining range
  ham_assert(r - l <= threshold);
  return (linear_search_sse(data, l, r - l, key));
}

template<>
inline int
linear_search_sse<ham_u16_t>(ham_u16_t *data, int start, int count,
                ham_u16_t key)
{
  __m128i key8 = _mm_set1_epi16(key);

  if (count < 16)
    return (linear_search(data, start, count, key));

  __m128i v1 = _mm_loadu_si128((const __m128i *)&data[start + 0]);
  __m128i v2 = _mm_loadu_si128((const __m128i *)&data[start + 8]);

  __m128i cmp0 = _mm_cmpeq_epi16(key8, v1);
  __m128i cmp1 = _mm_cmpeq_epi16(key8, v2);

  __m128i pack01 = _mm_packs_epi16(cmp0, cmp1);

  int res = _mm_movemask_epi8(pack01);
  if (res > 0)
    return (start + __builtin_ctz(~res + 1));

  ham_assert(16 == count);
  /* the new key is > the last key in the page */
  return (-1);
}

template<>
inline int
linear_search_sse<ham_u32_t>(ham_u32_t *data, int start, int count,
                ham_u32_t key)
{
  __m128i key4 = _mm_set1_epi32(key);

  if (count < 16)
    return (linear_search(data, start, count, key));

  __m128i v1 = _mm_loadu_si128((const __m128i *)&data[start + 0]);
  __m128i v2 = _mm_loadu_si128((const __m128i *)&data[start + 4]);
  __m128i v3 = _mm_loadu_si128((const __m128i *)&data[start + 8]);
  __m128i v4 = _mm_loadu_si128((const __m128i *)&data[start + 12]);

  __m128i cmp0 = _mm_cmpeq_epi32(key4, v1);
  __m128i cmp1 = _mm_cmpeq_epi32(key4, v2);
  __m128i cmp2 = _mm_cmpeq_epi32(key4, v3);
  __m128i cmp3 = _mm_cmpeq_epi32(key4, v4);

  __m128i pack01 = _mm_packs_epi32(cmp0, cmp1);
  __m128i pack23 = _mm_packs_epi32(cmp2, cmp3);
  __m128i pack0123 = _mm_packs_epi16(pack01, pack23);

  int res = _mm_movemask_epi8(pack0123);
  if (res > 0)
    return (start + __builtin_ctz(~res + 1));

  ham_assert(16 == count);
  /* the new key is > the last key in the page */
  return (-1);
}

template<>
inline int
linear_search_sse<float>(float *data, int start, int count,
                float key)
{
  __m128 key4 = _mm_set1_ps(key);

  if (count < 16)
    return (linear_search(data, start, count, key));

  __m128 v1 = _mm_loadu_ps((const float *)&data[start + 0]);
  __m128 v2 = _mm_loadu_ps((const float *)&data[start + 4]);
  __m128 v3 = _mm_loadu_ps((const float *)&data[start + 8]);
  __m128 v4 = _mm_loadu_ps((const float *)&data[start + 12]);

  __m128 cmp0 = _mm_cmpeq_ps(key4, v1);
  __m128 cmp1 = _mm_cmpeq_ps(key4, v2);
  __m128 cmp2 = _mm_cmpeq_ps(key4, v3);
  __m128 cmp3 = _mm_cmpeq_ps(key4, v4);

  __m128i pack01 = _mm_packs_epi32(_mm_castps_si128(cmp0),
                          _mm_castps_si128(cmp1));
  __m128i pack23 = _mm_packs_epi32(_mm_castps_si128(cmp2),
                          _mm_castps_si128(cmp3));
  __m128i pack0123 = _mm_packs_epi16(pack01, pack23);

  int res = _mm_movemask_epi8(pack0123);
  if (res > 0)
    return (start + __builtin_ctz(~res + 1));

  ham_assert(16 == count);
  /* the new key is > the last key in the page */
  return (-1);
}

#ifdef __SSE4_1__
template<>
inline int
linear_search_sse<ham_u64_t>(ham_u64_t *data, int start, int count,
                ham_u64_t key)
{
  __m128i key2 = _mm_set1_epi64x(key);

  if (count < 4)
    return (linear_search(data, start, count, key));

  __m128i v1 = _mm_loadu_si128((const __m128i *)&data[start + 0]);
  __m128i v2 = _mm_loadu_si128((const __m128i *)&data[start + 2]);

  __m128i cmp0 = _mm_cmpeq_epi64(key2, v1);
  __m128i cmp1 = _mm_cmpeq_epi64(key2, v2);

  __m128i low2  = _mm_shuffle_epi32(cmp0, 0xD8);
  __m128i high2 = _mm_shuffle_epi32(cmp1, 0xD8);
  __m128i pack = _mm_unpacklo_epi64(low2, high2);

  __m128i pack01 = _mm_packs_epi32(pack, _mm_setzero_si128());
  __m128i pack0123 = _mm_packs_epi16(pack01, _mm_setzero_si128());

  int res = _mm_movemask_epi8(pack0123);
  if (res > 0)
    return (start + __builtin_ctz(~res + 1));

  ham_assert(4 == count);
  /* the new key is > the last key in the page */
  return (-1);
}
#endif

} // namespace hamsterdb

#endif // HAM_ENABLE_SIMD

#endif /* HAM_SIMD_H__ */
