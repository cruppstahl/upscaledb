/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <stdio.h>
#include <stdlib.h>

#include "for.h"

typedef uint32_t (*for_unpackfunc_t) (uint32_t, const uint8_t *, uint32_t *);
typedef uint32_t (*for_packfunc_t)   (uint32_t, const uint32_t *, uint8_t *);
typedef uint32_t (*for_unpackxfunc_t)(uint32_t, const uint8_t *, uint32_t *,
                        uint32_t);
typedef uint32_t (*for_packxfunc_t)  (uint32_t, const uint32_t *, uint8_t *,
                        uint32_t);

extern for_packfunc_t for_pack32[33];
extern for_unpackfunc_t for_unpack32[33];
extern for_packfunc_t for_pack16[33];
extern for_unpackfunc_t for_unpack16[33];
extern for_packfunc_t for_pack8[33];
extern for_unpackfunc_t for_unpack8[33];
extern for_packxfunc_t for_packx[33];
extern for_unpackxfunc_t for_unpackx[33];

#define VERIFY(c)     while (!(c)) {                                        \
                        printf("%s:%d: expression failed\n",                \
                                        __FILE__, __LINE__);                \
                        abort();                                            \
                      }

#define VERIFY_ARRAY(a1, a2, len)                                           \
                      do {                                                  \
                        uint32_t i;                                         \
                        for (i = 0; i < len; i++) {                         \
                          if (a1[i] != a2[i]) {                             \
                            printf("data mismatch at %u\n", i);             \
                            abort();                                        \
                          }                                                 \
                        }                                                   \
                      } while (0)

static uint32_t inbuf[1024];

static uint32_t *
generate_input(uint32_t base, uint32_t length, uint32_t bits)
{
  uint32_t i;
  uint32_t max = (1 << bits) - 1;

  for (i = 0; i < length; i++) {
    if (bits == 0)
      inbuf[i] = base;
    else if (bits == 32)
      inbuf[i] = base + i;
    else
      inbuf[i] = base + (i % max);
  }

  return &inbuf[0];
}

static void
highlevel_sorted(uint32_t length)
{
  uint32_t i, s1, s2, s3;
  uint8_t out[1024 * 10];
  uint32_t in[1024 * 10];
  uint32_t tmp[1024 * 10];

  printf("highlevel sorted %u ints\n", length);

  for (i = 0; i < length; i++)
    in[i] = 33 + i;

  s3 = for_compressed_size_sorted(in, length);
  tmp[s3] = 'x';
  s1 = for_compress_sorted(in, out, length);
  VERIFY(tmp[s3] == 'x');
  s2 = for_uncompress(out, tmp, length);
  VERIFY(s1 == s2);
  VERIFY(s2 == s3);
  VERIFY_ARRAY(in, tmp, length);

  for (i = 0; i < length; i++)
    VERIFY(in[i] == for_select(out, i));

  for (i = 0; i < length; i++)
    VERIFY(i == for_linear_search(out, length, in[i]));

  for (i = 0; i < length; i++) {
    uint32_t actual;
    uint32_t index = for_lower_bound_search(out, length, in[i], &actual);
    VERIFY(in[i] == in[index]);
    VERIFY(actual == in[i]);
  }
}

static uint32_t
rnd()
{
  static uint32_t a = 3;
  a = (((a * 214013L + 2531011L) >> 16) & 32767);
  return (a);
}

static void
highlevel_unsorted(uint32_t length)
{
  uint32_t i, s1, s2, s3;
  uint8_t out[1024 * 10];
  uint32_t in[1024 * 10];
  uint32_t tmp[1024 * 10];

  printf("highlevel unsorted %u ints\n", length);

  for (i = 0; i < length; i++)
    in[i] = 7 + (rnd() - 7);

  s3 = for_compressed_size_unsorted(in, length);
  tmp[s3] = 'x';
  s1 = for_compress_unsorted(in, out, length);
  s2 = for_uncompress(out, tmp, length);
  VERIFY(s1 == s2);
  VERIFY(s2 == s3);
  VERIFY(tmp[s3] == 'x');
  VERIFY_ARRAY(in, tmp, length);

  for (i = 0; i < length; i++)
    VERIFY(in[i] == for_select(out, i));

  for (i = 0; i < length; i++) {
    uint32_t index = for_linear_search(out, length, in[i]);
    VERIFY(in[i] == in[index]);
  }
}

static void
lowlevel_block_func(uint32_t bits, for_packfunc_t pack, for_unpackfunc_t unpack,
                uint32_t *in, uint32_t base, uint32_t length)
{
  uint32_t i;
  uint8_t out[1024];
  uint32_t tmp[1024];

  uint32_t s1 = pack(base, in, out);
  uint32_t s2 = unpack(base, out, tmp);
  VERIFY(s1 == s2);
  VERIFY_ARRAY(in, tmp, length);

  for (i = 0; i < length; i++)
    VERIFY(in[i] == for_select_bits(out, base, bits, i));

  for (i = 0; i < length; i++) {
    uint32_t index = for_linear_search_bits(out, length, base, bits, in[i]);
    VERIFY(in[i] == in[index]);
  }
}

static void
lowlevel_blockx_func(uint32_t bits, for_packxfunc_t pack,
                for_unpackxfunc_t unpack, uint32_t *in, uint32_t base,
                uint32_t length)
{
  uint32_t i;
  uint8_t out[1024];
  uint32_t tmp[1024];

  uint32_t s1 = pack(base, in, out, length);
  uint32_t s2 = unpack(base, out, tmp, length);
  VERIFY(s1 == s2);
  VERIFY_ARRAY(in, tmp, length);

  for (i = 0; i < length; i++)
    VERIFY(in[i] == for_select_bits(out, base, bits, i));

  for (i = 0; i < length; i++) {
    uint32_t index = for_linear_search_bits(out, length, base, bits, in[i]);
    VERIFY(in[i] == in[index]);
  }
}

static void
lowlevel_block32(uint32_t bits)
{
  uint32_t *in = generate_input(10, 32, bits);

  printf("lowlevel pack/unpack 32 ints, %2d bits\n", bits);
  lowlevel_block_func(bits, for_pack32[bits], for_unpack32[bits], in, 10, 32);
}

static void
lowlevel_block16(uint32_t bits)
{
  uint32_t *in = generate_input(10, 16, bits);

  printf("lowlevel pack/unpack 16 ints, %2d bits\n", bits);
  lowlevel_block_func(bits, for_pack16[bits], for_unpack16[bits], in, 10, 16);
}

static void
lowlevel_block8(uint32_t bits)
{
  uint32_t *in = generate_input(10, 8, bits);

  printf("lowlevel pack/unpack  8 ints, %2d bits\n", bits);
  lowlevel_block_func(bits, for_pack8[bits], for_unpack8[bits], in, 10, 8);
}

static void
lowlevel_blockx(int length, uint32_t bits)
{
  uint32_t *in = generate_input(10, 8, bits);

  printf("lowlevel pack/unpack  %d ints, %2d bits\n", length, bits);
  lowlevel_blockx_func(bits, for_packx[bits],
                  for_unpackx[bits], in, 10, length);
}

int
main()
{
  int i, b;

  for (i = 0; i <= 32; i++)
    lowlevel_block32(i);

  for (i = 0; i <= 32; i++)
    lowlevel_block16(i);

  for (i = 0; i <= 32; i++)
    lowlevel_block8(i);

  for (i = 0; i < 32; i++) {
    for (b = 0; b < 8; b++)
      lowlevel_blockx(b, i);
  }

  highlevel_sorted(0);
  highlevel_sorted(1);
  highlevel_sorted(2);
  highlevel_sorted(3);
  highlevel_sorted(16);
  highlevel_sorted(17);
  highlevel_sorted(32);
  highlevel_sorted(33);
  highlevel_sorted(64);
  highlevel_sorted(65);
  highlevel_sorted(128);
  highlevel_sorted(129);
  highlevel_sorted(256);
  highlevel_sorted(257);
  highlevel_sorted(1024);
  highlevel_sorted(1025);
  highlevel_sorted(1333);

  highlevel_unsorted(0);
  highlevel_unsorted(1);
  highlevel_unsorted(2);
  highlevel_unsorted(3);
  highlevel_unsorted(16);
  highlevel_unsorted(17);
  highlevel_unsorted(32);
  highlevel_unsorted(33);
  highlevel_unsorted(64);
  highlevel_unsorted(65);
  highlevel_unsorted(128);
  highlevel_unsorted(129);
  highlevel_unsorted(256);
  highlevel_unsorted(257);
  highlevel_unsorted(1024);
  highlevel_unsorted(1025);
  highlevel_unsorted(1333);

  printf("\nsuccess!\n");
  return 0;
}
