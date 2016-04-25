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

#define VERIFY(c)     while (!c) {                                          \
                        printf("%s:%d: expression failed\n",                \
                                        __FILE__, __LINE__);                \
                        exit(-1);                                           \
                      }

#define VERIFY_ARRAY(a1, a2, len)                                           \
                      do {                                                  \
                        uint32_t i;                                         \
                        for (i = 0; i < len; i++) {                         \
                          if (a1[i] != a2[i]) {                             \
                            printf("data mismatch at %u\n", i);             \
                            exit(-1);                                       \
                          }                                                 \
                        }                                                   \
                      } while (0)

static void
run(uint32_t length)
{
  uint32_t i, s1, s2, s3;
  uint8_t  *out = (uint8_t *) malloc(length * sizeof(uint32_t));
  uint32_t *in =  (uint32_t *)malloc(length * sizeof(uint32_t));
  uint32_t *tmp = (uint32_t *)malloc(length * sizeof(uint32_t));

  for (i = 0; i < length; i++)
    in[i] = 33 + i;

  s1 = for_compress_sorted(in, out, length);
  s2 = for_uncompress(out, tmp, length);
  s3 = for_compressed_size_sorted(in, length);
  VERIFY(s1 == s2);
  VERIFY(s2 == s3);
  /* VERIFY_ARRAY(in, tmp, length); */

  free(in);
  free(out);
  free(tmp);
}

int
main()
{
  run(1024 * 1024 * 10); /* 10 mb */
  return 0;
}
