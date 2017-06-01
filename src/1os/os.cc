/*
 * Copyright (C) 2005-2017 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * See the file COPYING for License information.
 */

#include "1os/os.h"

namespace upscaledb {

#ifdef HAVE_SSE2

// AVX might be enabled at compile time, but it's still possible that
// it's not enabled at run-time because the CPU is an older model.

// from http://stackoverflow.com/questions/6121792/how-to-check-if-a-cpu-supports-the-sse3-instruction-set

#ifdef _WIN32
//  Windows
#  include <intrin.h>
#  define cpuid    __cpuid
#else
#  include <cpuid.h>
static void
cpuid(int info[4], int level) {
  __get_cpuid(level, (unsigned int *)&info[0], (unsigned int *)&info[1],
                  (unsigned int *)&info[2], (unsigned int *)&info[3]);
/*
  __asm__ __volatile__ (
      "cpuid":
      "=a" (cpuinfo[0]),
      "=b" (cpuinfo[1]),
      "=c" (cpuinfo[2]),
      "=d" (cpuinfo[3]) :
      "a" (infotype)
  );*/
}
#endif

bool
os_has_avx()
{
  static bool available = false;
  static bool initialized = false;
  if (!initialized) {
    initialized = true;

    int info[4];
    cpuid(info, 0);
    int num_ids = info[0];

    cpuid(info, 0x80000000);

    //  Detect Instruction Set
    if (num_ids >= 1) {
      cpuid(info, 0x00000001);
      available = (info[2] & ((int)1 << 28)) != 0;
    }
  }

  return available;
}

#else // !HAVE_SSE2

bool
os_has_avx()
{
  return false;
}

#endif // HAVE_SSE2

} // namespace upscaledb

