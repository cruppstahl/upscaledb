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

#include "1os/os.h"

namespace upscaledb {

#ifdef __SSE__

// AVX might be enabled at compile time, but it's still possible that
// it's not enabled at run-time because the CPU is an older model.

// from http://stackoverflow.com/questions/6121792/how-to-check-if-a-cpu-supports-the-sse3-instruction-set

#ifdef _WIN32
//  Windows
#  include <intrin.h>
#  define cpuid    __cpuid
#else
//  GCC Inline Assembly
static void
cpuid(int cpuinfo[4], int infotype){
  __asm__ __volatile__ (
      "cpuid":
      "=a" (cpuinfo[0]),
      "=b" (cpuinfo[1]),
      "=c" (cpuinfo[2]),
      "=d" (cpuinfo[3]) :
      "a" (infotype)
  );
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

  return (available);
}

int
os_get_simd_lane_width()
{
  return (os_has_avx() ? 8 : 4);
}

#else // !__SSE__

int
os_get_simd_lane_width()
{
  return (0);
}

#endif // __SSE__

} // namespace upscaledb

