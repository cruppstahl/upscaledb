/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
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

#include "1os/os.h"

namespace hamsterdb {

#ifdef HAM_ENABLE_SIMD

// AVX might be enabled at compile time, but it's still possible that
// it's not enabled at run-time because the CPU is an older model.

// from http://stackoverflow.com/questions/6121792/how-to-check-if-a-cpu-supports-the-sse3-instruction-set

#ifdef _WIN32
//  Windows
#define cpuid    __cpuid
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

extern bool
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

#else // !HAM_ENABLE_SIMD

int
os_get_simd_lane_width()
{
  return (0);
}

#endif // HAM_ENABLE_SIMD

} // namespace hamsterdb

