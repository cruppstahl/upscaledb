/**
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

#ifdef HAM_ENABLE_SSE2

#include "../src/config.h"

#include "3rdparty/catch/catch.hpp"

#include "globals.h"

#include "../src/os.h"

using namespace hamsterdb;

TEST_CASE("Sse2/isSse2Available", "")
{
  REQUIRE(os_has_feature(kCpuFeatureMMX));
  REQUIRE(os_has_feature(kCpuFeatureSSE));
  REQUIRE(os_has_feature(kCpuFeatureSSE2));
}

#endif // HAM_ENABLE_SSE2
