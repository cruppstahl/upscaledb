/**
 * Copyright (C) 2005-2013 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

#include "../src/config.h"

#define CATCH_CONFIG_RUNNER 1
#include "3rdparty/catch/catch.hpp"

#include "../src/error.h"
#ifdef HAM_ENABLE_REMOTE
#  include "../src/protocol/protocol.h"
#endif

int
main(int argc, char *const argv[])
{
  int result = Catch::Main(argc, argv);

#ifdef HAM_ENABLE_REMOTE
  Protocol::shutdown();
#endif

  return (result);
}
