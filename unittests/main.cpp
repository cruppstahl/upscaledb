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

#define CATCH_CONFIG_RUNNER 1
#include "3rdparty/catch/catch.hpp"

#include "1base/error.h"
#ifdef HAM_ENABLE_REMOTE
#  include "2protobuf/protocol.h"
#endif

int
main(int argc, char *const argv[])
{
  int result = Catch::Main(argc, argv);

#ifdef HAM_ENABLE_REMOTE
  Protocol::shutdown();
#endif

  Catch::cleanUp();

  return (result);
}
