/*
 * Copyright (C) 2005-2016 Christoph Rupp (chris@crupp.de).
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

#include "ups/upscaledb_int.h"

#include "1base/error.h"
#ifdef UPS_ENABLE_REMOTE
#  include "2protobuf/protocol.h"
#endif
#include "4uqi/plugins.h"

#define CATCH_CONFIG_RUNNER 1
#include "3rdparty/catch/catch.hpp"


int
main(int argc, char *argv[])
{
  Catch::Session session;
  int result = session.run(argc, argv);

  ups_at_exit();
  Catch::cleanUp();

  return result;
}
