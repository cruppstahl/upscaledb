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

#include "ups/upscaledb_int.h"

#define REQUIRE_CATCH(x, y) \
        try { x; } catch (Exception &ex) { REQUIRE(ex.code == y); }

struct Utils {
  static const char *opath(const char *filename) {
    return (filename);
  }

  static const char *ipath(const char *filename) {
    return (filename);
  }
};
