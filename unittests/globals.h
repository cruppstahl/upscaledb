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

#include "../src/config.h"

#include "ham/hamsterdb_int.h"

#define REQUIRE_CATCH(x, y) \
        try { x; } catch (Exception &ex) { REQUIRE(ex.code == y); }

struct Globals {
  static const char *opath(const char *filename) {
    return (filename);
  }

  static const char *ipath(const char *filename) {
    return (filename);
  }
};
