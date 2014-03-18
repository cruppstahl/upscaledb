/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

#include <stdio.h>

#include <ham/hamsterdb_int.h>

#include "common.h"

void
print_banner(const char *program_name)
{
  ham_u32_t maj, min, rev;
  const char *licensee, *product;
  ham_get_license(&licensee, &product);
  ham_get_version(&maj, &min, &rev);

  printf("hamsterdb %s%d.%d.%d - Copyright (C) 2005-2014 "
       "Christoph Rupp (chris@crupp.de).\n\n",
       ham_is_pro() ? "pro " : "", maj, min, rev);

  if (!ham_is_pro())
    printf(
       "This program is free software; you can redistribute "
       "it and/or modify it\nunder the terms of the GNU "
       "General Public License as published by the Free\n"
       "Software Foundation; either version 2 of the License,\n"
       "or (at your option) any later version.\n\n"
       "See file COPYING.GPL2 and COPYING.GPL3 for License "
       "information.\n\n");
  else
    printf(
       "Commercial version; licensed for %s (%s)\n\n",
       licensee, product);
}

