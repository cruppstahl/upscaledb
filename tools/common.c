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

#include <stdio.h>
#include <time.h>

#include <ham/hamsterdb_int.h>

#include "common.h"

void
print_banner(const char *program_name)
{
  uint32_t maj, min, rev;
  ham_get_version(&maj, &min, &rev);

  printf("hamsterdb %s%d.%d.%d - Copyright (C) 2005-2015 "
       "Christoph Rupp (chris@crupp.de).\n\n",
       ham_is_pro() ? "pro " : "", maj, min, rev);

  if (!ham_is_pro())
    printf(
"This program is free software: you can redistribute it and/or modify\n"
"it under the terms of the GNU General Public License as published by\n"
"the Free Software Foundation, either version 3 of the License, or\n"
"(at your option) any later version.\n"
"\n"
"This program is distributed in the hope that it will be useful,\n"
"but WITHOUT ANY WARRANTY; without even the implied warranty of\n"
"MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the\n"
"GNU General Public License for more details.\n\n");
  else {
    time_t end = ham_is_pro_evaluation();
    if (end != 0)
      printf("Commercial evaluation version; valid till %s.\n", ctime(&end));
    else
      printf("Commercial version.\n\n");
  }
}

