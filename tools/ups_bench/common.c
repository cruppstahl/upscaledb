/*
 * Copyright (C) 2005-2017 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * See the file COPYING for License information.
 */

#include <stdio.h>
#include <time.h>

#include <ups/upscaledb_int.h>

#include "common.h"

void
print_banner(const char *program_name)
{
  uint32_t maj, min, rev;
  ups_get_version(&maj, &min, &rev);

  printf("upscaledb %d.%d.%d - Copyright (C) 2005-2016 "
       "Christoph Rupp (chris@crupp.de).\n\n", maj, min, rev);

  printf(
"This program is free software: you can redistribute it and/or modify\n"
"it under the terms of the Apache Public License 2.0.\n"
"\n"
"This program is distributed in the hope that it will be useful,\n"
"but WITHOUT ANY WARRANTY; without even the implied warranty of\n"
"MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the\n"
"APL Apahe Public License for more details.\n\n");
}

