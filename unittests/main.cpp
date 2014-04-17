/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "../src/config.h"

#define CATCH_CONFIG_RUNNER 1
#include "3rdparty/catch/catch.hpp"

#include "../src/error.h"
#ifdef HAM_ENABLE_REMOTE
#  include "../src/protobuf/protocol.h"
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
