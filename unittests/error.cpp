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

#include <string.h>
#include <assert.h>

#include "3rdparty/catch/catch.hpp"

#include "utils.h"

#include "../src/error.h"

static void HAM_CALLCONV
my_handler(int level, const char *msg) {
  static int i = 0;
  static const char *s[] = {
    "hello world",
    "ham_verify test 1",
    "(none)",
    "hello world 42",
  };
  const char *p = strstr(msg, ": ");
  if (!p)
    return;
  p += 2;
  assert(0 == ::strcmp(s[i], p));
  i++;
}

static int g_aborted = 0;

static void
my_abort_handler() {
  g_aborted = 1;
}

TEST_CASE("ErrorTest/handler",
           "Tests the error logging handler")
{
  ham_set_errhandler(my_handler);
  ham_trace(("hello world"));
  ham_set_errhandler(0);
  ham_log(("testing error handler - hello world\n"));
}

TEST_CASE("ErrorTest/verify",
           "Tests the ham_verify handler")
{
  ham_set_errhandler(my_handler);
  hamsterdb::ham_test_abort = my_abort_handler;

  g_aborted = 0;
  ham_verify(0);
  REQUIRE(1 == g_aborted);
  g_aborted = 0;
  ham_verify(1);
  REQUIRE(0 == g_aborted);
  g_aborted = 0;
  ham_verify(!"expr");
  REQUIRE(1 == g_aborted);
  ham_verify(!"expr");
  REQUIRE(1 == g_aborted);

  hamsterdb::ham_test_abort = 0;
  ham_set_errhandler(0);
}

