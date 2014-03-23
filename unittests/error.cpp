/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 */

#include <string.h>
#include <assert.h>

#include "3rdparty/catch/catch.hpp"

#include "globals.h"

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

