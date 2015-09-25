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

#include <string.h>
#include <assert.h>

#include "3rdparty/catch/catch.hpp"

#include "utils.h"

#include "1base/error.h"

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

