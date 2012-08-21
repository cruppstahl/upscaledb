/**
 * Copyright (C) 2005-2008 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

#include <stdexcept>
#include <cstring>
#include <cassert>
#include <ham/hamsterdb_int.h>
#include "../src/error.h"

#include "bfc-testsuite.hpp"
#include "hamster_fixture.hpp"

using namespace bfc;
using namespace ham;

static void HAM_CALLCONV
my_handler(int level, const char *msg)
{
    static int i=0;
    static const char *s[]={
        "hello world",
        "ham_verify test 1",
        "(none)",
        "hello world 42",
    };
    const char *p=strstr(msg, ": ");
    if (!p)
        return;
    p+=2;
    assert(0==::strcmp(s[i], p));
    i++;
}

static int g_aborted=0;

static void
my_abort_handler(void)
{
    g_aborted=1;
}

class ErrorTest : public hamsterDB_fixture
{
    define_super(hamsterDB_fixture);

public:
    ErrorTest()
        : hamsterDB_fixture("ErrorTest")
    {
        testrunner::get_instance()->register_fixture(this);
        BFC_REGISTER_TEST(ErrorTest, errorHandlerTest);
        BFC_REGISTER_TEST(ErrorTest, verifyTest);
    }

public:
    virtual void setup()
    {
        __super::setup();

        ham_set_errhandler(my_handler);
    }

    virtual void teardown()
    {
        __super::teardown();

        ham_set_errhandler(0);
    }

    void errorHandlerTest()
    {
        ham_trace(("hello world"));
        ham_set_errhandler(0);
        ham_log(("testing error handler - hello world\n"));
    }

    void verifyTest()
    {
        ham_test_abort=my_abort_handler;

        g_aborted=0;
        ham_verify(0);
        BFC_ASSERT_EQUAL(1, g_aborted);
        g_aborted=0;
        ham_verify(1);
        BFC_ASSERT_EQUAL(0, g_aborted);
        g_aborted=0;
        ham_verify(!"expr");
        BFC_ASSERT_EQUAL(1, g_aborted);
        ham_verify(!"expr");
        BFC_ASSERT_EQUAL(1, g_aborted);

        ham_test_abort=0;
    }
};

BFC_REGISTER_FIXTURE(ErrorTest);

