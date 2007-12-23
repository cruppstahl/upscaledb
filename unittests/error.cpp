/**
 * Copyright (C) 2005-2007 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See file COPYING.GPL2 and COPYING.GPL3 for License information.
 */

#include <stdexcept>
#include <cppunit/extensions/HelperMacros.h>
#include <ham/hamsterdb_int.h>
#include "memtracker.h"
#include "../src/error.h"

static void
my_handler(const char *msg)
{
    static int i=0;
    static const char *s[]={
        "hello world",
        "ham_verify test 1",
        "(none)",
        "hello world 42",
    };
    const char *p=strstr(msg, ": ")+2;
    CPPUNIT_ASSERT_EQUAL(0, ::strcmp(s[i], p));
    i++;
}

static int g_aborted=0;

static void
my_abort_handler(void)
{ 
    g_aborted=1;
}

class ErrorTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(ErrorTest);
    CPPUNIT_TEST      (errorHandlerTest);
    CPPUNIT_TEST      (verifyTest);
    CPPUNIT_TEST_SUITE_END();

public:
    void setUp()
    { 
    }
    
    void tearDown() 
    { 
    }

    void errorHandlerTest()
    {
        ham_set_errhandler(my_handler);
        ham_log(("hello world"));
        ham_set_errhandler(0);
        ham_log(("testing error handler - hello world\n"));
    }

    void verifyTest()
    {
        ham_test_abort=my_abort_handler;

        g_aborted=0;
        ham_verify(0, ("ham_verify test 1"));
        CPPUNIT_ASSERT_EQUAL(1, g_aborted);
        g_aborted=0;
        ham_verify(1, ("ham_verify test 2"));
        CPPUNIT_ASSERT_EQUAL(0, g_aborted);
        g_aborted=0;
        ham_verify(!"expr", (0));
        CPPUNIT_ASSERT_EQUAL(1, g_aborted);
        ham_verify(!"expr", ("hello world %d", 42));
        CPPUNIT_ASSERT_EQUAL(1, g_aborted);

        ham_test_abort=abort;
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(ErrorTest);

