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

/*
This file showcases a minimal BFC test fixture template.


expected BFC output on Win64:
---------------------------
----- error #1 in EmptyTest::Test1
..\..\unittests\bfc-testsuite.cpp:734 system exception occurred during executing
 the test code. The thread tried to read from or write to a virtual address for
which it does not have the appropriate access. (The thread attempted to read the
 inaccessible data at address $0000000000000000)
----- error #2 in EmptyTest::EmptyTest::Test2
..\..\unittests\empty_sample.cpp:58 assertion failed in expr 0 == 1
---------------------------
*/


#include "../src/config.h"

#include "bfc-testsuite.hpp"
#include "hamster_fixture.hpp"


using namespace bfc;



class EmptyTest : public fixture
{
	// call this macro here to define __super for all compilers:
	define_super(fixture);

public:
    EmptyTest(const char *name="EmptyTest")
    :   fixture(name)
    {
        clear_tests(); // don't inherit tests from the parent class hierarchy
        testrunner::get_instance()->register_fixture(this);
        BFC_REGISTER_TEST(EmptyTest, Test1);
        BFC_REGISTER_TEST(EmptyTest, Test2);
    }

protected:
    virtual void setup() 
	{ 
		__super::setup();

		// add your own setup code, which is run before each test invocation
    }
    
    virtual void teardown() 
	{ 
		__super::teardown();

		// add your own teardown code, which is always run after each test invocation (even when the test failed dramatically!)
    }

    void Test1(void)
    {
        BFC_ASSERT_EQUAL(0, 0);

#if 0 // turn on to see a SIGSEGV being caught by BFC: one failed test

		// cause a SIGSEGV (or something along those lines: a hardware trap)
		char *p = 0;
		char c = *p;
		BFC_ASSERT(c != 0);

#endif
	}

    void Test2(void)
    {
#if 0 // turn on to see an intentional validation failure occur: one failed test
		BFC_ASSERT_EQUAL(0, 1);
#endif
	}

};


BFC_REGISTER_FIXTURE(EmptyTest);


