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

#ifndef HAMSTER_BFC_FIXTURE_HPP__
#define HAMSTER_BFC_FIXTURE_HPP__

#include "bfc-testsuite.hpp"
#include <ham/hamsterdb.h>
#include <assert.h>

/*
 * While this is a hamsterdb specific extension of the BFC fixture class,
 * park it in the BFC namespace nevertheless.
 *
 * It's just an act of utter laziness. ;-)
 */
namespace bfc
{

/*
 * __super is MSVC specific, but other compilers can simply offer the same
 * using this macro; place at the top of class definition and you're good
 * to go.
 */
#if !defined(_MSC_VER) || (_MSC_VER < 1310 /* MSVC.NET 2003 */)
#   define define_super(c) private: typedef c __super
#else
#   define define_super(c)
#endif


class hamsterDB_fixture: public fixture
{
    define_super(fixture);

public:
    hamsterDB_fixture(const char *name)
        : fixture(name)
    { }

    virtual ~hamsterDB_fixture()
    { }

    // make sure any hamsterdb-internal assertion failures are caught:
    virtual void setup()
    {
        __super::setup();

        ham_set_errhandler(hamster_dbghandler);
    }

    virtual void teardown()
    {
        __super::teardown();
    }

private:
    static void HAM_CALLCONV
    hamster_dbghandler(int level, const char *message)
    {
        std::cout << message << std::endl;
        if (level == HAM_DEBUG_LEVEL_FATAL)
        {
            assert(!message);
            throw bfc::error(__FILE__, __LINE__, NULL, NULL, "%s", message);
        }
    }
};


} // namespace bfc

#endif // HAMSTER_BFC_FIXTURE_HPP__
