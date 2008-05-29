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

#include <cppunit/CompilerOutputter.h>
#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>

#ifdef VISUAL_STUDIO
#   include <windows.h>
#endif

int 
main(int argc, char **argv)
{
    (void)argc;
    (void)argv;

    CppUnit::Test *suite=CppUnit::TestFactoryRegistry::getRegistry().makeTest();

    CppUnit::TextUi::TestRunner runner;
    runner.addTest(suite);

    runner.setOutputter(new CppUnit::CompilerOutputter(&runner.result(), 
                    std::cerr));

    /*
     * when running in visual studio, the working directory is different
     * from the unix/cygwin environment. this can be changed, but the
     * working directory setting is not stored in the unittests.vcproj file, 
     * but in unittests.vcproj.<hostname><username>; and this file is not
     * distributed.
     *
     * therefore, at runtime, if we're compiling under visual studio, set
     * the working directory manually.
     */
#ifdef VISUAL_STUDIO
    SetCurrentDirectory(L"../unittests");
#endif

    return runner.run() ? 0 : 1;
}
