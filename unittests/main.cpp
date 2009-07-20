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

#include "../src/config.h"

#include "bfc-testsuite.hpp"

#ifdef VISUAL_STUDIO
#   include <windows.h>
#endif

using namespace bfc;

testrunner *testrunner::s_instance=0;

int 
main(int argc, char **argv)
{
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
    SetCurrentDirectoryA("../unittests"); /* [i_a] */
#endif

	unsigned r;
	if (argc > 1)
	{
		r = 0;
		for (int i = 1; i < argc; i++)
		{
			std::string fixture_name = argv[i];
			size_t pos = fixture_name.find(':');
			std::string test_name;
			if (pos != std::string::npos)
			{
				test_name = fixture_name.substr(pos + 1);
				fixture_name = fixture_name.substr(0, pos);
				while ((pos = test_name.find(':')) != std::string::npos)
				{
					test_name.erase(pos, 1);
				}
			}
			r = testrunner::get_instance()->run(fixture_name.c_str(), 
                    test_name.c_str());
		}
	}
	else
	{
		r = testrunner::get_instance()->run();
	}
    delete testrunner::get_instance();

    return (r);
}
