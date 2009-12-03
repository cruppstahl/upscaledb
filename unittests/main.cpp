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

#include <stdexcept>
#include <ham/hamsterdb.hpp>

#include "bfc-testsuite.hpp"

#ifdef VISUAL_STUDIO
#include <windows.h>
#include <crtdbg.h>
#endif

#include "../src/error.h"


using namespace bfc;




#if (defined(WIN32) || defined(_WIN32) || defined(_WIN64) || defined(WIN64)) \
	&& defined(_DEBUG)

_CrtMemState crm_memdbg_state_snapshot1;
int trigger_memdump = 0;
int trigger_debugger = 0;

/*
 * Define our own reporting function.
 * We'll hook it into the debug reporting
 * process later using _CrtSetReportHook.
 */
static int 
crm_dbg_report_function(int report_type, char *usermsg, int *retval)
{
    /*
     * By setting retVal to zero, we are instructing _CrtDbgReport
     * to continue with normal execution after generating the report.
     * If we wanted _CrtDbgReport to start the debugger, we would set
     * retVal to one.
     */
    *retval = !!trigger_debugger;

    /*
     * When the report type is for an ASSERT,
     * we'll report some information, but we also
     * want _CrtDbgReport to get called -
     * so we'll return TRUE.
     *
     * When the report type is a WARNing or ERROR,
     * we'll take care of all of the reporting. We don't
     * want _CrtDbgReport to get called -
     * so we'll return FALSE.
     */
    switch (report_type)
    {
    default:
    case _CRT_WARN:
    case _CRT_ERROR:
    case _CRT_ERRCNT:
        fwrite(usermsg, 1, strlen(usermsg), stderr);
        fflush(stderr);
        return FALSE;

    case _CRT_ASSERT:
        fwrite(usermsg, 1, strlen(usermsg), stderr);
        fflush(stderr);
		break;
    }
    return TRUE;
}

static void 
crm_report_mem_analysis(void)
{
    _CrtMemState msNow;

    if (!_CrtCheckMemory())
    {
        fprintf(stderr, ">>>Failed to validate memory heap<<<\n");
    }

    /* only dump leaks when there are in fact leaks */
    _CrtMemCheckpoint(&msNow);

    if (msNow.lCounts[_CLIENT_BLOCK] != 0
        || msNow.lCounts[_NORMAL_BLOCK] != 0
        || (_crtDbgFlag & _CRTDBG_CHECK_CRT_DF
            && msNow.lCounts[_CRT_BLOCK] != 0)
    )
    {
        /* difference detected: dump objects since start. */
        _RPT0(_CRT_WARN, "============== Detected memory leaks! ====================\n");

	    _CrtMemState diff;
		if (_CrtMemDifference(&diff, &crm_memdbg_state_snapshot1, &msNow))
		{
	        //_CrtMemDumpAllObjectsSince(&crm_memdbg_state_snapshot1);

			_CrtMemDumpStatistics(&diff);
		}
    }
}


#endif

int 
main(int argc, char **argv)
{
#if 0
#if (defined(WIN32) || defined(_WIN32) || defined(_WIN64) || defined(WIN64)) \
        && defined(_DEBUG)
    /*
     * Hook in our client-defined reporting function.
     * Every time a _CrtDbgReport is called to generate
     * a debug report, our function will get called first.
     */
    _CrtSetReportHook(crm_dbg_report_function);

    /*
     * Define the report destination(s) for each type of report
     * we are going to generate.  In this case, we are going to
     * generate a report for every report type: _CRT_WARN,
     * _CRT_ERROR, and _CRT_ASSERT.
     * The destination(s) is defined by specifying the report mode(s)
     * and report file for each report type.
     */
    _CrtSetReportMode(_CRT_WARN, _CRTDBG_MODE_DEBUG);
    _CrtSetReportFile(_CRT_WARN, _CRTDBG_FILE_STDERR);
    _CrtSetReportMode(_CRT_ERROR, _CRTDBG_MODE_DEBUG);
    _CrtSetReportFile(_CRT_ERROR, _CRTDBG_FILE_STDERR);
    _CrtSetReportMode(_CRT_ASSERT, _CRTDBG_MODE_DEBUG);
    _CrtSetReportFile(_CRT_ASSERT, _CRTDBG_FILE_STDERR);

    // Store a memory checkpoint in the s1 memory-state structure
    _CrtMemCheckpoint(&crm_memdbg_state_snapshot1);

    atexit(crm_report_mem_analysis);

    // Get the current bits
    int i = _CrtSetDbgFlag(_CRTDBG_REPORT_FLAG);

	i |= _CRTDBG_ALLOC_MEM_DF;

    // Set the debug-heap flag so that freed blocks are kept on the
    // linked list, to catch any inadvertent use of freed memory
    i |= _CRTDBG_DELAY_FREE_MEM_DF;

    // Set the debug-heap flag so that memory leaks are reported when
    // the process terminates. Then, exit.
    i |= _CRTDBG_LEAK_CHECK_DF;

    // Clear the upper 16 bits and OR in the desired freqency
#if 0
	i = (i & 0x0000FFFF) | _CRTDBG_CHECK_EVERY_1024_DF;
#else
    i |= _CRTDBG_CHECK_ALWAYS_DF;
#endif

    // Set the new bits
    _CrtSetDbgFlag(i);

    // set a malloc marker we can use it in the leak dump at the end of 
    // the program:
//    (void)_calloc_dbg(1, 1, _CLIENT_BLOCK, __FILE__, __LINE__);
#endif
#endif // #if 0

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
#ifdef UNITTEST_PATH 
    SetCurrentDirectoryA(UNITTEST_PATH);
else
    SetCurrentDirectoryA("../unittests");
#endif
#endif

	// set up the testrunner rig:
#if 0 // turn this on (--> #if 01) to assist with debugging testcases: 
    // exceptions, etc. will pass through to your debugger
	testrunner::get_instance()->catch_coredumps(0);
	testrunner::get_instance()->catch_exceptions(0);
#else
	testrunner::get_instance()->catch_coredumps(0);
	testrunner::get_instance()->catch_exceptions(1);
#endif
#if (defined(WIN32) || defined(_WIN32) || defined(_WIN64) || defined(WIN64))
	testrunner::get_instance()->outputdir("./");
	testrunner::get_instance()->inputdir("./");
#endif

	// as we wish to print all collected errors at the very end, we act
	// as if we don't want the default built-in reporting, hence we MUST
	// call init_run() here.
	testrunner::get_instance()->init_run();
	unsigned int r;
	if (argc > 1)
	{
		std::string lead_fixture;
		std::string lead_test;
		bool lead = false;
		bool inclusive_begin = true;

		r = 0;
		for (int i = 1; i <= argc; i++)
		{
			std::string fixture_name;
			if (i < argc)
			{
				fixture_name = argv[i];
			}

			if (fixture_name == "*")
			{
				// lead or tail or chain?
				lead = true;
			}
			else
			{
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

				if (!lead && (i < argc)
					&& (i+1 >= argc || std::string(argv[i+1]) != "*"))
				{
					// single case:
					r = testrunner::get_instance()->run(
							fixture_name.c_str(), test_name.c_str(),
							false);
					inclusive_begin = true;
				}
				else if (lead)
				{
					r = testrunner::get_instance()->run(
							lead_fixture, lead_test,
							fixture_name, test_name,
							inclusive_begin,
							false,
							false);
					inclusive_begin = false;
				}
				lead_fixture = fixture_name;
				lead_test = test_name;
				lead = false;
			}
		}
	}
	else
	{
		r = testrunner::get_instance()->run(false);
	}
	testrunner::get_instance()->print_errors();
	testrunner::delete_instance();

    return (r);
}


