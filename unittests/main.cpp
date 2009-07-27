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

using namespace bfc;

testrunner *testrunner::s_instance=0;



#if (defined(WIN32) || defined(_WIN32) || defined(_WIN64) || defined(WIN64)) && defined(_DEBUG)

static int crm_dbg_report_function(int reportType, char *userMessage, 
        int *retVal);
static void crm_report_mem_analysis(void);

#else

static void crm_report_mem_analysis(void)
{
}

#endif

#if (defined(WIN32) || defined(_WIN32) || defined(_WIN64) || defined(WIN64)) 
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
        fputs(usermsg, stderr);
        fflush(stderr);
        return FALSE;

    case _CRT_ASSERT:
        fputs(usermsg, stderr);
        fflush(stderr);
		break;
    }
    return TRUE;
}

void 
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

#if (defined(WIN32) || defined(_WIN32) || defined(_WIN64) || defined(WIN64)) 
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

    // Set the debug-heap flag so that freed blocks are kept on the
    // linked list, to catch any inadvertent use of freed memory
#if 0
    i |= _CRTDBG_DELAY_FREE_MEM_DF;
#endif

    // Set the debug-heap flag so that memory leaks are reported when
    // the process terminates. Then, exit.
    i |= _CRTDBG_LEAK_CHECK_DF;

    // Clear the upper 16 bits and OR in the desired freqency
    //i = (i & 0x0000FFFF) | _CRTDBG_CHECK_EVERY_16_DF;

    i |= _CRTDBG_CHECK_ALWAYS_DF;

    // Set the new bits
    _CrtSetDbgFlag(i);

      // set a malloc marker we can use it in the leak dump at the end of 
      // the program:
//    (void)_calloc_dbg(1, 1, _CLIENT_BLOCK, __FILE__, __LINE__);
#endif

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
#if 1 // [i_a] given my own build env, this directory changes as well
    SetCurrentDirectoryA("../unittests"); /* [i_a] */
#else
	// .\win32\msvc2008\bin\Win32_MSVC2008.Debug -> .\unittests
    SetCurrentDirectoryA("../../../../unittests"); /* [i_a] */
#endif
#endif

	// set up the testrunner rig:
#if 01 // turn this on (--> #if 01) to assist with debugging testcases: 
    // exceptions, etc. will pass through to your debugger
	testrunner::get_instance()->catch_coredumps(0);
	testrunner::get_instance()->catch_exceptions(0);
#else
	testrunner::get_instance()->catch_coredumps(1);
	testrunner::get_instance()->catch_exceptions(1);
#endif
#if (defined(WIN32) || defined(_WIN32) || defined(_WIN64) || defined(WIN64))
	testrunner::get_instance()->outputdir("./");
	testrunner::get_instance()->inputdir("./");
#endif

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

				if (!lead 
					&& (i+1 == argc || std::string(argv[i+1]) != "*"))
				{
					// single case:
					r = testrunner::get_instance()->run(
							fixture_name.c_str(), test_name.c_str());
					inclusive_begin = true;
				}
				else if (lead)
				{
					r = testrunner::get_instance()->run(
							lead_fixture, lead_test,
							fixture_name, test_name,
							inclusive_begin);
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
		r = testrunner::get_instance()->run();
	}
    testrunner::delete_instance();

    return (r);
}


class EmptyTest : public fixture
{
public:
    EmptyTest(const char *name=0)
    :   fixture(name ? name : "EmptyTest")
    {
        if (name)
            return;
        testrunner::get_instance()->register_fixture(this);
        BFC_REGISTER_TEST(EmptyTest, Test1);
    }

protected:
    void setup()
    { 
    }
    
    void teardown() 
    { 
    }

    void Test1(void)
    {
        BFC_ASSERT_EQUAL(0, 0);
    }

};


BFC_REGISTER_FIXTURE(EmptyTest);

/*
 * Make sure we catch C++ exceptions before we fall into the
 * SEH system exception handling pit.
 * 
 * To prevent those C++ exceptions from falling in there, we
 * need to provide a soft landing: report when/if a C++ EX 
 * occurred and pass along the EH details to the caller.
 */
bool 
testrunner::cpp_eh_run(testrunner *me, fixture *f, method m, 
        const char *funcname, error &ex)
{
	if (me->m_catch_exceptions || me->m_catch_coredumps)
	{
		try 
		{
			return f->FUT_invoker(me, m, funcname, ex);
		}
		catch (error &e)
		{
			ex = e;
			return true;
		}
	}
	else
	{
		return f->FUT_invoker(me, m, funcname, ex);
	}
}

/*
 * This function must not contain any object instantions because 
 * otherwise MSVC will complain loudly then:
 * error C2712: Cannot use __try in functions that require object unwinding
 */
bool 
testrunner::exec_testfun(testrunner *me, fixture *f, method m, 
        const char *funcname, error &ex)
{
	bool throw_ex = false;

	if (me->m_catch_coredumps)
	{
#if defined(_MSC_VER)
		EXCEPTION_RECORD er;

		__try
		{
			throw_ex = cpp_eh_run(me, f, m, funcname, ex);
		}
		__except(is_hw_exception(GetExceptionCode(), 
                GetExceptionInformation(), &er))
		{
			cvt_hw_ex_as_cpp_ex(&er, me, f, m, funcname, ex);
			throw_ex = true;
		}	
#else
		throw_ex = cpp_eh_run(me, f, m, funcname, ex);
#endif
	}
	else
	{
		throw_ex = cpp_eh_run(me, f, m, funcname, ex);
	}

	return throw_ex;
}

#if defined(_MSC_VER)

void 
testrunner::cvt_hw_ex_as_cpp_ex(const EXCEPTION_RECORD *e, testrunner *me, 
        const fixture *f, method m, const char *funcname, error &err)
{
	unsigned int code = e->ExceptionCode;
	const char *msg = NULL;
	char msgbuf[256];

	msgbuf[0] = 0; // mark as (yet) unused
	switch (code)
	{
	case EXCEPTION_ACCESS_VIOLATION:
		msg = "The thread tried to read from or write to a virtual address "
              "for which it does not have the appropriate access.";
		/*
		 * The first element of the array contains a read-write flag
		 * that indicates the type of operation that caused the access
		 * violation. If this value is zero, the thread attempted to
		 * read the inaccessible data. If this value is 1, the thread
		 * attempted to write to an inaccessible address. If this
		 * value is 8, the thread causes a user-mode data execution
		 * prevention (DEP) violation.
         * 
		 * The second array element specifies the virtual address of
		 * the inaccessible data.
		 */
		if (e->NumberParameters >= 2)
		{
			const char *cause_msg;

			switch ((int)e->ExceptionInformation[0])
			{
			case 0:
				cause_msg = "The thread attempted to read the inaccessible data ";
				break;

			case 1:
				cause_msg = "The thread attempted to write to an inaccessible address ";
				break;

			case 8:
				cause_msg = "The thread causes a user-mode data execution prevention (DEP) violation ";
				break;

			default:
				cause_msg = "";
				break;
			}
			_snprintf(msgbuf, sizeof(msgbuf), "%s (%sat address $%p)",
					  msg, cause_msg,
					  (void *)e->ExceptionInformation[1]);
			msg = msgbuf;
		}
		break;

	case EXCEPTION_DATATYPE_MISALIGNMENT:
		msg =  "The thread tried to read or write data that is misaligned on hardware that does not provide alignment. For example, 16-bit values must be aligned on 2-byte boundaries; 32-bit values on 4-byte boundaries, and so on.";
		break;

	case EXCEPTION_BREAKPOINT:
		msg =  "A breakpoint was encountered.";
		break;

	case EXCEPTION_SINGLE_STEP:
		msg =  "A trace trap or other single-instruction mechanism signaled that one instruction has been executed.";
		break;

	case EXCEPTION_ARRAY_BOUNDS_EXCEEDED:
		msg =  "The thread tried to access an array element that is out of bounds and the underlying hardware supports bounds checking.";
		break;

	case EXCEPTION_FLT_DENORMAL_OPERAND:
		msg =  "One of the operands in a floating-point operation is denormal. A denormal value is one that is too small to represent as a standard floating-point value.";
		break;

	case EXCEPTION_FLT_DIVIDE_BY_ZERO:
		msg =  "The thread tried to divide a floating-point value by a floating-point divisor of zero.";
		break;

	case EXCEPTION_FLT_INEXACT_RESULT:
		msg =  "The result of a floating-point operation cannot be represented exactly as a decimal fraction.";
		break;

	case EXCEPTION_FLT_INVALID_OPERATION:
		msg =  "This exception represents any floating-point exception not included in this list.";
		break;

	case EXCEPTION_FLT_OVERFLOW:
		msg =  "The exponent of a floating-point operation is greater than the magnitude allowed by the corresponding type.";
		break;

	case EXCEPTION_FLT_STACK_CHECK:
		msg =  "The stack overflowed or underflowed as the result of a floating-point operation.";
		break;

	case EXCEPTION_FLT_UNDERFLOW:
		msg =  "The exponent of a floating-point operation is less than the magnitude allowed by the corresponding type.";
		break;

	case EXCEPTION_INT_DIVIDE_BY_ZERO:
		msg =  "The thread tried to divide an integer value by an integer divisor of zero.";
		break;

	case EXCEPTION_INT_OVERFLOW:
		msg =  "The result of an integer operation caused a carry out of the most significant bit of the result.";
		break;

	case EXCEPTION_PRIV_INSTRUCTION:
		msg =  "The thread tried to execute an instruction whose operation is not allowed in the current machine mode.";
		break;

	case EXCEPTION_IN_PAGE_ERROR:
		msg =  "The thread tried to access a page that was not present, and the system was unable to load the page. For example, this exception might occur if a network connection is lost while running a program over the network.";
		/*
		The first element of the array contains a read-write flag
		that indicates the type of operation that caused the access
		violation. If this value is zero, the thread attempted to
		read the inaccessible data. If this value is 1, the thread
		attempted to write to an inaccessible address. If this
		value is 8, the thread causes a user-mode data execution
		prevention (DEP) violation.

		The second array element specifies the virtual address of
		the inaccessible data.

		The third array element specifies the underlying NTSTATUS
		code that resulted in the exception.
		*/
		if (e->NumberParameters >= 3)
		{
			const char *cause_msg;

			switch ((int)e->ExceptionInformation[0])
			{
			case 0:
				cause_msg = "The thread attempted to read the inaccessible data ";
				break;

			case 1:
				cause_msg = "The thread attempted to write to an inaccessible address ";
				break;

			case 8:
				cause_msg = "The thread causes a user-mode data execution prevention (DEP) violation ";
				break;

			default:
				cause_msg = "";
				break;
			}
			_snprintf(msgbuf, sizeof(msgbuf), "%s (%sat address $%p, NT STATUS = $%08X (%u))",
					  msg, cause_msg,
					  (void *)e->ExceptionInformation[1],
					  (unsigned int)e->ExceptionInformation[2],
					  (unsigned int)e->ExceptionInformation[2]);
			msg = msgbuf;
		}
		break;

	case EXCEPTION_ILLEGAL_INSTRUCTION:
		msg =  "The thread tried to execute an invalid instruction.";
		break;

	case EXCEPTION_NONCONTINUABLE_EXCEPTION:
		msg =  "The thread tried to continue execution after a noncontinuable exception occurred.";
		break;

	case EXCEPTION_STACK_OVERFLOW:
		msg =  "The thread used up its stack.";
		break;

	case EXCEPTION_INVALID_DISPOSITION:
		msg =  "An exception handler returned an invalid disposition to the exception dispatcher. Programmers using a high-level language such as C should never encounter this exception.";
		break;

	case EXCEPTION_GUARD_PAGE:
		msg =  "EXCEPTION_GUARD_PAGE";
		break;

	case EXCEPTION_INVALID_HANDLE:
		msg =  "EXCEPTION_INVALID_HANDLE";
		break;

#if defined(STATUS_POSSIBLE_DEADLOCK)
	case EXCEPTION_POSSIBLE_DEADLOCK:
		msg =  "EXCEPTION_POSSIBLE_DEADLOCK";
		break;
#endif

	case CONTROL_C_EXIT:
		msg = "CTRL+C is input.";
		break;

	case DBG_CONTROL_C:
		msg = "CTRL+C is input to this console process that handles CTRL+C signals and is being debugged.";
		break;

	default:
		_snprintf(msgbuf, sizeof(msgbuf), "Unidentified system exception $%08X (%u) has been raised.", code, code);
		msg = msgbuf;
		break;
	}

	if (!msgbuf[0] && e->ExceptionAddress)
	{
		_snprintf(msgbuf, sizeof(msgbuf), "%s (at address $%p)",
				  msg, (void *)e->ExceptionAddress);
		msg = msgbuf;
	}

	char buf[2048];
	_snprintf(buf, sizeof(buf), "system exception occurred during executing the test code. %s", msg);

	err = error(__FILE__, __LINE__, f->get_name(), funcname, buf);
}



int testrunner::is_hw_exception(unsigned int code, struct _EXCEPTION_POINTERS *ep, EXCEPTION_RECORD *dst)
{
	/* copy exception info for future reference/use */
	if (ep && ep->ExceptionRecord)
	{
		*dst = ep->ExceptionRecord[0];
	}
	else
	{
		memset(dst, 0, sizeof(*dst));
	}
	dst->ExceptionCode = code;

	switch (code)
	{
	case EXCEPTION_ACCESS_VIOLATION:
	case EXCEPTION_DATATYPE_MISALIGNMENT:
	case EXCEPTION_BREAKPOINT:
	case EXCEPTION_SINGLE_STEP:
	case EXCEPTION_ARRAY_BOUNDS_EXCEEDED:
	case EXCEPTION_FLT_DENORMAL_OPERAND:
	case EXCEPTION_FLT_DIVIDE_BY_ZERO:
	case EXCEPTION_FLT_INEXACT_RESULT:
	case EXCEPTION_FLT_INVALID_OPERATION:
	case EXCEPTION_FLT_OVERFLOW:
	case EXCEPTION_FLT_STACK_CHECK:
	case EXCEPTION_FLT_UNDERFLOW:
	case EXCEPTION_INT_DIVIDE_BY_ZERO:
	case EXCEPTION_INT_OVERFLOW:
	case EXCEPTION_PRIV_INSTRUCTION:
	case EXCEPTION_IN_PAGE_ERROR:
	case EXCEPTION_ILLEGAL_INSTRUCTION:
	case EXCEPTION_NONCONTINUABLE_EXCEPTION:
	case EXCEPTION_STACK_OVERFLOW:
	case EXCEPTION_INVALID_DISPOSITION:
	case EXCEPTION_GUARD_PAGE:
	case EXCEPTION_INVALID_HANDLE:
#if defined(STATUS_POSSIBLE_DEADLOCK)
	case EXCEPTION_POSSIBLE_DEADLOCK:
#endif
		return EXCEPTION_EXECUTE_HANDLER;

	case CONTROL_C_EXIT:
	case DBG_CONTROL_C:
		return EXCEPTION_CONTINUE_SEARCH;

	default:
		return EXCEPTION_EXECUTE_HANDLER;
	}
}

#endif


static void mk_abs_path(std::string &path, const std::string &basedir, const char *relative_filepath)
{
	bool is_abs_path = false;

	path = relative_filepath;
#if defined(_MSC_VER)
	for (size_t i = 0; i < path.size(); i++)
	{
		if (path[i] == '\\')
		{
			path[i] = '/';
		}
	}
	is_abs_path = (path.find(":/") != std::string::npos);
#endif
	is_abs_path |= (path[0] == '/');
	if (!is_abs_path)
	{
		path = basedir + path;
	}
}

std::string testrunner::expand_inputpath(const char *relative_filepath)
{
	testrunner *t = testrunner::get_instance();
	std::string path;
	mk_abs_path(path, t->m_inputdir, relative_filepath);
	return path;
}

std::string testrunner::expand_outputpath(const char *relative_filepath)
{
	testrunner *t = testrunner::get_instance();
	std::string path;
	mk_abs_path(path, t->m_outputdir, relative_filepath);
	return path;
}


