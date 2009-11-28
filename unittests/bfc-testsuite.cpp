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

/* #include "../src/config.h"    - not an integral part of hamster but rather the, ah, 'platform independent' BFC */

#include <stdexcept>

#include "bfc-testsuite.hpp"

#ifdef VISUAL_STUDIO
#include <windows.h>
#include <crtdbg.h>
#endif
#include <signal.h> /* the signal catching / hardware exception catching stuff for UNIX (and a bit for Win32/64 too) */
#include <errno.h>
#include <string.h>
#include <assert.h>

#include "bfc-signal.h"


using namespace bfc;

testrunner *testrunner::s_instance=0;

testrunner::bfc_signal_context_t testrunner::m_current_signal_context;






/*
   For a complete run-down on UNIX [hardware] exception handling intricacies, see
   [APitUE], pp. 263..324, Chapter 10.

   NOTE THAT WE KNOWINGLY TAKE SEVERAL SHORTCUTS IN THIS IMPLEMENTATION, CUTTING A FEW 
   DANGEROUS CORNERS REGARDING QUEUED UNRELIABLE & RELIABLE SIGNALS HERE. However,
   we feel this is permissible for two reasons:
   
   1) the signals we catch/handle here, all assume some type of failure 
	   occurring within the Function-Under-Test (or it's accompanying fixture
	   setup or teardown code), WHILE WE ASSUME THAT THE BFC FRAMEWORK ITSELF WILL
	   *NOT* RAISE THESE (FAILURE) SIGNALS. As such, we can treat unreliable 
	   signals as if they are reliable as we hence assume these (failure) signals 
	   only occur _once_; events like the MC68K double-bus error would only be 
	   possible if our signal-handling code itself would be flaky. ;-)

   2) This is rather a non-reason, but yet here it is: we would have coded 
	   this in a more conservative manner if such would be doable without 
	   additional, significant GNU configure / etc. code portability 
	   configuration effort. By chosing the path of the Lowest Common Denominator
	   here, we introduce an implicit requirement for BFC and some risk as well:
   
   2a) FUTs which come with their own signal setup/teardown code, may do so,
	   but this MAY clash with our 'rig' here. When you've got FUTs/fixtures like
	   that, YOU ARE IMPLICITLY ASSUMED TO KNOW WHAT YOU ARE DOING. In other
	   words: Caveat Emptor.
   
	   (Hint: you may wish to #define 
				BFC_HAS_CUSTOM_SIGNAL_SETUP
	   in your project to disable this default implementation.)

   2b) The current signal handling implementation is not suitable for a 
	   multi-threaded testing environment: the current implementation assumes
	   only a single testrunner instance exists at any time, while any fixture-
	   or FUT code runs in a single thread.

   2c) The current implementation does not unblock / dequeue multiple, near-simultaneous
       occurrences of the signals we deign to catch. More specifically, we 
	   do not use sigsetjmp() / siglongjmp() to unblock pending signals, though 
	   in the face of using C++ here (nobody says 'throw' unblocks the pending signal: as
	   such it has similar issues as longjmp() ([APitUE], pp. 209..309)), we try to
	   resolve this (and thus act like we'd be using sigsetjmp()/siglongjmp() in a 
	   crude fashion) by calling 
			sigprocmask(SIG_UNBLOCK, ...)
	   before throwing a bfc::error C++ exception, if the sigprocmask() function is 
	   guestimated to be available on your platform (i.e. if the SIG_UNBLOCK constant is
	   #define'd).

   IMPLEMENTATION SPECIFIC NOTES:

   The signal handler is assumed to be invoked only while inside the Function Under Test.
   However, the code is a little (over-?)conservative in that is will catch bfc:error
   C++ exceptions thrown from inside this signal handler from any point in the run-time
   flow from the moment the signal handler has been set up: this is the reason for the 
   extra, hopefully superfluous, try..catch in testrunner::

   References: 

   [APitUE] W. Richard Stevens [R.I.P.], Advanced Programming in the UNIX Environment, Addison-Wesley, ISBN 0-201-56317-7, 10th printing (1995)

*/   
const int testrunner::m_signals_to_catch[] =
{ 
#if defined(SIGINT)
	// SIGINT,
#endif
	SIGILL,
#if defined(SIGEMT)
	SIGEMT,
#endif
#if defined(SIGIOT)
	SIGIOT,
#endif
#if defined(SIGBUS)
	SIGBUS,
#endif
#if defined(SIGSYS)
	SIGSYS,
#endif
#if defined(SIGPIPE)
	//SIGPIPE,
#endif
	SIGSEGV,
#if defined(SIGTERM)
	SIGTERM,
#endif
#if defined(SIGBREAK)
	// SIGBREAK, /* Ctrl-Break sequence */
#endif
	SIGABRT,
#if defined(SIGABRT_COMPAT)
	SIGABRT_COMPAT, // MSVC: /* SIGABRT compatible with other platforms, same as SIGABRT -- but not the same value! */
#endif
#if defined(SIGQUIT)
	// SIGQUIT,
#endif
#if defined(SIGXCPU)
	SIGXCPU, /* UNIX: CPU time limit exceeded */
#endif
#if defined(SIGXFSZ)
	SIGXFSZ, /* UNIX: file size limit exceeded */
#endif
	SIGFPE, /* floating point exception; MSVC: must call _fpreset() */

	0 // sentinel
};

int testrunner::BFC_universal_signal_handler(int signal_code, int sub_code)
{
	bool may_throw = (!m_current_signal_context.error_set && m_current_signal_context.sig_handlers_set);

	assert(m_current_signal_context.sig_handlers_set);
	assert(m_current_signal_context.this_is_me == testrunner::get_instance());

	// when we get here, something went pear shaped in the test. Throw an 
	// appropriate bfc:error to signal this!
	//
	// But BEFORE we do that, we should unblock this particular signal, as
	// throw/try/catch will not unblock pending signals (compare with [APitUE],
	// ch. 10, longjmp()/setjmp() vs. siglongjmp()/sigsetjmp(): what we're
	// trying to do here is emulate siglongjmp() in a portable C++ way, at least
	// so it suits our purposes within the BFC test rig.
	if (may_throw)
	{
		bfc::error ex(__FILE__,
			__LINE__,
			m_current_signal_context.current_error.m_fixture_name,
			m_current_signal_context.current_error.m_test,
			"SIGNAL RAISED: signal %d (%s)",
			signal_code, bfc_sigdescr(signal_code));

		m_current_signal_context.current_error = ex;

		// mark that we've thrown an exception, so we don't do so recursively while
		// the signals fly around ;-)
		m_current_signal_context.error_set = true;

#if defined(SIG_UNBLOCK) && !defined(OS_HAS_NO_SIGPROCMASK)
		sigset_t n;
		sigset_t o;
		sigemptyset(&n);
		sigemptyset(&o);
		sigaddset(&n, signal_code);
		sigprocmask(SIG_UNBLOCK, &n, &o); // we don't mind receiving another signal now.
#endif

#if 10
		std::cerr << "GENERAL FAILURE: " << m_current_signal_context.current_error.m_message.c_str() << may_throw << std::endl;
#endif
	
		longjmp(m_current_signal_context.signal_return_point, 2);
#if 0
		throw ex;
#endif
	}

#if defined(SIG_UNBLOCK) && !defined(OS_HAS_NO_SIGPROCMASK)
	sigset_t n;
	sigset_t o;
	sigemptyset(&n);
	sigemptyset(&o);
	sigaddset(&n, signal_code);
	sigprocmask(SIG_UNBLOCK, &n, &o); // we don't mind receiving another signal now.
#endif

	return 1;
}


const char *testrunner::bfc_sigdescr(int signal_code)
{
	// some platforms have sys_siglist[], but not all, so roll our own specific list here:
	switch (signal_code)
	{
#if defined(SIGINT)
	case SIGINT:
		return "SIGINT";
#endif
	case SIGILL:
		return "SIGILL";
#if defined(SIGEMT)
	case SIGEMT:
		return "SIGEMT";
#endif
#if defined(SIGIOT)
#if (SIGIOT != SIGABRT) // Ubuntu/Linux has SIGABRT==SIGIOT
	case SIGIOT:
		return "SIGIOT";
#endif
#endif
#if defined(SIGBUS)
	case SIGBUS:
		return "SIGBUS";
#endif
#if defined(SIGSYS)
	case SIGSYS:
		return "SIGSYS";
#endif
#if defined(SIGPIPE)
	case SIGPIPE:
		return "SIGPIPE";
#endif
	case SIGSEGV:
		return "SIGSEGV";
#if defined(SIGTERM)
	case SIGTERM:
		return "SIGTERM";
#endif
#if defined(SIGBREAK)
	case SIGBREAK:
		return "SIGBREAK";
#endif
	case SIGABRT:
		return "SIGABRT";
#if defined(SIGABRT_COMPAT)
	case SIGABRT_COMPAT:
		return "SIGABRT_COMPAT";
#endif
#if defined(SIGQUIT)
	case SIGQUIT:
		return "SIGQUIT";
#endif
#if defined(SIGXCPU)
	case SIGXCPU:
		return "SIGXCPU";
#endif
#if defined(SIGXFSZ)
	case SIGXFSZ:
		return "SIGXFSZ";
#endif
	case SIGFPE:
		return "SIGFPE";

	default:
		break;
	}
	return "(unidentified)";
}


#if 0 // this code MUST be compiled in *C* mode: it's available in bfc_signal.c
extern "C" 
{
	/*
	Use this *C* function to prevent compiler errors due to different
	function types being passed around here.

	We know what we're doing here. ;-)
	*/
	static signal_handler_f bfc_signal(int code, signal_handler_f handler)
	{
		return (signal_handler_f)signal(code, handler);
	}
}
#endif

bool testrunner::setup_signal_handlers(testrunner *me, const fixture *f, method m, const char *funcname, bfc_state_t sub_state, error &err)
{
	bool threw_ex = false;

	assert(m_current_signal_context.this_is_me == me);
	assert(m_current_signal_context.active_fixture == f);
#if 0
	assert(m_current_signal_context.active_method == m);
	assert(m_current_signal_context.active_funcname.compare(funcname ? funcname : "") == 0);
#endif
	m_current_signal_context.active_method = m;
	m_current_signal_context.active_funcname = (funcname ? funcname : "");
	m_current_signal_context.active_state = sub_state;
	//m_current_signal_context.print_err_report = BFC_QUIET;

	if (!m_current_signal_context.sig_handlers_set
		&& (sub_state & BFC_STATE_BEFORE))
	{
		// drop marker of previous errors: it's a new test we're starting here
		m_current_signal_context.error_set = false;
		m_current_signal_context.current_error = err;

		for (int i = 0; m_signals_to_catch[i] != 0; i++)
		{
			assert(i < int(sizeof(m_current_signal_context.old_sig_handlers) 
							/ sizeof(m_current_signal_context.old_sig_handlers[0]))); 
			m_current_signal_context.old_sig_handlers[i].handler = bfc_signal(m_signals_to_catch[i], BFC_universal_signal_handler);
			if (m_current_signal_context.old_sig_handlers[i].handler == (signal_handler_f)SIG_ERR)
			{
				err = bfc::error(__FILE__, __LINE__, f->get_name(), funcname, 
							"BFC cannot set up the signal handler %d (%s) : %d (%s)", 
							m_signals_to_catch[i], bfc_sigdescr(m_signals_to_catch[i]),
							errno, strerror(errno));
				threw_ex = true;
				break;
			}
		}
		m_current_signal_context.sig_handlers_set = true;
	}
	else if (m_current_signal_context.sig_handlers_set
		&& (sub_state & BFC_STATE_AFTER))
	{
		// make sure we're decommisioning any custom signal handler as we are leaving FUT invocation scope!
		for (int i = 0; m_signals_to_catch[i] != 0; i++)
		{
			assert(i < int(sizeof(m_current_signal_context.old_sig_handlers) 
							/ sizeof(m_current_signal_context.old_sig_handlers[0]))); 
			// restore originalm signal handler:
			if (bfc_signal(m_signals_to_catch[i], m_current_signal_context.old_sig_handlers[i].handler) == (signal_handler_f)SIG_ERR)
			{
				err = bfc::error(__FILE__, __LINE__, f->get_name(), funcname, 
							"BFC cannot unwind/restore the signal handler %d (%s) : %d (%s)", 
							m_signals_to_catch[i], bfc_sigdescr(m_signals_to_catch[i]),
							errno, strerror(errno));
				threw_ex = true;
				break;
			}
		}
		m_current_signal_context.sig_handlers_set = false;
	}

	return threw_ex;
}



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
        const char *funcname, bfc_state_t state, error &ex)
{
	if (me->m_catch_exceptions || me->m_catch_coredumps)
	{
		try 
		{
			return f->FUT_invoker(me, m, funcname, state, ex);
		}
		catch (bfc::error &e)
		{
			ex = e;
			return true;
		}
		// <stdexcept exception types are caught here as well: */
		catch (std::domain_error &e)
		{
			ex.m_message = "std::domain_error exception: ";
			ex.m_message += e.what();
			return true;
		}
		catch (std::invalid_argument &e)
		{
			ex.m_message = "std::invalid_argument exception: ";
			ex.m_message += e.what();
			return true;
		}
		catch (std::length_error &e)
		{
			ex.m_message = "std::length_error exception: ";
			ex.m_message += e.what();
			return true;
		}
		catch (std::out_of_range &e)
		{
			ex.m_message = "std::out_of_range exception: ";
			ex.m_message += e.what();
			return true;
		}
		catch (std::logic_error &e)
		{
			ex.m_message = "std::logic_error exception: ";
			ex.m_message += e.what();
			return true;
		}
		catch (std::overflow_error &e)
		{
			ex.m_message = "std::overflow_error exception: ";
			ex.m_message += e.what();
			return true;
		}
		catch (std::underflow_error &e)
		{
			ex.m_message = "std::underflow_error exception: ";
			ex.m_message += e.what();
			return true;
		}
		catch (std::range_error &e)
		{
			ex.m_message = "std::range_error exception: ";
			ex.m_message += e.what();
			return true;
		}
		catch (std::runtime_error &e)
		{
			ex.m_message = "std::runtime_error exception: ";
			ex.m_message += e.what();
			return true;
		}
	}
	else
	{
		return f->FUT_invoker(me, m, funcname, state, ex);
	}
}

/*
 * This function must not contain any object instantions because 
 * otherwise MSVC will complain loudly then:
 * error C2712: Cannot use __try in functions that require object unwinding
 */
bool 
testrunner::exec_testfun(testrunner *me, fixture *f, method m, 
        const char *funcname, bfc_state_t state, error &ex)
{
	bool threw_ex = false;

	if (me->m_catch_coredumps)
	{
		/*
		We know that using setjmp()/longjmp() (or sigsetjmp()/siglongjmp())
		destroys our C++ stack unwinding, so we WILL loose quite a few
		C++ destructors and related cleanup in the methods invoked from here,
		but this is a desperate measure in a desperate time.
		All we want is get a somewhat decent error report out there before
		we go belly-up all the way.
		*/
		if (!setjmp(m_current_signal_context.signal_return_point))
		{
			threw_ex = setup_signal_handlers(me, f, m, funcname, 
							bfc_state_t((state & BFC_STATE_MAJOR_STATE_MASK) | BFC_STATE_BEFORE), 
							ex);

			if (!threw_ex)
			{
#if defined(_MSC_VER)
				EXCEPTION_RECORD er;

				__try
				{
					threw_ex = cpp_eh_run(me, f, m, funcname, state, ex);
				}
				__except(is_hw_exception(GetExceptionCode(), 
						GetExceptionInformation(), &er))
				{
					cvt_hw_ex_as_cpp_ex(&er, me, f, m, funcname, ex);
					
					std::cout << ex.m_message << std::endl;
					
					threw_ex = true;
				}	
#else
				threw_ex = cpp_eh_run(me, f, m, funcname, state, ex);
#endif
			}

			threw_ex |= setup_signal_handlers(me, f, m, funcname, 
							bfc_state_t((state & BFC_STATE_MAJOR_STATE_MASK) | BFC_STATE_AFTER), 
							ex);
		}
		else
		{
			// longjmp() out of a raised signal handler:
			setup_signal_handlers(me, f, m, funcname, 
							bfc_state_t((state & BFC_STATE_MAJOR_STATE_MASK) | BFC_STATE_AFTER), 
							ex);
			ex = m_current_signal_context.current_error;
			threw_ex = true;
		}
	}
	else
	{
		assert(m_current_signal_context.this_is_me == me);
		assert(m_current_signal_context.active_fixture == f);
#if 0
		assert(m_current_signal_context.active_method == m);
		assert(m_current_signal_context.active_funcname.compare(funcname ? funcname : "") == 0);
#endif
		m_current_signal_context.active_method = m;
		m_current_signal_context.active_funcname = (funcname ? funcname : "");
		m_current_signal_context.active_state = state;
		//m_current_signal_context.print_err_report = BFC_QUIET;

		if (!m_current_signal_context.sig_handlers_set)
		{
			// drop marker of previous errors: it's a new test we're starting here
			m_current_signal_context.error_set = false;
			m_current_signal_context.current_error = ex;
			//m_current_signal_context.sig_handlers_set = true;
		}

		threw_ex = cpp_eh_run(me, f, m, funcname, state, ex);

		//m_current_signal_context.sig_handlers_set = false;
	}

	return threw_ex;
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




















/*
################################################################

compile-time speedup by offloading the method implementations from bfc-testsuite.hpp into this single source file:
*/

/*
Note: when we don't receive a valid fixture and/or test name, we take the second
best option there: we grab those from the global storage available for 
signal processing. Such names will be surrounded by '?' to make it clear
to the viewer that we 'fudged' it a little.

Of course, this assumes bfc::error is not used outside the regular BFC realm, but
I think that's going to be a reasonable assumption.
*/
const char *testrunner::get_bfc_case_filename(const char *f)
{
	if (f && *f && *f != '?')
		return f;

	if (m_current_signal_context.this_is_me) // signals there's valid data in there
	{
		f = m_current_signal_context.current_error.m_file.c_str();
		if (f && *f && *f != '?')
			return f;
	}

	return "???";
}

int testrunner::get_bfc_case_lineno(int l)
{
	if (l > 0)
		return l;

	if (m_current_signal_context.this_is_me) // signals there's valid data in there
	{
		l = m_current_signal_context.current_error.m_line;
		if (l > 0)
			return l;
	}

	return 0;
}

const char *testrunner::get_bfc_case_fixturename(const char *f)
{
	if (f && *f && *f != '?')
		return f;

	if (m_current_signal_context.this_is_me  // signals there's valid data in there
		&& m_current_signal_context.active_fixture)
	{
		f = m_current_signal_context.active_fixture->get_name();
		if (f && *f)
			return f;
	}

	return "???";
}

const char *testrunner::get_bfc_case_testname(const char *f)
{
	if (f && *f && *f != '?')
		return f;

	if (m_current_signal_context.this_is_me) // signals there's valid data in there
	{
		f = m_current_signal_context.active_funcname.c_str();
		if (f && *f)
			return f;
	}

	return "???";
}


// cut down on the verbiage
#define BFC_MK_FNAM(f)		testrunner::get_bfc_case_filename(f)
#define BFC_MK_LINE(l)		testrunner::get_bfc_case_lineno(l)
#define BFC_MK_FIXN(f)		testrunner::get_bfc_case_fixturename(f)
#define BFC_MK_CASN(t)		testrunner::get_bfc_case_testname(t)

error::error(const char *f, int l, const char *fix, const char *t, const char *m, ...) 
: m_file(BFC_MK_FNAM(f)), m_line(BFC_MK_LINE(l)), 
	m_fixture_name(BFC_MK_FIXN(fix)), m_test(BFC_MK_CASN(t))
{
	va_list a;
	va_start(a, m);
	vfmt_message(m, a);
	va_end(a);
}

error::error(const char *f, int l, const std::string &fix, const std::string &t, const char *m, ...)
: m_file(BFC_MK_FNAM(f)), m_line(BFC_MK_LINE(l)), 
	m_fixture_name(BFC_MK_FIXN(fix.c_str())), m_test(BFC_MK_CASN(t.c_str()))
{
	va_list a;
	va_start(a, m);
	vfmt_message(m, a);
	va_end(a);
}

error::error(const std::string &f, int l, const std::string &fix, const std::string &t, const char *m, ...)
    : m_file(BFC_MK_FNAM(f.c_str())), m_line(BFC_MK_LINE(l)), 
	m_fixture_name(BFC_MK_FIXN(fix.c_str())), m_test(BFC_MK_CASN(t.c_str()))
{
	va_list a;
	va_start(a, m);
	vfmt_message(m, a);
	va_end(a);
}

error::error(const error &base, const char *m, ...)
    : m_file(BFC_MK_FNAM(base.m_file.c_str())), m_line(BFC_MK_LINE(base.m_line)), 
	m_fixture_name(BFC_MK_FIXN(base.m_fixture_name.c_str())), m_test(BFC_MK_CASN(base.m_test.c_str()))
{
	va_list a;
	va_start(a, m);
	vfmt_message(m, a);
	va_end(a);
}

error::error(const char *f, int l, fixture &fix, const char *t, const char *m, va_list args)
    : m_file(BFC_MK_FNAM(f)), m_line(BFC_MK_LINE(l)), 
	m_fixture_name(BFC_MK_FIXN(fix.get_name())), m_test(BFC_MK_CASN(t))
{
	vfmt_message(m, args);
}

#undef BFC_MK_FNAM
#undef BFC_MK_LINE
#undef BFC_MK_FIXN
#undef BFC_MK_CASN



error::error(const error &src)
{
	if (this != &src)
	{
		m_file = src.m_file.c_str();
		m_line = src.m_line;
		m_fixture_name = src.m_fixture_name.c_str();
		m_test = src.m_test.c_str();
		m_message = src.m_message.c_str();
	}
}

error::~error()
{
}

void error::vfmt_message(const char *msg, va_list args)
{
	char buf[2048];
	
	if (!msg) 
	{
		*buf = 0;
	}
	else
	{
#if 0 // general use, so no direct dependency on hamster:util.h
		util_vsnprintf(buf, sizeof(buf), msg, args);
#else
#if defined __USE_BSD || defined __USE_ISOC99 || defined __USE_UNIX98 \
		|| defined __USE_POSIX || defined __USE_POSIX2 \
		|| defined __CYGWIN32__
	    	vsnprintf(buf, sizeof(buf), msg, args);
#elif defined(_MSC_VER)
    		_vsnprintf(buf, sizeof(buf), msg, args);
#else
#error "remove this #error if you are sure your platform does not have a vsnprintf() equivalent function"
    		vsprintf(buf, msg, args); // unsafe
#endif
#endif
		buf[sizeof(buf)-1] = 0;
	}
	m_message = buf;
}

void error::fmt_message(const char *msg, ...)
{
	va_list a;
	va_start(a, msg);
	vfmt_message(msg, a);
	va_end(a);
}








/*
register a new test function
*/
void fixture::register_test(const char *name, method foo) {
	static method *m;
    test t;
    t.name=name;
    t.foo=foo;
	// UGLY!!
	// add some random shitty code, otherwise the MSVC compiler
	// will set t.foo to zero because of optimization
	// (thanks, Microsoft...)
	m=&t.foo;
	if (foo)
		m++;
    m_tests.push_back(t);
}

void fixture::throw_bfc_error(const char *file, int line, const char *function, const char *message, ...)
{
	va_list args;
	va_start(args, message);
	bfc::error e(file, line, *this, function, message, args);
	va_end(args);

	// before we throw this error, we traverse the list of registered assertion monitors:
	// they may want to add / edit this error report
	assert_monitor_stack_t::iterator it = m_assert_monitors.begin();
	assert_monitor_stack_t::iterator itend = m_assert_monitors.end();
	while (it != itend)
	{
		(*it)->handler(e);
		it++;
	}

	// now remove all monitors from the queue:
	m_assert_monitors.clear();

	throw e;
}

void fixture::push_assert_monitor(bfc_assert_monitor &handler)
{
	// make sure the monitor has not yet been registered:
	assert_monitor_stack_t::iterator it = m_assert_monitors.begin();
	assert_monitor_stack_t::iterator itend = m_assert_monitors.end();
	while (it != itend)
	{
		if ((*it) == &handler)
			return;
	}

	m_assert_monitors.push_back(&handler);
}

void fixture::pop_assert_monitor(void)
{
	m_assert_monitors.pop_back();
}





testrunner::testrunner()
	: m_success(0),
	m_catch_coredumps(1),
	m_catch_exceptions(1),
	m_outputdir(""),
	m_inputdir("")
{ 
}

testrunner::~testrunner()
{ }



/*
reset error collection, etc.

invoke this before calling a run() method when you don't wish to use
the default, built-in reporting (print_err_report == true)
*/
void testrunner::init_run(void)
{
	m_errors.clear();
}


// print all errors
void testrunner::print_errors(bool panic_flush) {
        std::vector<error>::iterator it;
        unsigned i=1;

        for (it=m_errors.begin(); it!=m_errors.end(); it++, i++) {
#if 0
			std::cout << "----- error #" << i << " in " 
                      << it->fixture << "::" << it->test << std::endl;
            std::cout << it->file << ":" << it->line << " "
                      << it->message.c_str() << std::endl;
#else
            std::cout << "----- error #";
			std::cout << i;
			std::cout << " in ";
			std::cout << (it->m_fixture_name.size() > 0  ? it->m_fixture_name.c_str() : "???");
			std::cout << "::";
			std::cout << (it->m_test.size() > 0  ? it->m_test.c_str() : "???");
			std::cout << std::endl;
			std::cout << (it->m_file.size() > 0  ? it->m_file.c_str() : "???");
			std::cout << ":";
			std::cout << it->m_line;
			std::cout << " ";
			std::cout << (it->m_message.size() > 0 ? it->m_message.c_str() : "???");
			if (it->m_message.size() == 0 || !strchr("\r\n", *(it->m_message.rbegin())))
				std::cout << std::endl;
#endif
			if (panic_flush)
			{
				std::flush(std::cout);
			}
		}

        std::cout << "-----------------------------------------" << std::endl;
        std::cout << "total: " << m_errors.size() << " errors, "
                  << (m_success+m_errors.size()) << " tests" << std::endl;
		if (panic_flush)
		{
			std::flush(std::cout);
		}
    }

// run all tests - returns number of errors
unsigned int testrunner::run(bool print_err_report) 
	{
		std::string fixname("");
		std::string testname("");
		
		return run(fixname, testname, fixname, testname, true, false, print_err_report);
	}

// run all tests (optional fixture and/or test selection) - returns number of errors
unsigned int testrunner::run(const char *fixture_name, const char *test_name, 
			bool print_err_report) 
	{
		std::string fixname(fixture_name ? fixture_name : "");
		std::string testname(test_name ? test_name : "");
		
		return run(fixname, testname, fixname, testname, true, true, print_err_report);
	}

// run all tests in a given range (start in/exclusive, end inclusive)
//
// returns number of errors
unsigned int testrunner::run(
		const std::string &begin_fixture, const std::string &begin_test,
		const std::string &end_fixture, const std::string &end_test,
		bool inclusive_begin, 
		bool is_not_a_series, 
		bool print_err_report)
	{
		std::vector<fixture *>::iterator it;
		if (print_err_report)
		{
			init_run();
		}
		bool f_start = (begin_fixture.size() == 0);
		bool f_end = false;
		bool delay = !inclusive_begin;
		bool t_start = (begin_test.size() == 0);
		bool t_end = false;

		for (it = m_fixtures.begin(); it != m_fixtures.end() && !f_end; it++)
		{
			bool b_match = (begin_fixture.size() == 0 
							|| begin_fixture.compare((*it)->get_name()) == 0);
			bool e_match = (end_fixture.size() == 0 
							|| end_fixture.compare((*it)->get_name()) == 0);
			/* 
			is_not_a_series: do not treat start-end as a single to-from range of tests.

			Instead, only tests in fixtures which contain matching start or end tests
			are executed.
			*/
			if (is_not_a_series)
			{
				t_start = (begin_test.size() == 0);
				t_end = false;
			}

			f_start |= b_match;

			if (f_start && !f_end)
			{
				// fixture-wise, we've got a 'GO!'
				std::vector<test>::iterator it2;
				fixture *f = (*it);

				for (it2 = f->get_tests().begin(); 
					it2 != f->get_tests().end() && !t_end; 
					it2++) 
				{
					t_start |= (b_match && begin_test.compare(it2->name) == 0);

					if (t_start && delay)
					{
						delay = false;
					}
					else if (t_start && (!t_end || !delay))
					{
						const test &t = *it2;
					    run(f, &t, (print_err_report ? BFC_REPORT_IN_OUTER : BFC_QUIET));
					}

					if (t_end)
					{
						delay = true;
					}
					t_end |= (e_match && end_test.compare(it2->name) == 0);
				}
			}

			f_end |= (e_match && end_fixture.size() != 0); // explicit match only
		}

		if (print_err_report)
		{
			print_errors();
		}
		return ((unsigned int)m_errors.size());
	}

// run all tests of a fixture
unsigned int testrunner::run(fixture *f, const char *test_name, bool print_err_report) 
	{
		if (print_err_report)
		{
			init_run();
		}
        std::vector<test>::iterator it;
		std::string testname(test_name ? test_name : "");

        for (it=f->get_tests().begin(); it!=f->get_tests().end(); it++) 
		{
			if (testname.size() == 0 || testname.compare(it->name) == 0)
			{
				run(f, &(*it), (print_err_report ? BFC_REPORT_IN_OUTER : BFC_QUIET));
			}
		}

		if (print_err_report)
		{
			print_errors();
		}
		return ((unsigned int)m_errors.size());
	}

// run a single test of a fixture
bool testrunner::run(fixture *f, const test *test, bfc_error_report_mode_t print_err_report)
	{
        bool success = false;

		std::cout << "starting " << f->get_name()
				  << "::" << test->name << std::endl;

		// initialize the signal context:
		m_current_signal_context.print_err_report = print_err_report;
		m_current_signal_context.this_is_me = this;
		m_current_signal_context.active_fixture = f;
		//m_current_signal_context.active_funcname = test->name;
		m_current_signal_context.active_method = test->foo;
		m_current_signal_context.active_state = BFC_STATE_NONE;

		if ( /* m_catch_exceptions || */ m_catch_coredumps)
		{
			// see IMPLEMENTATION SPECIFIC NOTES in main.cpp ~ line 390:
			try 
			{
				success = !exec_a_single_test(f, test);
			}
			catch (bfc::error &ex)
			{
				/*
				when we get here, we are VERY probably going to be toast...

				Anyway, when we arrive in here, one thing's for sure: we got a
				signal outside the assumed run-time zone and our signal setup
				did not get a chance to 'unregister'.

				We could try to recover from such a disaster, but chances are
				HUGE we won't make it through alive, so we leave it be and
				wait for that core dump to happen.
				*/
				bfc::error e(ex, "UNEXPECTED exception caught "
					"(this hints at a bug in the BFC framework itself!): %s", 
					ex.m_message.c_str());
				//printf("FAILED!\n");
				success = false;
				add_error(&e);

				/*
				dump the error list NOW, while we still got a chance.
				
				ignore the fact we may print the error list once again in 
				the outer call.
				*/
				if (m_current_signal_context.print_err_report != BFC_QUIET)
				{
					print_errors(true);
				}
			}
		}
		else
		{
			success = !exec_a_single_test(f, test);
		}

		// invalidate the current signal state data
		m_current_signal_context.this_is_me = NULL;

		/* only count a completely flawless run as a success: */
		if (success)
		{
			add_success();
		}
		return !success;
	}

// run a single test of a fixture
bool testrunner::exec_a_single_test(fixture *f, const test *test)
	{
		method m = test->foo;
		error e(__FILE__, __LINE__, f->get_name(), test->name.c_str(), "");
		bool threw_ex = false;

		threw_ex = exec_testfun(this, f, &fixture::setup, "setup", BFC_STATE_SETUP, e);
		if (threw_ex)
		{
			if (e.m_test != "setup")
			{
				// failure probably happened in a subroutine called from setup(); 
				// make sure both the origin and this test name are present in the 
				// error info then!
				std::string msg = e.m_message;
				e.m_message = "failure in ";
				e.m_message += e.m_test;
				e.m_message += "(): ";
				e.m_message += msg;
				e.m_test = "setup";
			}
			add_error(&e);
		}
		else
		{
			threw_ex = exec_testfun(this, f, m, test->name.c_str(), BFC_STATE_FUT_INVOCATION, e);
			if (threw_ex)
			{
				if (e.m_test != test->name)
				{
					// failure probably happened in a subroutine called from setup(); 
					// make sure both the origin and this test name are present in the 
					// error info then!
					std::string msg = e.m_message;
					e.m_message = "failure in ";
					e.m_message += e.m_test;
					e.m_message += "(): ";
					e.m_message += msg;
					e.m_test = test->name;
				}
				add_error(&e);
			}
		}

		/* in any case: call the teardown function */
		bool threw_ex_after = exec_testfun(this, f, &fixture::teardown, "teardown", BFC_STATE_TEARDOWN, e);
		if (threw_ex_after)
		{
			if (e.m_test != "teardown")
			{
				// failure probably happened in a subroutine called from setup(); 
				// make sure both the origin and this test name are present in the 
				// error info then!
				std::string msg = e.m_message;
				e.m_message = "failure in ";
				e.m_message += e.m_test;
				e.m_message += "(): ";
				e.m_message += msg;
				e.m_test = "teardown";
			}
			add_error(&e);
		}
		threw_ex |= threw_ex_after;

		return threw_ex;
	}

testrunner *testrunner::get_instance()  {
        if (!s_instance)
            s_instance=new testrunner();
        return (s_instance);
    }

void testrunner::delete_instance(void)
	{
        if (s_instance)
            delete s_instance;
        s_instance = NULL;
    }


const std::string &testrunner::outputdir(const char *outputdir)
	{
		if (outputdir)
		{
			m_outputdir = outputdir;
#if defined(_MSC_VER)
			size_t i;
			for (i = 0; i < m_outputdir.size(); i++)
			{
				if (m_outputdir[i] == '\\')
				{
					m_outputdir[i] = '/';
				}
			}
#endif
			if (*outputdir && *(m_outputdir.rbegin()) != '/')
				m_outputdir += '/';
		}

		return m_outputdir;
	}

const std::string &testrunner::inputdir(const char *inputdir)
	{
		if (inputdir)
		{
			m_inputdir = inputdir;
#if defined(_MSC_VER)
			size_t i;
			for (i = 0; i < m_inputdir.size(); i++)
			{
				if (m_inputdir[i] == '\\')
				{
					m_inputdir[i] = '/';
				}
			}
#endif
			if (*inputdir && *(m_inputdir.rbegin()) != '/')
				m_inputdir += '/';
		}

		return m_inputdir;
	}








testrunner::bfc_signal_context_t::bfc_signal_context_t()
	: 	this_is_me(NULL),
		active_fixture(NULL),
		active_method(0),
		//active_funcname(""),
		active_state(BFC_STATE_NONE),
		print_err_report(BFC_QUIET),
		current_error(__FILE__, __LINE__, "???", "???", ""),
		error_set(false),
        sig_handlers_set(false)
{
	for (int i = 0; 
		i < int(sizeof(old_sig_handlers) / sizeof(old_sig_handlers[0])); 
		i++)
	{
		old_sig_handlers[i].handler = (signal_handler_f)SIG_DFL;
	}

	//memset(&signal_return_point, 0, sizeof(signal_return_point));
}

testrunner::bfc_signal_context_t::~bfc_signal_context_t()
{
}
