/**
  Copyright (C) 2009 Ger Hobbelt, www.hebbut.net
  Copyright (C) 2009 Christoph Rupp, www.crupp.de
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

#include "bfc-signal.h"


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



/*
Use this *C* function to prevent compiler errors due to different
function types being passed around here.

We know what we're doing here. ;-)

There's several signal handler function types out there:

	void (*f)(int);
	int (*f)(int);
	void (*f)(int, int); // SIGFPE
	int (*f)(int, int); // SIGFPE

and we handle them all through one function type.
*/
signal_handler_f
bfc_signal(int code, signal_handler_f handler)
{
	/* 
	Note: this call will generate a compiler warning. Ignore that warning. 
	*/
#if defined(_MSC_VER)
#pragma warning(push)
#pragma warning(disable: 4113)
#endif

	return (signal_handler_f)signal(code, handler);

#if defined(_MSC_VER)
#pragma warning(pop)
#endif
}
