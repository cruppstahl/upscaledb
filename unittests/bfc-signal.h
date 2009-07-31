/* 
 * bfc-testsuite

  Copyright (C) 2009 Ger Hobbelt, www.hebbut.net

  This software is provided 'as-is', without any express or implied
  warranty.  In no event will the authors be held liable for any damages
  arising from the use of this software.

  Permission is granted to anyone to use this software for any purpose,
  including commercial applications, and to alter it and redistribute it
  freely, subject to the following restrictions:

  1. The origin of this software must not be misrepresented; you must not
     claim that you wrote the original software. If you use this software
     in a product, an acknowledgment in the product documentation would be
     appreciated but is not required.
  2. Altered source versions must be plainly marked as such, and must not be
     misrepresented as being the original software.
  3. This notice may not be removed or altered from any source distribution.
 *
 */

#ifndef BFC_SIGNAL_H__
#define BFC_SIGNAL_H__

#include <signal.h> /* the signal catching / hardware exception catching stuff for UNIX (and a bit for Win32/64 too) */

#ifdef  __cplusplus
extern "C"
{
#endif


/*
   WARNING: some systems have 'int' returning signal handlers, others
   have 'void' returning signal handlers. Since the ones, which expect
   a 'void' return type, will silently ignore the return value
   at run-time anyhow, we can keep things simple here and just 
   specify 'int'.

   However, this will cause a set of compiler warnings to appear;
   which will be compiler _errors_ when we're compiling the setup
   code in C++ mode; hence the forced use of an 'extern "C"'
   setup function for this; see the internals in main.cpp...

   The 'subcode' is in the arg list for the SIGFPE handler.
 */
typedef int (*signal_handler_f)(int signal_code, int sub_code);

extern signal_handler_f bfc_signal(int code, signal_handler_f handler);

#ifdef  __cplusplus
}
#endif

#endif // BFC_SIGNAL_H__
