/* 
 * bfc-testsuite

  Copyright (C) 2005-2008 Christoph Rupp, www.crupp.de

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
 */

/*
 * The BFC unit test suite comes as a set of one header file, which you must
 * include in each test source file defining tests / test fixtures, plus a 
 * few support files, which must be compiled into the test binary.
 * 
 * This is the global header file (bfc-testsuite.hpp).
 * 
 * The support source files are:
 * 
 *   bfc-testsuite.cpp  (implements all BFC core methods; compile-time speedup)
 * 
 *   bfc-signal.c       (implements a portable signal function, 
 *                       used by the BFC kernel)
 * 
 *   bfc_signal.h       (header file which exports the relevant content 
 *                       of bfc_signal.c)
 * 
 *   empty_sample.cpp   (an example BFC test fixture and (nil) test case: Test1
 */

#ifndef BFC_TESTSUITE_HPP__
#define BFC_TESTSUITE_HPP__

#if defined(_MSC_VER)
#include <windows.h>    // __try/__except support API
#endif

#include <string.h>
#include <cstdio>
#include <cstdlib>
#include <vector>
#include <deque>
#include <string>
#include <iostream>
#include <stdarg.h>
#ifndef UNDER_CE
/* the signal catching / hardware exception catching stuff 
 * for UNIX (and a bit for Win32/64 too) */
#   include <signal.h> 
#endif
#include <setjmp.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>


namespace bfc {

// forward declarations
class error;
class fixture;
class testrunner;

typedef void (fixture::*method)();

#define BFC_REGISTER_TEST(cls, mthd)  register_test(#mthd, (method)&cls::mthd)

#define BFC_REGISTER_FIXTURE(fix)     static fix __the_fixture##fix

#define BFC_ASSERT(expr)  \
                    do { \
                        ditch_all_caught_values(); \
                        if (catch_value(0, expr)==0) { \
                            throw_bfc_error(__FILE__, __LINE__, \
                            __FUNCTION__, \
                            "assertion failed in expr "#expr \
                            "; actual value: '%s'", \
                            get_caught_value_string(0)); } \
                    } while (0)


#define BFC_ASSERT_EQUAL(exp, act) \
                    do { \
                        ditch_all_caught_values(); \
                        if (catch_value(0, exp)!=catch_value(1, act)) { \
                            throw_bfc_error(__FILE__, __LINE__, \
                            __FUNCTION__, \
                            "assertion failed in expr "#exp" != "#act \
                            "; actual values: '%s' == '%s'", \
                            get_caught_value_string(0), \
                            get_caught_value_string(1)); } \
                    } while (0)

#define BFC_ASSERT_NOTEQUAL(exp, act) \
                    do { \
                        ditch_all_caught_values(); \
                        if (catch_value(0, exp)==catch_value(1, act)) { \
                            throw_bfc_error(__FILE__, __LINE__, \
                            __FUNCTION__, \
                            "assertion failed in expr "#exp" == "#act \
                            "; actual values: '%s' != '%s'", \
                            get_caught_value_string(0), \
                            get_caught_value_string(1)); } \
                    } while (0)

#define BFC_ASSERT_NULL(expr)  \
                    do { \
                        ditch_all_caught_values(); \
                        if (catch_value(0, expr)!=0) { \
                            throw_bfc_error(__FILE__, __LINE__, \
                            __FUNCTION__, \
                            "assertion failed in expr "#expr \
                            " != NULL" \
                            "; actual value: '%s' == NULL", \
                            get_caught_value_string(0)); } \
                    } while (0)

#define BFC_ASSERT_NOTNULL(expr)  \
                    do { \
                        ditch_all_caught_values(); \
                        if (catch_value(0, expr)==0) { \
                            throw_bfc_error(__FILE__, __LINE__, \
                            __FUNCTION__, \
                            "assertion failed in expr "#expr \
                            " == NULL" \
                            "; actual value: '%s' != NULL", \
                            get_caught_value_string(0)); } \
                    } while (0)

// for checks within loops: report round # as 'scenario #'
#define BFC_ASSERT_I(expr, scenario)  \
                    do { \
                        ditch_all_caught_values(); \
                        if (catch_value(0, expr)==0) { \
                            throw_bfc_error(__FILE__, __LINE__, \
                            __FUNCTION__, \
                            "assertion failed in expr "#expr \
                            " for scenario #%d" \
                            "; actual value: '%s'", \
                            int(scenario), \
                            get_caught_value_string(0)); } \
                    } while (0)

#define BFC_ASSERT_EQUAL_I(exp, act, scenario) \
                    do { \
                        ditch_all_caught_values(); \
                        if (catch_value(0, exp)!=catch_value(1, act)) { \
                            throw_bfc_error(__FILE__, __LINE__, \
                            __FUNCTION__, \
                            "assertion failed in expr "#exp" != "#act \
                            " for scenario #%d" \
                            "; actual values: '%s' == '%s'", \
                            int(scenario), \
                            get_caught_value_string(0), \
                            get_caught_value_string(1)); } \
                    } while (0)

#define BFC_ASSERT_NOTEQUAL_I(exp, act, scenario) \
                    do { \
                        ditch_all_caught_values(); \
                        if (catch_value(0, exp)==catch_value(1, act)) { \
                            throw_bfc_error(__FILE__, __LINE__, \
                            __FUNCTION__, \
                            "assertion failed in expr "#exp" == "#act \
                            " for scenario #%d" \
                            "; actual values: '%s' != '%s'", \
                            int(scenario), \
                            get_caught_value_string(0), \
                            get_caught_value_string(1)); } \
                    } while (0)

#define BFC_ASSERT_NULL_I(expr, scenario)  \
                    do { \
                        ditch_all_caught_values(); \
                        if (catch_value(0, expr)!=0) { \
                            throw_bfc_error(__FILE__, __LINE__, \
                            __FUNCTION__, \
                            "assertion failed in expr "#expr \
                            " != NULL" \
                            " for scenario #%d" \
                            "; actual value: '%s' == NULL", \
                            int(scenario), \
                            get_caught_value_string(0)); } \
                    } while (0)

#define BFC_ASSERT_NOTNULL_I(expr, scenario)  \
                    do { \
                        ditch_all_caught_values(); \
                        if (catch_value(0, expr)==0) { \
                            throw_bfc_error(__FILE__, __LINE__, \
                            __FUNCTION__, \
                            "assertion failed in expr "#expr \
                            " == NULL" \
                            " for scenario #%d" \
                            "; actual value: '%s' != NULL", \
                            int(scenario), \
                            get_caught_value_string(0)); } \
                    } while (0)

struct test
{
    std::string name;
    method foo;
};

class error
{
public:
    error(const char *f, int l, const char *fix, const char *t, const char *m, ...);
    error(const char *f, int l, const std::string &fix, const std::string &t, const char *m, ...);
    error(const std::string &f, int l, const std::string &fix, const std::string &t, const char *m, ...);
    error(const error &base, const char *m, ...);
    error(const char *f, int l, fixture &fix, const char *t, const char *m, va_list args);
    error(const error &src);
    ~error();
    void vfmt_message(const char *msg, va_list args);
    void fmt_message(const char *msg, ...);

public:
    std::string m_file;
    int m_line;
    std::string m_fixture_name;
    std::string m_test;
    std::string m_message;
};



/*
   State used to signal the (UNIX, [Win32/64]) hardware exception
   handler setup code where we are in the testing process, as we
   are pushing and popping custom, BFC-specific signal handlers
   on/off the signal, ahem, 'stack'.

   On Win32/64, using MSVC, most of this burden is taken up by the
   MSVC-specific __try/__except mechanism, but UNIX/GCC doesn't have 
   such a mechanism; there, it's signals galore.
 */
typedef enum 
{
    // 'major' states:
    BFC_STATE_NONE             = 0,
    BFC_STATE_SETUP            = 1,
    BFC_STATE_FUT_INVOCATION   = 2,
    BFC_STATE_TEARDOWN         = 3,

    BFC_STATE_MAJOR_STATE_MASK = 0x0FFF,

    // 'minor' states:
    BFC_STATE_BEFORE           = 0x1000,
    BFC_STATE_AFTER            = 0x2000,

    BFC_STATE_MINOR_STATE_MASK = 0xF0000
} bfc_state_t;


/**
 functor/callback class which can be registered with BFC to be invoked when a
 unittest assertion (@ref BFC_ASSERT et al) fires.

 @note Once an assertion has fired and this functor/callback has been invoked,
 it will be removed from the BFC assertion monitor stack; this is done so that monitors instantiated in local scoped
 storage (i.e. instantiated in the stack) are never invoked after the stack is
 unwound and the instance becomes invalid. Such a scenario could otherwise
 happen when @ref BFC_ASSERT checks are part of the @ref teardown() process
 and any of those assertions fire while the stack-instantiated monitors have not
 been popped off the stack, because @ref teardown() was invoked after the unittest
 it cleans up after had fired an assertion itself.

 This implies that monitors must re-registered in @ref teardown() when you wish to have
 them active in there when they have been invoked by a previous @ref BFC_ASSERT in the
 unittest method proper.
*/
class bfc_assert_monitor
{
public:
    virtual void handler(bfc::error &err) = 0;
    virtual ~bfc_assert_monitor() {};
};



class bfc_value_catcher
{
public:
    virtual ~bfc_value_catcher() {};
    virtual const char *value(void) = 0;
};


class bfc_value_catcher_bool : public bfc_value_catcher
{
public:
    bfc_value_catcher_bool(bool v)
    {
        _v = v;
    }
protected:
    bool _v;
    char _msg[5];
public:
    virtual ~bfc_value_catcher_bool() {};
    virtual const char *value(void)
    {
        if (_v)
            strcpy(_msg, "YES");
        else
            strcpy(_msg, "NO");
        return _msg;
    }
};



class bfc_value_catcher_char : public bfc_value_catcher
{
public:
    bfc_value_catcher_char(signed char v)
    {
        _v = v;
    }
protected:
    char _v;
    char _msg[10];
public:
    virtual ~bfc_value_catcher_char() {};
    virtual const char *value(void)
    {
        if (_v > 32 && _v < 127)
        {
            sprintf(_msg, "%c", _v);
        }
        else
        {
            sprintf(_msg, "\\x%02X", (unsigned int)((unsigned char)_v));
        }
        return _msg;
    }
};



class bfc_value_catcher_uchar : public bfc_value_catcher
{
public:
    bfc_value_catcher_uchar(unsigned char v)
    {
        _v = v;
    }
protected:
    unsigned char _v;
    char _msg[10];
public:
    virtual ~bfc_value_catcher_uchar() {};
    virtual const char *value(void)
    {
        if (_v > 32 && _v < 127)
        {
            sprintf(_msg, "%c", _v);
        }
        else
        {
            sprintf(_msg, "\\x%02X", (unsigned int)_v);
        }
        return _msg;
    }
};



class bfc_value_catcher_short : public bfc_value_catcher
{
public:
    bfc_value_catcher_short(signed short v)
    {
        _v = v;
    }
protected:
    signed short _v;
    char _msg[28];
public:
    virtual ~bfc_value_catcher_short() {};
    virtual const char *value(void)
    {
        sprintf(_msg, "%d", (int)_v);
        return _msg;
    }
};



class bfc_value_catcher_ushort : public bfc_value_catcher
{
public:
    bfc_value_catcher_ushort(unsigned short v)
    {
        _v = v;
    }
protected:
    unsigned short _v;
    char _msg[28];
public:
    virtual ~bfc_value_catcher_ushort() {};
    virtual const char *value(void)
    {
        sprintf(_msg, "%u", (unsigned int)_v);
        return _msg;
    }
};


class bfc_value_catcher_int : public bfc_value_catcher
{
public:
    bfc_value_catcher_int(signed int v)
    {
        _v = v;
    }
protected:
    signed int _v;
    char _msg[28];
public:
    virtual ~bfc_value_catcher_int() {};
    virtual const char *value(void)
    {
        sprintf(_msg, "%d", _v);
        return _msg;
    }
};



class bfc_value_catcher_uint : public bfc_value_catcher
{
public:
    bfc_value_catcher_uint(unsigned int v)
    {
        _v = v;
    }
protected:
    unsigned int _v;
    char _msg[28];
public:
    virtual ~bfc_value_catcher_uint() {};
    virtual const char *value(void)
    {
        sprintf(_msg, "%u", _v);
        return _msg;
    }
};



class bfc_value_catcher_long : public bfc_value_catcher
{
public:
    bfc_value_catcher_long(signed long v)
    {
        _v = v;
    }
protected:
    signed long _v;
    char _msg[28];
public:
    virtual ~bfc_value_catcher_long() {};
    virtual const char *value(void)
    {
        sprintf(_msg, "%ld", _v);
        return _msg;
    }
};



class bfc_value_catcher_ulong : public bfc_value_catcher
{
public:
    bfc_value_catcher_ulong(unsigned long v)
    : _v(v) {
        _msg[0] = 0;
    }
protected:
    unsigned long _v;
    char _msg[28];
public:
    virtual ~bfc_value_catcher_ulong() {};
    virtual const char *value(void)
    {
        sprintf(_msg, "%lu", _v);
        return _msg;
    }
};


#ifndef _MSC_VER
class bfc_value_catcher_longlong : public bfc_value_catcher
{
public:
    bfc_value_catcher_longlong(signed long long v)
    : _v(v) {
        _msg[0] = 0;
    }
protected:
    signed long long _v;
    char _msg[28];
public:
    virtual ~bfc_value_catcher_longlong() {};
    virtual const char *value(void)
    {
        sprintf(_msg, "%lld", _v);
        return _msg;
    }
};



class bfc_value_catcher_ulonglong : public bfc_value_catcher
{
public:
    bfc_value_catcher_ulonglong(unsigned long long v)
    : _v(v) {
        _msg[0] = 0;
    }
protected:
    unsigned long long _v;
    char _msg[28];
public:
    virtual ~bfc_value_catcher_ulonglong() {};
    virtual const char *value(void)
    {
        sprintf(_msg, "%llu", _v);
        return _msg;
    }
};

#else

class bfc_value_catcher_int64 : public bfc_value_catcher
{
public:
    bfc_value_catcher_int64(signed __int64 v)
    : _v(v) {
        _msg[0] = 0;
    }
protected:
    signed __int64 _v;
    char _msg[28];
public:
    virtual ~bfc_value_catcher_int64() {};
    virtual const char *value(void)
    {
        sprintf(_msg, "%L64d", _v);
        return _msg;
    }
};



class bfc_value_catcher_uint64 : public bfc_value_catcher
{
public:
    bfc_value_catcher_uint64(unsigned __int64 v)
    : _v(v) {
        _msg[0] = 0;
    }
protected:
    unsigned __int64 _v;
    char _msg[28];
public:
    virtual ~bfc_value_catcher_uint64() {};
    virtual const char *value(void)
    {
        sprintf(_msg, "%L64u", _v);
        return _msg;
    }
};

#endif


class bfc_value_catcher_charptr : public bfc_value_catcher
{
public:
    bfc_value_catcher_charptr(const char *v)
    : _v(v) {
    }
protected:
    const char *_v;
public:
    virtual ~bfc_value_catcher_charptr() {};
    virtual const char *value(void)
    {
        if (_v)
            return _v;
        return "(NULL)";
    }
};



class bfc_value_catcher_voidptr : public bfc_value_catcher
{
public:
    bfc_value_catcher_voidptr(const void *v)
    : _v(v) {
        _msg[0] = 0;
    }
protected:
    const void *_v;
    char _msg[28];
public:
    virtual ~bfc_value_catcher_voidptr() {};
    virtual const char *value(void)
    {
        if (_v)
        {
            sprintf(_msg, "%p", _v);
            return _msg;
        }
        return "(NULL)";
    }
};







class fixture
{
public:
    fixture(const char *name)
        : m_name(name),
        _caught_values(NULL),
        _caught_values_size(0)
    { }

    virtual ~fixture()
    {
        ditch_all_caught_values();
    }

    const char *get_name() const
    { return m_name; }

    virtual void setup() { }
    virtual void teardown() { }

    /**
       Invoke the Function Under Test

       implement this one when you want to catch custom C++ exceptions;
       these must be converted to bfc::error class throwing exceptions 
       to work best with BFC.
       
       If you don't do this, your exceptions may be either caught by the
       platform-specific SEH handler or fall through the test rig and
       out the other side, losing information on the way, leading to
       harder to analyze output.

       @return
        Return TRUE when an exception occurred and the bfc::error ex
        instance is filled accordingly, FALSE otherwise (~ successful test).
    */
    virtual bool FUT_invoker(testrunner *me, method m, const char *funcname, bfc_state_t state, error &ex)
    {
        (this->*m)();
        return false;
    }

    virtual void throw_bfc_error(const char *file, int line, const char *function, const char *message, ...);

    // clear all tests
    void clear_tests() {
        m_tests.resize(0);
    }

    // register a new test function
    void register_test(const char *name, method foo);

    std::vector<test> &get_tests() {
        return m_tests;
    }

private:
    typedef std::deque<bfc_assert_monitor *> assert_monitor_stack_t;

protected:
    /**
     Adds a assertion monitor to the queue.

     @note As monitors are removed immediately after they have been invoked
     when a @ref BFC_ASSERT fired (see also the notes for @ref bfc_assert_monitor)
     you must re-register them after they've been invoked and you wish them to
     remain 'active'. To reduce BFC user code complexity, the need to check whether
     a given monitor has already been registered or not is not the responsibility
     of the BFC user but is handled in this method instead.
    */
    void push_assert_monitor(bfc_assert_monitor &handler);
    void pop_assert_monitor(void);

private:
    const char *m_name;
    std::vector<test> m_tests;
    assert_monitor_stack_t m_assert_monitors;

protected:
    bfc_value_catcher **_caught_values;
    size_t _caught_values_size;

    virtual void add_caught_value(unsigned int idx, bfc_value_catcher *c)
    {
        if (!_caught_values)
        {
            _caught_values_size = idx + 1;
            _caught_values = (bfc_value_catcher **)calloc(_caught_values_size, sizeof(_caught_values[0]));
            if (_caught_values == NULL)
            {
                /* panic! */
                _caught_values = NULL;
                _caught_values_size = 0;
            }
        }
        else if (_caught_values_size <= idx)
        {
            size_t n = idx + 1;
            _caught_values = (bfc_value_catcher **)realloc((void *)_caught_values, n * sizeof(_caught_values[0]));
            if (_caught_values == NULL)
            {
                /* panic! */
                _caught_values = NULL;
                _caught_values_size = 0;
            }
            else
            {
                memset(_caught_values + _caught_values_size, 0, (n - _caught_values_size) * sizeof(_caught_values[0]));
                _caught_values_size = n;
            }
        }
        if (_caught_values)
            _caught_values[idx] = c;
    }

    virtual const char *get_caught_value_string(unsigned int idx)
    {
        if (idx >= _caught_values_size)
            return "(NULL)";
        if (NULL == _caught_values[idx])
            return "(NULL)";
        return _caught_values[idx]->value();
    }

    virtual void ditch_all_caught_values(void)
    {
        for (unsigned i=0; i<_caught_values_size; i++)
            if (_caught_values[i])
                delete _caught_values[i];
        if (_caught_values)
            free((void *)_caught_values);
        _caught_values = NULL;
        _caught_values_size = 0;
    }

    bool catch_value(int idx, bool v)
    {
        bfc_value_catcher_bool *c = new bfc_value_catcher_bool(v);
        add_caught_value(idx, c);
        return v;
    }
    signed char catch_value(int idx, signed char v)
    {
        bfc_value_catcher_char *c = new bfc_value_catcher_char(v);
        add_caught_value(idx, c);
        return v;
    }
    unsigned char catch_value(int idx, unsigned char v)
    {
        bfc_value_catcher_uchar *c = new bfc_value_catcher_uchar(v);
        add_caught_value(idx, c);
        return v;
    }
    signed int catch_value(int idx, signed int v)
    {
        bfc_value_catcher_int *c = new bfc_value_catcher_int(v);
        add_caught_value(idx, c);
        return v;
    }
    unsigned int catch_value(int idx, unsigned int v)
    {
        bfc_value_catcher_uint *c = new bfc_value_catcher_uint(v);
        add_caught_value(idx, c);
        return v;
    }
    signed long catch_value(int idx, signed long v)
    {
        bfc_value_catcher_long *c = new bfc_value_catcher_long(v);
        add_caught_value(idx, c);
        return v;
    }
    unsigned long catch_value(int idx, unsigned long v)
    {
        bfc_value_catcher_ulong *c = new bfc_value_catcher_ulong(v);
        add_caught_value(idx, c);
        return v;
    }
#ifndef _MSC_VER
    signed long long catch_value(int idx, signed long long v)
    {
        bfc_value_catcher_longlong *c = new bfc_value_catcher_longlong(v);
        add_caught_value(idx, c);
        return v;
    }
    unsigned long long catch_value(int idx, unsigned long long v)
    {
        bfc_value_catcher_ulonglong *c = new bfc_value_catcher_ulonglong(v);
        add_caught_value(idx, c);
        return v;
    }
#else
    signed __int64 catch_value(int idx, signed __int64 v)
    {
        bfc_value_catcher_int64 *c = new bfc_value_catcher_int64(v);
        add_caught_value(idx, c);
        return v;
    }
    unsigned __int64 catch_value(int idx, unsigned __int64 v)
    {
        bfc_value_catcher_uint64 *c = new bfc_value_catcher_uint64(v);
        add_caught_value(idx, c);
        return v;
    }
#endif
    const char *catch_value(int idx, const char *v)
    {
        bfc_value_catcher_charptr *c = new bfc_value_catcher_charptr(v);
        add_caught_value(idx, c);
        return v;
    }
    const void *catch_value(int idx, const void *v)
    {
        bfc_value_catcher_voidptr *c = new bfc_value_catcher_voidptr(v);
        add_caught_value(idx, c);
        return v;
    }
};








typedef enum
{
    BFC_REPORT_IN_OUTER = 1,
    BFC_REPORT_IN_HERE = 2,
    BFC_QUIET = 0
} bfc_error_report_mode_t;


extern "C" {
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

       This type must match the type as defined in bfc_signal.h; it is redefined
       here for our convenience: now we still only need to #include
       this header file in our fixture/test sources.
     */
    typedef int (*signal_handler_f)(int signal_code, int sub_code);
}


class testrunner
{
protected:
    testrunner();
    ~testrunner();

protected:
    /*
    This function must be 'static' because otherwise MSVC will complain loudly:

    error C2712: Cannot use __try in functions that require object unwinding
    */
    static bool exec_testfun(testrunner *me, fixture *f, method m, const char *funcname, bfc_state_t state, error &ex);
    static bool cpp_eh_run(testrunner *me, fixture *f, method m, const char *funcname, bfc_state_t state, error &ex);

#if defined(_MSC_VER)
    static void cvt_hw_ex_as_cpp_ex(const EXCEPTION_RECORD *e, testrunner *me, const fixture *f, method m, const char *funcname, error &err);
    static int is_hw_exception(unsigned int code, struct _EXCEPTION_POINTERS *ep, EXCEPTION_RECORD *dst);
#endif

    static const int m_signals_to_catch[];

    // MSVC: #define NSIG 23     /* maximum signal number + 1 */
    struct bfc_signal_context_t
    {
        bfc_signal_context_t();
        ~bfc_signal_context_t();

        struct
        {
            signal_handler_f handler;
#ifdef UNDER_CE
        } old_sig_handlers[1]; 
#else
        } old_sig_handlers[NSIG + 1]; 
#endif
        /*
           ^^^ most systems include signal 0 in the NSIG count, but we should 
           dimension this one conservatively.

           (see also [APitUE, pp. 292])
         */

        testrunner *this_is_me;
        const fixture *active_fixture;
        method active_method;
        std::string active_funcname;
        jmp_buf signal_return_point;

        // things that may get changed inside the signal handler (~ will change asynchronously):
        volatile bfc_state_t active_state;
        volatile bfc_error_report_mode_t print_err_report;
        /* volatile */ bfc::error current_error;
        volatile bool error_set;


        bool sig_handlers_set;
    };

    static bfc_signal_context_t m_current_signal_context;

    static bool setup_signal_handlers(testrunner *me, const fixture *f, method m, const char *funcname, bfc_state_t sub_state, error &err);
    static int BFC_universal_signal_handler(int signal_code, int sub_code);
    static const char *bfc_sigdescr(int signal_code);

public:
    // register a new test fixture
    void register_fixture(fixture *f) {
        m_fixtures.push_back(f);
    }

    // add an error
    void add_error(const error *e) {
        m_errors.push_back(*e);
    }

    // add a successful run
    void add_success(void) {
        m_success++;
    }

    /**
    reset error collection, etc.
    
    invoke this before calling a @ref run() method when you don't wish to use
    the default, built-in reporting (@a print_err_report == true)
    */
    void init_run(void);

    /**
    print an error report listing all errors.
    */
    void print_errors(bool panic_flush = false);

    /**
 run all tests - returns number of errors 
*/
    unsigned int run(bool print_err_report = true);
    /**
 run all tests (optional fixture and/or test selection) - returns number of errors
*/
    unsigned int run(const char *fixture_name, const char *test_name = NULL, 
            bool print_err_report = true);

    /**
 run all tests in a given range (start in/exclusive, end inclusive)
    
@return number of errors
*/
    unsigned int run(
        const std::string &begin_fixture, const std::string &begin_test,
        const std::string &end_fixture, const std::string &end_test,
        bool inclusive_begin, 
        bool is_not_a_series = false,
        bool print_err_report = true);

    // run all tests of a fixture
    unsigned int run(fixture *f, const char *test_name = NULL, bool print_err_report = true);

    // run a single test of a fixture
    bool run(fixture *f, const test *test, bfc_error_report_mode_t print_err_report = BFC_REPORT_IN_HERE);

protected:
    // run a single test of a fixture
    bool exec_a_single_test(fixture *f, const test *test);

public:
    // singleton:
    static testrunner *get_instance();
    static void delete_instance(void);

    bool catch_coredumps(int catch_coredumps = -1)
    {
        if (catch_coredumps >= 0)
        {
            m_catch_coredumps = catch_coredumps;
        }
        return m_catch_coredumps;
    }

    bool catch_exceptions(int catch_exceptions = -1)
    {
        if (catch_exceptions >= 0)
        {
            m_catch_exceptions = catch_exceptions;
        }
        return m_catch_exceptions;
    }

    const std::string &outputdir(const char *outputdir = NULL);
    const std::string &inputdir(const char *inputdir = NULL);

    static std::string expand_inputpath(const char *relative_filepath);
    static std::string expand_outputpath(const char *relative_filepath);

#define BFC_IPATH(p)    testrunner::expand_inputpath(p).c_str()
#define BFC_OPATH(p)    testrunner::expand_outputpath(p).c_str()

    static const char *get_bfc_case_filename(const char *f);
    static int get_bfc_case_lineno(int l);
    static const char *get_bfc_case_fixturename(const char *f);
    static const char *get_bfc_case_testname(const char *f);

private:
    static testrunner *s_instance;
    std::vector<fixture *> m_fixtures;
    std::vector<error> m_errors;
    unsigned m_success;
    unsigned m_catch_coredumps : 1;
    unsigned m_catch_exceptions : 1;
    std::string m_outputdir;
    std::string m_inputdir;
};

} // namespace bfc

#endif // BFC_TESTSUITE_HPP__
