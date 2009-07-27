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
 *
 */

#ifndef BFC_TESTSUITE_HPP__
#define BFC_TESTSUITE_HPP__

#include <vector>
#include <string>
#include <iostream>
#include <stdarg.h>

#if defined(_MSC_VER)
#include <windows.h>    // __try/__except support API
#endif

namespace bfc {

// forward declarations
class fixture;
class testrunner;

typedef void (fixture::*method)();

#define BFC_REGISTER_TEST(cls, mthd)  register_test(#mthd, (method)&cls::mthd)

#define BFC_REGISTER_FIXTURE(fix)     static fix __the_fixture##fix

#define BFC_ASSERT(expr)  \
                        while ((expr)==0) { \
                            throw error(__FILE__, __LINE__, \
                            get_name(), __FUNCTION__, \
                            "assertion failed in expr "#expr); }

#define BFC_ASSERT_EQUAL(exp, act) \
                        while ((exp)!=(act)) { \
                            throw error(__FILE__, __LINE__, \
                            get_name(), __FUNCTION__, \
                            "assertion failed in expr "#exp" == "#act); }

#define BFC_ASSERT_NOTEQUAL(exp, act) \
                        while ((exp)==(act)) { \
                            throw error(__FILE__, __LINE__, \
                            get_name(), __FUNCTION__, \
                            "assertion failed in expr "#exp" != "#act); }

#define BFC_ASSERT_NULL(expr)  \
                        while ((expr)!=0) { \
                            throw error(__FILE__, __LINE__, \
                            get_name(), __FUNCTION__, \
                            "assertion failed in expr "#expr" == NULL"); }

#define BFC_ASSERT_NOTNULL(expr)  \
                        while ((expr)==0) { \
                            throw error(__FILE__, __LINE__, \
                            get_name(), __FUNCTION__, \
                            "assertion failed in expr "#expr" != NULL"); }

// for checks within loops: report round # as 'scenario #'
#define BFC_ASSERT_I(expr, scenario)  \
                        while ((expr)==0) { \
                            throw error(__FILE__, __LINE__, \
                            get_name(), __FUNCTION__, \
							"assertion failed in expr "#expr " for scenario #%d", int(scenario)); }

#define BFC_ASSERT_EQUAL_I(exp, act, scenario) \
                        while ((exp)!=(act)) { \
                            throw error(__FILE__, __LINE__, \
                            get_name(), __FUNCTION__, \
							"assertion failed in expr "#exp" == "#act " for scenario #%d", int(scenario)); }

#define BFC_ASSERT_NOTEQUAL_I(exp, act, scenario) \
                        while ((exp)==(act)) { \
                            throw error(__FILE__, __LINE__, \
                            get_name(), __FUNCTION__, \
							"assertion failed in expr "#exp" != "#act " for scenario #%d", int(scenario)); }

#define BFC_ASSERT_NULL_I(expr, scenario)  \
                        while ((expr)!=0) { \
                            throw error(__FILE__, __LINE__, \
                            get_name(), __FUNCTION__, \
							"assertion failed in expr "#expr" == NULL for scenario #%d", int(scenario)); }

#define BFC_ASSERT_NOTNULL_I(expr, scenario)  \
                        while ((expr)==0) { \
                            throw error(__FILE__, __LINE__, \
                            get_name(), __FUNCTION__, \
							"assertion failed in expr "#expr" != NULL for scenario #%d", int(scenario)); }

struct test
{
	std::string name;
    method foo;
};

struct error
{
    error(const char *f, int l, const char *fix, const char *t, const char *m, ...) 
    :   file(f), line(l), fixture(fix), test(t)
    {
	va_list a;
	va_start(a, m);
	char buf[2048];
	if (!m) m = "";
#if 0 // general use, so no direct dependency on hamster:util.h
	util_vsnprintf(buf, sizeof(buf), m, a);
#else
#if defined(HAM_OS_POSIX)
    vsnprintf(buf, sizeof(buf), m, a);
#elif defined(HAM_OS_WIN32)
    _vsnprintf(buf, sizeof(buf), m, a);
#elif defined(_MSC_VER)
    _vsnprintf(buf, sizeof(buf), m, a);
#else
    vsprintf(buf, m, a); // unsafe
#endif
#endif
	va_end(a);
	buf[sizeof(buf)-1] = 0;
	message = buf;
	}

    std::string file;
    int line;
    std::string fixture;
    std::string test;
    std::string message;
};

class fixture
{
public:
    fixture(const char *name)
        : m_name(name)
    { }

    virtual ~fixture()
    { }

    const char *get_name() const
    { return m_name; }

    virtual void setup() { }
    virtual void teardown() { }

	/*
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
	virtual bool FUT_invoker(testrunner *me, method m, const char *funcname, error &ex)
	{
        try {
		    (this->*m)();
        }
        catch (bfc::error &e) {
			ex = e;
            return (true);
        }
		return (false);
	}

	// clear all tests
    void clear_tests() {
        m_tests.resize(0);
    }

    // register a new test function
    void register_test(const char *name, method foo) {
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

    std::vector<test> &get_tests() {
        return m_tests;
    }

private:
    const char *m_name;
    std::vector<test> m_tests;
};

class testrunner
{
protected:
    testrunner()
	: m_success(0),
	m_catch_coredumps(1),
	m_catch_exceptions(1),
	m_outputdir(""),
	m_inputdir("")
	{ }

    ~testrunner()
    { }

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

    // print all errors
    void print_errors(void) {
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
			std::cout << (it->fixture.size() > 0  ? it->fixture.c_str() : "???");
			std::cout << "::";
			std::cout << (it->test.size() > 0  ? it->test.c_str() : "???");
			std::cout << std::endl;
			std::cout << (it->file.size() > 0  ? it->file.c_str() : "???");
			std::cout << ":";
			std::cout << it->line;
			std::cout << " ";
			std::cout << (it->message.size() > 0 ? it->message.c_str() : "???");
			std::cout << std::endl;
#endif
		}

        std::cout << "-----------------------------------------" << std::endl;
        std::cout << "total: " << m_errors.size() << " errors, "
                  << (m_success+m_errors.size()) << " tests" << std::endl;
    }

	// run all tests (optional fixture and/or test selection) - returns number of errors
	unsigned int run(const char *fixture_name = NULL, const char *test_name = NULL, 
			bool print_err_report = true) 
	{
		std::string fixname(fixture_name ? fixture_name : "");
		std::string testname(test_name ? test_name : "");
		
		return run(fixname, testname, fixname, testname, true, print_err_report);
	}

	// run all tests in a given range (start in/exclusive, end inclusive)
	//
	// returns number of errors
	unsigned int run(
		const std::string &begin_fixture, const std::string &begin_test,
		const std::string &end_fixture, const std::string &end_test,
		bool inclusive_begin, bool print_err_report = true)
	{
		std::vector<fixture *>::iterator it;
		if (print_err_report)
		{
			m_errors.clear();
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
					    run(f, &t);
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
    unsigned int run(fixture *f, const char *test_name = NULL, bool print_err_report = true) 
	{
		if (print_err_report)
		{
			m_errors.clear();
		}
        std::vector<test>::iterator it;
		std::string testname(test_name ? test_name : "");

        for (it=f->get_tests().begin(); it!=f->get_tests().end(); it++) 
		{
			if (testname.size() == 0 || testname.compare(it->name) == 0)
			{
			    run(f, &(*it));
			}
		}

		if (print_err_report)
		{
			print_errors();
		}
		return ((unsigned int)m_errors.size());
	}

	// run a single test of a fixture
    bool run(fixture *f, const test *test) 
	{
        bool success=true;

		method m = test->foo;
		error e(__FILE__, __LINE__, f->get_name(), test->name.c_str(), "");
		bool threw_ex;

		std::cout << "starting " << f->get_name()
				  << "::" << test->name << std::endl;

		threw_ex = exec_testfun(this, f, &fixture::setup, "setup", e);
		if (threw_ex)
		{
			//printf("FAILED!\n");
			success=false;
			add_error(&e);
		}
		else
		{
			threw_ex = exec_testfun(this, f, m, test->name.c_str(), e);
			if (threw_ex)
			{
				//printf("FAILED!\n");
				success=false;
				add_error(&e);
			}
		}

		/* in any case: call the teardown function */
		threw_ex = exec_testfun(this, f, &fixture::teardown, "teardown", e);
		if (threw_ex)
		{
			//printf("FAILED!\n");
			success=false;
			add_error(&e);
		}

		/* only count a completely flawless run as a success: */
		if (success)
		{
			add_success();
		}
		return success;
	}

	/*
	This function must be 'static' because otherwise MSVC will complain loudly:

	error C2712: Cannot use __try in functions that require object unwinding
	*/
	static bool exec_testfun(testrunner *me, fixture *f, method m, const char *funcname, error &ex);
	static bool cpp_eh_run(testrunner *me, fixture *f, method m, const char *funcname, error &ex);

#if defined(_MSC_VER)
	static void cvt_hw_ex_as_cpp_ex(const EXCEPTION_RECORD *e, testrunner *me, const fixture *f, method m, const char *funcname, error &err);
	static int is_hw_exception(unsigned int code, struct _EXCEPTION_POINTERS *ep, EXCEPTION_RECORD *dst);
#endif

    static testrunner *get_instance()  {
        if (!s_instance)
            s_instance=new testrunner();
        return (s_instance);
    }

    static void delete_instance(void)
	{
        if (s_instance)
            delete s_instance;
        s_instance = NULL;
    }

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

	const std::string &outputdir(const char *outputdir = NULL)
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

	const std::string &inputdir(const char *inputdir = NULL)
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

	static std::string expand_inputpath(const char *relative_filepath);
	static std::string expand_outputpath(const char *relative_filepath);

#define BFC_IPATH(p)	testrunner::expand_inputpath(p).c_str()
#define BFC_OPATH(p)	testrunner::expand_outputpath(p).c_str()

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
