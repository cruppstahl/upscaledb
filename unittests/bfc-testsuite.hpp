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

namespace bfc {

// forward declaratios
class fixture;

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

struct test
{
	std::string name;
    method foo;
};

struct error
{
    error(const char *f, int l, const char *fix, const char *t, std::string m) 
    :   file(f), line(l), fixture(fix), test(t), message(m) 
    { }

    const char *file;
    int line;
    const char *fixture;
    const char *test;
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

    const char *get_name()
    { return m_name; }

    virtual void setup() { }
    virtual void teardown() { }

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
public:
    testrunner()
    : m_success(0)
    { }

    ~testrunner()
    { }

    // register a new test fixture
    void register_fixture(fixture *f) {
        m_fixtures.push_back(f);
    }

    // add an error
    void add_error(error *e) {
        m_errors.push_back(*e);
    }

    // add a successful run
    void add_success() {
        m_success++;
    }

    // print all errors
    void print_errors() {
        std::vector<error>::iterator it;
        unsigned i=1;

        for (it=m_errors.begin(); it!=m_errors.end(); it++, i++) {
            std::cout << "----- error #" << i << " in " 
                      << (*it).fixture << "::" << (*it).test << std::endl;
            std::cout << (*it).file << ":" << (*it).line << " "
                      << (*it).message << std::endl;
        }

        std::cout << "-----------------------------------------" << std::endl;
        std::cout << "total: " << m_errors.size() << " errors, "
                  << (m_success+m_errors.size()) << " tests" << std::endl;
    }

    // run all tests - returns number of errors
    unsigned run() {
        std::vector<fixture *>::iterator it;
        m_errors.clear();

        for (it=m_fixtures.begin(); it!=m_fixtures.end(); it++) {
            run(*it);
        }

        print_errors();
        return ((unsigned)m_errors.size());
    }

    // run all tests of a fixture
    void run(fixture *f) {
        std::vector<test>::iterator it;

        for (it=f->get_tests().begin(); it!=f->get_tests().end(); it++) {
            bool success=true;
            try {
                method m=it->foo;
				std::cout << "starting " << f->get_name() 
						  << "::" << (*it).name << std::endl;
                f->setup();
                (f->*m)();
            }
            catch (error e) {
                //printf("FAILED!\n");
                success=false;
                add_error(&e);
            }
            if (success)
                add_success();

            /* in any case: call the teardown function */
            try {
                f->teardown();
            }
            catch (error e) {
                add_error(&e);
            }
        }
    }

    static testrunner *get_instance()  {
        if (!s_instance)
            s_instance=new testrunner();
        return (s_instance);
    }

private:
    static testrunner *s_instance;
    std::vector<fixture *> m_fixtures;
    std::vector<error> m_errors;
    unsigned m_success;
};

} // namespace bfc

#endif // BFC_TESTSUITE_HPP__
