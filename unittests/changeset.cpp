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
#include <string.h>
#include <ham/hamsterdb.h>
#include "../src/changeset.h"
#include "../src/page.h"
#include "../src/db.h"

#include "bfc-testsuite.hpp"
#include "hamster_fixture.hpp"

using namespace bfc;

class ChangesetTest : public hamsterDB_fixture
{
    define_super(hamsterDB_fixture);

public:
    ChangesetTest()
    :   hamsterDB_fixture("ChangesetTest") 
    {
        testrunner::get_instance()->register_fixture(this);
        BFC_REGISTER_TEST(ChangesetTest, addPagesTest);
        BFC_REGISTER_TEST(ChangesetTest, getPagesTest);
        BFC_REGISTER_TEST(ChangesetTest, clearTest);
    }

protected:
    ham_db_t *m_db;
    ham_env_t *m_env;

public:
    virtual void setup() 
    { 
        __super::setup();

        BFC_ASSERT_EQUAL(0, ham_new(&m_db));
        BFC_ASSERT_EQUAL(0, 
                ham_create_ex(m_db, BFC_OPATH(".test"), 
                HAM_ENABLE_RECOVERY, 0644, 0));
        m_env=ham_get_env(m_db);
    }
    
    virtual void teardown() 
    { 
        __super::teardown();

        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        ham_delete(m_db);
    }

    void addPagesTest()
    {
        Changeset ch;
        Page *page[3];
        for (int i=0; i<3; i++) {
            page[i]=new Page((Environment *)m_env);
            page[i]->set_self(1024*i);
        }
        for (int i=0; i<3; i++)
            ch.add_page(page[i]);
        BFC_ASSERT_EQUAL(page[1],
                    page[2]->get_next(Page::LIST_CHANGESET));
        BFC_ASSERT_EQUAL(page[0],
                    page[1]->get_next(Page::LIST_CHANGESET));
        BFC_ASSERT_EQUAL((Page *)NULL,
                    page[0]->get_next(Page::LIST_CHANGESET));
        BFC_ASSERT_EQUAL(page[1],
                    page[0]->get_previous(Page::LIST_CHANGESET));
        BFC_ASSERT_EQUAL(page[2],
                    page[1]->get_previous(Page::LIST_CHANGESET));
        BFC_ASSERT_EQUAL((Page *)NULL,
                    page[2]->get_previous(Page::LIST_CHANGESET));
        for (int i=0; i<3; i++)
            delete page[i];
    }

    void getPagesTest()
    {
        Changeset ch;
        Page *page[3];
        for (int i=0; i<3; i++) {
            page[i]=new Page((Environment *)m_env);
            page[i]->set_self(1024*i);
        }
        for (int i=0; i<3; i++)
            ch.add_page(page[i]);

        for (int i=0; i<3; i++)
            BFC_ASSERT_EQUAL(page[i], ch.get_page(page[i]->get_self()));
        BFC_ASSERT_EQUAL((Page *)NULL, ch.get_page(999));

        for (int i=0; i<3; i++)
            delete page[i];
    }

    void clearTest()
    {
        Changeset ch;
        Page *page[3];
        for (int i=0; i<3; i++) {
            page[i]=new Page((Environment *)m_env);
            page[i]->set_self(1024*i);
        }
        for (int i=0; i<3; i++)
            ch.add_page(page[i]);

        BFC_ASSERT_EQUAL(false, ch.is_empty());
        ch.clear();
        BFC_ASSERT_EQUAL((Page *)NULL, ch.get_head());
        BFC_ASSERT_EQUAL(true, ch.is_empty());

        for (int i=0; i<3; i++)
            BFC_ASSERT_EQUAL((Page *)NULL,
                    ch.get_page(page[i]->get_self()));

        for (int i=0; i<3; i++)
            delete page[i];
    }
};

BFC_REGISTER_FIXTURE(ChangesetTest);

