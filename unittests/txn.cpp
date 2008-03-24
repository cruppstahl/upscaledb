/**
 * Copyright (C) 2005-2008 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See file COPYING.GPL2 and COPYING.GPL3 for License information.
 */

#include <stdexcept>
#include <vector>
#include <cppunit/extensions/HelperMacros.h>
#include <ham/hamsterdb.h>
#include "../src/db.h"
#include "../src/txn.h"
#include "../src/page.h"
#include "../src/error.h"
#include "memtracker.h"

class TxnTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(TxnTest);
    CPPUNIT_TEST      (beginCommitTest);
    CPPUNIT_TEST      (beginAbortTest);
    CPPUNIT_TEST      (structureTest);
    CPPUNIT_TEST      (addPageTest);
    CPPUNIT_TEST      (addPageAbortTest);
    CPPUNIT_TEST      (removePageTest);
    CPPUNIT_TEST_SUITE_END();

protected:
    ham_db_t *m_db;
    ham_device_t *m_dev;
    memtracker_t *m_alloc;

public:
    void setUp()
    { 
        ham_page_t *p;
        CPPUNIT_ASSERT((m_alloc=memtracker_new())!=0);
        CPPUNIT_ASSERT(0==ham_new(&m_db));
        db_set_allocator(m_db, (mem_allocator_t *)m_alloc);
        CPPUNIT_ASSERT((m_dev=ham_device_new((mem_allocator_t *)m_alloc, 
                        HAM_TRUE))!=0);
        CPPUNIT_ASSERT(m_dev->create(m_dev, ".test", 0, 0644)==HAM_SUCCESS);
        db_set_device(m_db, m_dev);
        p=page_new(m_db);
        CPPUNIT_ASSERT(0==page_alloc(p, m_dev->get_pagesize(m_dev)));
        db_set_header_page(m_db, p);
        db_set_pagesize(m_db, m_dev->get_pagesize(m_dev));
    }
    
    void tearDown() 
    { 
        if (db_get_header_page(m_db)) {
            page_free(db_get_header_page(m_db));
            page_delete(db_get_header_page(m_db));
            db_set_header_page(m_db, 0);
        }
        if (db_get_cache(m_db)) {
            cache_delete(m_db, db_get_cache(m_db));
            db_set_cache(m_db, 0);
        }
        if (db_get_device(m_db)) {
            if (db_get_device(m_db)->is_open(db_get_device(m_db)))
                db_get_device(m_db)->close(db_get_device(m_db));
            db_get_device(m_db)->destroy(db_get_device(m_db));
            db_set_device(m_db, 0);
        }
        ham_delete(m_db);
        CPPUNIT_ASSERT(!memtracker_get_leaks(m_alloc));
    }

    void beginCommitTest(void)
    {
        ham_txn_t txn;

        CPPUNIT_ASSERT(ham_txn_begin(&txn, m_db)==HAM_SUCCESS);
        CPPUNIT_ASSERT(ham_txn_commit(&txn, 0)==HAM_SUCCESS);
    }

    void beginAbortTest(void)
    {
        ham_txn_t txn;

        CPPUNIT_ASSERT(ham_txn_begin(&txn, m_db)==HAM_SUCCESS);
        CPPUNIT_ASSERT(ham_txn_abort(&txn)==HAM_SUCCESS);
    }

    void structureTest(void)
    {
        ham_txn_t txn;

        CPPUNIT_ASSERT(ham_txn_begin(&txn, m_db)==HAM_SUCCESS);
        CPPUNIT_ASSERT(txn_get_db(&txn)==m_db);
        CPPUNIT_ASSERT(txn_get_pagelist(&txn)==0);
        CPPUNIT_ASSERT_EQUAL((ham_u64_t)1, txn_get_id(&txn));
        txn_set_last_lsn(&txn, 0x15);
        CPPUNIT_ASSERT_EQUAL((ham_u64_t)0x15, txn_get_last_lsn(&txn));
        txn_set_pagelist(&txn, (ham_page_t *)0x13);
        CPPUNIT_ASSERT(txn_get_pagelist(&txn)==(ham_page_t *)0x13);
        txn_set_pagelist(&txn, 0);
        txn_set_log_desc(&txn, 4);
        CPPUNIT_ASSERT_EQUAL(4, txn_get_log_desc(&txn));
        CPPUNIT_ASSERT(txn_get_pagelist(&txn)==0);
        CPPUNIT_ASSERT(ham_txn_commit(&txn, 0)==HAM_SUCCESS);
    }

    void addPageTest(void)
    {
        ham_txn_t txn;
        ham_page_t *page;

        CPPUNIT_ASSERT((page=page_new(m_db))!=0);
        page_set_self(page, 0x12345);

        CPPUNIT_ASSERT(ham_txn_begin(&txn, m_db)==HAM_SUCCESS);
        CPPUNIT_ASSERT(txn_get_page(&txn, 0x12345)==0);
        CPPUNIT_ASSERT(txn_add_page(&txn, page, 0)==HAM_SUCCESS);
        CPPUNIT_ASSERT(txn_add_page(&txn, page, 1)==HAM_SUCCESS);
        CPPUNIT_ASSERT(txn_get_page(&txn, 0x12345)==page);

        CPPUNIT_ASSERT(ham_txn_commit(&txn, 0)==HAM_SUCCESS);

        page_delete(page);
    }

    void addPageAbortTest(void)
    {
        ham_txn_t txn;
        ham_page_t *page;

        CPPUNIT_ASSERT((page=page_new(m_db))!=0);
        page_set_self(page, 0x12345);

        CPPUNIT_ASSERT(ham_txn_begin(&txn, m_db)==HAM_SUCCESS);
        CPPUNIT_ASSERT(txn_get_page(&txn, 0x12345)==0);
        CPPUNIT_ASSERT(txn_add_page(&txn, page, 0)==HAM_SUCCESS);
        CPPUNIT_ASSERT(txn_add_page(&txn, page, 1)==HAM_SUCCESS);
        CPPUNIT_ASSERT(txn_get_page(&txn, 0x12345)==page);
        CPPUNIT_ASSERT_EQUAL(0, txn_free_page(&txn, page));
        CPPUNIT_ASSERT(page_get_npers_flags(page)&PAGE_NPERS_DELETE_PENDING);

        CPPUNIT_ASSERT(ham_txn_abort(&txn)==HAM_SUCCESS);

        page_delete(page);
    }

    void removePageTest(void)
    {
        ham_txn_t txn;
        ham_page_t *page;

        CPPUNIT_ASSERT((page=page_new(m_db))!=0);
        page_set_self(page, 0x12345);

        CPPUNIT_ASSERT(ham_txn_begin(&txn, m_db)==HAM_SUCCESS);
        CPPUNIT_ASSERT(txn_add_page(&txn, page, 0)==HAM_SUCCESS);
        CPPUNIT_ASSERT(txn_get_page(&txn, page_get_self(page))==page);
        CPPUNIT_ASSERT(txn_remove_page(&txn, page)==HAM_SUCCESS);
        CPPUNIT_ASSERT(txn_get_page(&txn, page_get_self(page))==0);

        CPPUNIT_ASSERT(ham_txn_commit(&txn, 0)==HAM_SUCCESS);

        page_delete(page);
    }

};

CPPUNIT_TEST_SUITE_REGISTRATION(TxnTest);

