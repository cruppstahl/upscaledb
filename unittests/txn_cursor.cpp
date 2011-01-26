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
#include <cstdlib>
#include <cstring>
#include <vector>
#include <ham/hamsterdb.h>
#include "../src/db.h"
#include "../src/txn.h"
#include "../src/txn_cursor.h"
#include "../src/page.h"
#include "../src/error.h"
#include "../src/cursor.h"
#include "../src/env.h"
#include "../src/freelist.h"
#include "../src/os.h"
#include "memtracker.h"

#include "bfc-testsuite.hpp"
#include "hamster_fixture.hpp"

using namespace bfc;

class TxnCursorTest : public hamsterDB_fixture
{
    define_super(hamsterDB_fixture);

public:
    TxnCursorTest()
    : hamsterDB_fixture("TxnCursorTest")
    {
        testrunner::get_instance()->register_fixture(this);
        BFC_REGISTER_TEST(TxnCursorTest, structureTest);
        BFC_REGISTER_TEST(TxnCursorTest, cursorIsNilTest);
        BFC_REGISTER_TEST(TxnCursorTest, txnOpLinkedListTest);
        BFC_REGISTER_TEST(TxnCursorTest, getKeyFromCoupledCursorTest);
        BFC_REGISTER_TEST(TxnCursorTest, getKeyFromCoupledCursorUserAllocTest);
        BFC_REGISTER_TEST(TxnCursorTest, getKeyFromCoupledCursorEmptyKeyTest);
        BFC_REGISTER_TEST(TxnCursorTest, getKeyFromNilCursorTest);
        BFC_REGISTER_TEST(TxnCursorTest, getRecordFromCoupledCursorTest);
        BFC_REGISTER_TEST(TxnCursorTest, getRecordFromCoupledCursorUserAllocTest);
        BFC_REGISTER_TEST(TxnCursorTest, getRecordFromCoupledCursorEmptyRecordTest);
        BFC_REGISTER_TEST(TxnCursorTest, getRecordFromNilCursorTest);
        BFC_REGISTER_TEST(TxnCursorTest, findInsertTest);
        BFC_REGISTER_TEST(TxnCursorTest, findInsertEraseTest);
        BFC_REGISTER_TEST(TxnCursorTest, findInsertEraseOverwriteTest);
        BFC_REGISTER_TEST(TxnCursorTest, findCreateConflictTest);
        BFC_REGISTER_TEST(TxnCursorTest, moveFirstTest);
        BFC_REGISTER_TEST(TxnCursorTest, moveFirstInEmptyTreeTest);
        BFC_REGISTER_TEST(TxnCursorTest, moveNextWithNilCursorTest);
        BFC_REGISTER_TEST(TxnCursorTest, moveNextTest);
        BFC_REGISTER_TEST(TxnCursorTest, moveNextAfterEndTest);
        BFC_REGISTER_TEST(TxnCursorTest, moveNextSkipEraseTest);
        BFC_REGISTER_TEST(TxnCursorTest, moveNextSkipEraseInNodeTest);
        BFC_REGISTER_TEST(TxnCursorTest, moveLastTest);
        BFC_REGISTER_TEST(TxnCursorTest, moveLastInEmptyTreeTest);
        BFC_REGISTER_TEST(TxnCursorTest, movePrevWithNilCursorTest);
        BFC_REGISTER_TEST(TxnCursorTest, movePrevTest);
        BFC_REGISTER_TEST(TxnCursorTest, movePrevAfterEndTest);
        BFC_REGISTER_TEST(TxnCursorTest, movePrevSkipEraseTest);
        BFC_REGISTER_TEST(TxnCursorTest, movePrevSkipEraseInNodeTest);
        BFC_REGISTER_TEST(TxnCursorTest, insertKeysTest);
        BFC_REGISTER_TEST(TxnCursorTest, negativeInsertKeysTest);
        BFC_REGISTER_TEST(TxnCursorTest, insertOverwriteKeysTest);
        BFC_REGISTER_TEST(TxnCursorTest, insertCreateConflictTest);
        BFC_REGISTER_TEST(TxnCursorTest, eraseKeysTest);
        BFC_REGISTER_TEST(TxnCursorTest, negativeEraseKeysTest);
        BFC_REGISTER_TEST(TxnCursorTest, negativeEraseKeysNilTest);
        BFC_REGISTER_TEST(TxnCursorTest, overwriteRecordsTest);
        BFC_REGISTER_TEST(TxnCursorTest, overwriteRecordsNilCursorTest);
        BFC_REGISTER_TEST(TxnCursorTest, flushCoupledOpTest);
    }

protected:
    ham_cursor_t *m_cursor;
    ham_db_t *m_db;
    ham_env_t *m_env;
    memtracker_t *m_alloc;

public:
    virtual void setup() 
    { 
        __super::setup();

        BFC_ASSERT((m_alloc=memtracker_new())!=0);

        BFC_ASSERT_EQUAL(0, ham_new(&m_db));

        BFC_ASSERT_EQUAL(0, ham_env_new(&m_env));
        env_set_allocator(m_env, (mem_allocator_t *)m_alloc);

        BFC_ASSERT_EQUAL(0, 
                ham_env_create(m_env, BFC_OPATH(".test"), 
                    HAM_ENABLE_DUPLICATES
                        |HAM_ENABLE_RECOVERY
                        |HAM_ENABLE_TRANSACTIONS, 0664));
        BFC_ASSERT_EQUAL(0, 
                ham_env_create_db(m_env, m_db, 13, 0, 0));
        BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &m_cursor));
    }
    
    virtual void teardown() 
    { 
        __super::teardown();

        BFC_ASSERT_EQUAL(0, ham_cursor_close(m_cursor));
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        BFC_ASSERT_EQUAL(0, ham_env_close(m_env, 0));
        ham_delete(m_db);
        ham_env_delete(m_env);
        BFC_ASSERT(!memtracker_get_leaks(m_alloc));
    }

    void structureTest(void)
    {
        txn_cursor_t cursor={0};
        txn_op_t op={0};

        BFC_ASSERT_EQUAL(0u, txn_cursor_get_flags(&cursor));
        txn_cursor_set_flags(&cursor, 0x345);
        BFC_ASSERT_EQUAL(0x345u, txn_cursor_get_flags(&cursor));
        txn_cursor_set_flags(&cursor, 0);

        BFC_ASSERT_EQUAL((txn_op_t *)0, txn_cursor_get_coupled_op(&cursor));
        txn_cursor_set_coupled_op(&cursor, &op);
        BFC_ASSERT_EQUAL(&op, txn_cursor_get_coupled_op(&cursor));
        txn_cursor_set_coupled_op(&cursor, 0);

        BFC_ASSERT_EQUAL((txn_cursor_t *)0, 
                    txn_cursor_get_coupled_next(&cursor));
        txn_cursor_set_coupled_next(&cursor, (txn_cursor_t *)0x12);
        BFC_ASSERT_EQUAL((txn_cursor_t *)0x12,
                    txn_cursor_get_coupled_next(&cursor));
        txn_cursor_set_coupled_next(&cursor, 0);

        BFC_ASSERT_EQUAL((txn_cursor_t *)0, 
                    txn_cursor_get_coupled_previous(&cursor));
        txn_cursor_set_coupled_previous(&cursor, (txn_cursor_t *)0x21);
        BFC_ASSERT_EQUAL((txn_cursor_t *)0x21,
                    txn_cursor_get_coupled_previous(&cursor));
        txn_cursor_set_coupled_previous(&cursor, 0);
    }

    void cursorIsNilTest(void)
    {
        txn_cursor_t cursor={0};

        BFC_ASSERT_EQUAL(HAM_TRUE, txn_cursor_is_nil(&cursor));
        txn_cursor_set_flags(&cursor, TXN_CURSOR_FLAG_COUPLED);
        BFC_ASSERT_EQUAL(HAM_FALSE, txn_cursor_is_nil(&cursor));
        txn_cursor_set_to_nil(&cursor);
        BFC_ASSERT_EQUAL(HAM_TRUE, txn_cursor_is_nil(&cursor));

        BFC_ASSERT_EQUAL(0u, txn_cursor_get_flags(&cursor));
    }

    void txnOpLinkedListTest(void)
    {
        ham_txn_t *txn;
        txn_optree_t *tree;
        txn_opnode_t *node;
        txn_op_t *op;
        ham_key_t key={0};
        ham_record_t record={0};
        key.data=(void *)"hello";
        key.size=5;

        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
        tree=txn_tree_get_or_create(m_db);
        node=txn_opnode_create(m_db, &key);
        op=txn_opnode_append(txn, node, TXN_OP_INSERT_DUP, 55, &record);
        BFC_ASSERT(op!=0);

        txn_cursor_t c1={0};
        txn_cursor_set_flags(&c1, TXN_CURSOR_FLAG_COUPLED);
        txn_cursor_set_coupled_op(&c1, op);
        txn_cursor_t c2=c1;
        txn_cursor_t c3=c1;
        
        BFC_ASSERT_EQUAL((txn_cursor_t *)0, txn_op_get_cursors(op));

        txn_op_add_cursor(op, &c1);
        BFC_ASSERT_EQUAL(&c1, txn_op_get_cursors(op));
        BFC_ASSERT_EQUAL((txn_cursor_t *)0, 
                    txn_cursor_get_coupled_previous(&c1));
        BFC_ASSERT_EQUAL((txn_cursor_t *)0, 
                    txn_cursor_get_coupled_next(&c1));

        txn_op_add_cursor(op, &c2);
        BFC_ASSERT_EQUAL(&c2, txn_op_get_cursors(op));
        BFC_ASSERT_EQUAL(&c2,
                    txn_cursor_get_coupled_previous(&c1));
        BFC_ASSERT_EQUAL(&c1,
                    txn_cursor_get_coupled_next(&c2));
        BFC_ASSERT_EQUAL((txn_cursor_t *)0,
                    txn_cursor_get_coupled_previous(&c2));

        txn_op_add_cursor(op, &c3);
        BFC_ASSERT_EQUAL(&c3, txn_op_get_cursors(op));
        BFC_ASSERT_EQUAL(&c3,
                    txn_cursor_get_coupled_previous(&c2));
        BFC_ASSERT_EQUAL(&c2,
                    txn_cursor_get_coupled_next(&c3));
        BFC_ASSERT_EQUAL((txn_cursor_t *)0,
                    txn_cursor_get_coupled_previous(&c3));

        txn_op_remove_cursor(op, &c2);
        BFC_ASSERT_EQUAL(&c3, txn_op_get_cursors(op));
        BFC_ASSERT_EQUAL(&c3,
                    txn_cursor_get_coupled_previous(&c1));
        BFC_ASSERT_EQUAL(&c1,
                    txn_cursor_get_coupled_next(&c3));
        BFC_ASSERT_EQUAL((txn_cursor_t *)0,
                    txn_cursor_get_coupled_next(&c1));

        txn_op_remove_cursor(op, &c3);
        BFC_ASSERT_EQUAL(&c1, txn_op_get_cursors(op));
        BFC_ASSERT_EQUAL((txn_cursor_t *)0,
                    txn_cursor_get_coupled_previous(&c1));
        BFC_ASSERT_EQUAL((txn_cursor_t *)0,
                    txn_cursor_get_coupled_next(&c1));

        txn_op_remove_cursor(op, &c1);
        BFC_ASSERT_EQUAL((txn_cursor_t *)0, txn_op_get_cursors(op));

        txn_free_ops(txn);
        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    }

    void getKeyFromCoupledCursorTest(void)
    {
        ham_txn_t *txn;
        txn_optree_t *tree;
        txn_opnode_t *node;
        txn_op_t *op;
        ham_key_t k={0};
        ham_key_t key={0};
        ham_record_t record={0};
        key.data=(void *)"hello";
        key.size=5;

        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
        tree=txn_tree_get_or_create(m_db);
        node=txn_opnode_create(m_db, &key);
        op=txn_opnode_append(txn, node, TXN_OP_INSERT_DUP, 55, &record);
        BFC_ASSERT(op!=0);

        txn_cursor_t c={0};
        txn_cursor_set_flags(&c, TXN_CURSOR_FLAG_COUPLED);
        txn_cursor_set_coupled_op(&c, op);

        BFC_ASSERT_EQUAL(0, txn_cursor_get_key(&c, &k));
        BFC_ASSERT_EQUAL(k.size, key.size);
        BFC_ASSERT_EQUAL(0, memcmp(k.data, key.data, key.size));

        txn_free_ops(txn);
        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    }

    void getKeyFromCoupledCursorUserAllocTest(void)
    {
        ham_txn_t *txn;
        txn_optree_t *tree;
        txn_opnode_t *node;
        txn_op_t *op;
        ham_key_t k={0};
        ham_key_t key={0};
        ham_record_t record={0};
        key.data=(void *)"hello";
        key.size=5;

        char buffer[1024]={0};
        k.data=&buffer[0];
        k.flags=HAM_KEY_USER_ALLOC;

        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
        tree=txn_tree_get_or_create(m_db);
        node=txn_opnode_create(m_db, &key);
        op=txn_opnode_append(txn, node, TXN_OP_INSERT_DUP, 55, &record);
        BFC_ASSERT(op!=0);

        txn_cursor_t c={0};
        txn_cursor_set_flags(&c, TXN_CURSOR_FLAG_COUPLED);
        txn_cursor_set_coupled_op(&c, op);

        BFC_ASSERT_EQUAL(0, txn_cursor_get_key(&c, &k));
        BFC_ASSERT_EQUAL(k.size, key.size);
        BFC_ASSERT_EQUAL(0, memcmp(k.data, key.data, key.size));

        txn_free_ops(txn);
        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    }

    void getKeyFromCoupledCursorEmptyKeyTest(void)
    {
        ham_txn_t *txn;
        txn_optree_t *tree;
        txn_opnode_t *node;
        txn_op_t *op;
        ham_key_t k={0};
        ham_key_t key={0};
        ham_record_t record={0};

        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
        tree=txn_tree_get_or_create(m_db);
        node=txn_opnode_create(m_db, &key);
        op=txn_opnode_append(txn, node, TXN_OP_INSERT_DUP, 55, &record);
        BFC_ASSERT(op!=0);

        txn_cursor_t c={0};
        txn_cursor_set_flags(&c, TXN_CURSOR_FLAG_COUPLED);
        txn_cursor_set_coupled_op(&c, op);

        BFC_ASSERT_EQUAL(0, txn_cursor_get_key(&c, &k));
        BFC_ASSERT_EQUAL(k.size, key.size);
        BFC_ASSERT_EQUAL((void *)0, k.data);

        txn_free_ops(txn);
        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    }

    void getKeyFromNilCursorTest(void)
    {
        ham_txn_t *txn;
        txn_optree_t *tree;
        txn_opnode_t *node;
        txn_op_t *op;
        ham_key_t k={0};
        ham_key_t key={0};
        ham_record_t record={0};
        key.data=(void *)"hello";
        key.size=5;

        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
        tree=txn_tree_get_or_create(m_db);
        node=txn_opnode_create(m_db, &key);
        op=txn_opnode_append(txn, node, TXN_OP_INSERT_DUP, 55, &record);
        BFC_ASSERT(op!=0);

        txn_cursor_t c={0};
        txn_cursor_set_flags(&c, 0);

        BFC_ASSERT_EQUAL(HAM_CURSOR_IS_NIL, txn_cursor_get_key(&c, &k));

        txn_free_ops(txn);
        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    }

    void getRecordFromCoupledCursorTest(void)
    {
        ham_txn_t *txn;
        txn_optree_t *tree;
        txn_opnode_t *node;
        txn_op_t *op;
        ham_key_t key={0};
        ham_record_t r={0};
        ham_record_t record={0};
        record.data=(void *)"hello";
        record.size=5;

        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
        tree=txn_tree_get_or_create(m_db);
        node=txn_opnode_create(m_db, &key);
        op=txn_opnode_append(txn, node, TXN_OP_INSERT_DUP, 55, &record);
        BFC_ASSERT(op!=0);

        txn_cursor_t c={0};
        txn_cursor_set_flags(&c, TXN_CURSOR_FLAG_COUPLED);
        txn_cursor_set_coupled_op(&c, op);

        BFC_ASSERT_EQUAL(0, txn_cursor_get_record(&c, &r));
        BFC_ASSERT_EQUAL(r.size, record.size);
        BFC_ASSERT_EQUAL(0, memcmp(r.data, record.data, record.size));

        txn_free_ops(txn);
        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    }

    void getRecordFromCoupledCursorUserAllocTest(void)
    {
        ham_txn_t *txn;
        txn_optree_t *tree;
        txn_opnode_t *node;
        txn_op_t *op;
        ham_key_t key={0};
        ham_record_t r={0};
        ham_record_t record={0};
        record.data=(void *)"hello";
        record.size=5;

        char buffer[1024]={0};
        r.data=&buffer[0];
        r.flags=HAM_RECORD_USER_ALLOC;

        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
        tree=txn_tree_get_or_create(m_db);
        node=txn_opnode_create(m_db, &key);
        op=txn_opnode_append(txn, node, TXN_OP_INSERT_DUP, 55, &record);
        BFC_ASSERT(op!=0);

        txn_cursor_t c={0};
        txn_cursor_set_flags(&c, TXN_CURSOR_FLAG_COUPLED);
        txn_cursor_set_coupled_op(&c, op);

        BFC_ASSERT_EQUAL(0, txn_cursor_get_record(&c, &r));
        BFC_ASSERT_EQUAL(r.size, record.size);
        BFC_ASSERT_EQUAL(0, memcmp(r.data, record.data, record.size));

        txn_free_ops(txn);
        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    }

    void getRecordFromCoupledCursorEmptyRecordTest(void)
    {
        ham_txn_t *txn;
        txn_optree_t *tree;
        txn_opnode_t *node;
        txn_op_t *op;
        ham_key_t key={0};
        ham_record_t record={0};
        ham_record_t r={0};

        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
        tree=txn_tree_get_or_create(m_db);
        node=txn_opnode_create(m_db, &key);
        op=txn_opnode_append(txn, node, TXN_OP_INSERT_DUP, 55, &record);
        BFC_ASSERT(op!=0);

        txn_cursor_t c={0};
        txn_cursor_set_flags(&c, TXN_CURSOR_FLAG_COUPLED);
        txn_cursor_set_coupled_op(&c, op);

        BFC_ASSERT_EQUAL(0, txn_cursor_get_record(&c, &r));
        BFC_ASSERT_EQUAL(r.size, record.size);
        BFC_ASSERT_EQUAL((void *)0, r.data);

        txn_free_ops(txn);
        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    }

    void getRecordFromNilCursorTest(void)
    {
        ham_txn_t *txn;
        txn_optree_t *tree;
        txn_opnode_t *node;
        txn_op_t *op;
        ham_key_t key={0};
        ham_record_t record={0};
        ham_record_t r={0};

        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
        tree=txn_tree_get_or_create(m_db);
        node=txn_opnode_create(m_db, &key);
        op=txn_opnode_append(txn, node, TXN_OP_INSERT_DUP, 55, &record);
        BFC_ASSERT(op!=0);

        txn_cursor_t c={0};
        txn_cursor_set_flags(&c, 0);

        BFC_ASSERT_EQUAL(HAM_CURSOR_IS_NIL, txn_cursor_get_record(&c, &r));

        txn_free_ops(txn);
        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    }

    ham_status_t insert(ham_txn_t *txn, const char *key, const char *record=0,
                    ham_u32_t flags=0)
    {
        ham_key_t k={0};
        if (key) {
            k.data=(void *)key;
            k.size=strlen(key)+1;
        }
        ham_record_t r={0};
        if (record) {
            r.data=(void *)record;
            r.size=sizeof(record);
        }
        return (ham_insert(m_db, txn, &k, &r, flags));
    }

    ham_status_t insertCursor(txn_cursor_t *cursor, const char *key, 
                    const char *record=0, ham_u32_t flags=0)
    {
        ham_key_t k={0};
        if (key) {
            k.data=(void *)key;
            k.size=strlen(key)+1;
        }
        ham_record_t r={0};
        if (record) {
            r.data=(void *)record;
            r.size=strlen(record)+1;
        }
        return (txn_cursor_insert(cursor, &k, &r, flags));
    }

    ham_status_t overwriteCursor(txn_cursor_t *cursor, const char *record)
    {
        ham_record_t r={0};
        if (record) {
            r.data=(void *)record;
            r.size=strlen(record)+1;
        }
        return (txn_cursor_overwrite(cursor, &r));
    }

    ham_status_t erase(ham_txn_t *txn, const char *key)
    {
        ham_key_t k={0};
        if (key) {
            k.data=(void *)key;
            k.size=strlen(key)+1;
        }
        return (ham_erase(m_db, txn, &k, 0));
    }

    ham_status_t findCursor(txn_cursor_t *cursor, const char *key, 
                    const char *record=0)
    {
        ham_key_t k={0};
        if (key) {
            k.data=(void *)key;
            k.size=strlen(key)+1;
        }
        ham_status_t st=txn_cursor_find(cursor, &k);
        if (st)
            return (st);
        if (record) {
            ham_record_t r={0};
            BFC_ASSERT_EQUAL(0, txn_cursor_get_record(cursor, &r));
            BFC_ASSERT_EQUAL(r.size, strlen(record)+1);
            BFC_ASSERT_EQUAL(0, memcmp(r.data, record, r.size));
        }
        return (0);
    }

    ham_status_t moveCursor(txn_cursor_t *cursor, const char *key, 
                    ham_u32_t flags)
    {
        ham_status_t st=0;
        ham_key_t k={0};
        st=txn_cursor_move(cursor, flags);
        if (st)
            return (st);
        st=txn_cursor_get_key(cursor, &k);
        if (st)
            return (st);
        if (key) {
            if (strcmp((char *)k.data, key))
                return (HAM_INTERNAL_ERROR);
        }
        else {
            if (k.size!=0)
                return (HAM_INTERNAL_ERROR);
        }
        return (HAM_SUCCESS);
    }

    void initialize(txn_cursor_t *cursor)
    {
        memset(cursor, 0, sizeof(*cursor));
        txn_cursor_set_parent(cursor, m_cursor);
    }

    void findInsertEraseTest(void)
    {
        ham_txn_t *txn;

        txn_cursor_t cursor;
        initialize(&cursor);

        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));

        /* hack the cursor and attach it to the txn */
        cursor_set_txn(m_cursor, txn);

        /* insert two different keys, delete the first one */
        BFC_ASSERT_EQUAL(0, insert(txn, "key1"));
        BFC_ASSERT_EQUAL(0, erase(txn, "key1"));
        BFC_ASSERT_EQUAL(0, insert(txn, "key2"));

        /* find the first key - fails */
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, findCursor(&cursor, "key1"));

        /* insert it again */
        BFC_ASSERT_EQUAL(0, insert(txn, "key1"));
        BFC_ASSERT_EQUAL(0, findCursor(&cursor, "key1"));

        /* find second key */
        BFC_ASSERT_EQUAL(0, findCursor(&cursor, "key2"));

        /* reset cursor hack */
        cursor_set_txn(m_cursor, 0);

        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    }

    void findInsertEraseOverwriteTest(void)
    {
        ham_txn_t *txn;

        txn_cursor_t cursor;
        initialize(&cursor);

        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));

        /* hack the cursor and attach it to the txn */
        cursor_set_txn(m_cursor, txn);

        /* insert a key and overwrite it twice */
        BFC_ASSERT_EQUAL(0, insert(txn, "key1", "rec1"));
        BFC_ASSERT_EQUAL(0, insert(txn, "key1", "rec2", HAM_OVERWRITE));
        BFC_ASSERT_EQUAL(0, insert(txn, "key1", "rec3", HAM_OVERWRITE));

        /* find the first key */
        BFC_ASSERT_EQUAL(0, findCursor(&cursor, "key1"));

        /* erase it, then insert it again */
        BFC_ASSERT_EQUAL(0, erase(txn, "key1"));
        BFC_ASSERT_EQUAL(0, insert(txn, "key1", "rec4", HAM_OVERWRITE));
        BFC_ASSERT_EQUAL(0, findCursor(&cursor, "key1"));

        /* reset cursor hack */
        cursor_set_txn(m_cursor, 0);

        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    }

    void findInsertTest(void)
    {
        ham_txn_t *txn;

        txn_cursor_t cursor;
        initialize(&cursor);

        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));

        /* hack the cursor and attach it to the txn */
        cursor_set_txn(m_cursor, txn);

        /* insert two different keys */
        BFC_ASSERT_EQUAL(0, insert(txn, "key1"));
        BFC_ASSERT_EQUAL(0, insert(txn, "key2"));

        /* find the first key */
        BFC_ASSERT_EQUAL(0, findCursor(&cursor, "key1"));

        /* now the cursor is coupled to this key */
        BFC_ASSERT_EQUAL((unsigned)TXN_CURSOR_FLAG_COUPLED, 
                    txn_cursor_get_flags(&cursor));
        txn_op_t *op=txn_cursor_get_coupled_op(&cursor);
        ham_key_t *key=txn_opnode_get_key(txn_op_get_node(op));
        BFC_ASSERT_EQUAL(5, key->size);
        BFC_ASSERT_EQUAL(0, strcmp((char *)key->data, "key1"));

        /* now the key is coupled; find second key */
        BFC_ASSERT_EQUAL(0, findCursor(&cursor, "key2"));

        /* and the cursor is still coupled */
        BFC_ASSERT_EQUAL((unsigned)TXN_CURSOR_FLAG_COUPLED, 
                    txn_cursor_get_flags(&cursor));
        op=txn_cursor_get_coupled_op(&cursor);
        key=txn_opnode_get_key(txn_op_get_node(op));
        BFC_ASSERT_EQUAL(5, key->size);
        BFC_ASSERT_EQUAL(0, strcmp((char *)key->data, "key2"));

        /* reset cursor hack */
        cursor_set_txn(m_cursor, 0);

        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    }

    void moveFirstTest(void)
    {
        ham_txn_t *txn;

        txn_cursor_t cursor;
        initialize(&cursor);

        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));

        /* hack the cursor and attach it to the txn */
        cursor_set_txn(m_cursor, txn);

        /* insert a few different keys */
        BFC_ASSERT_EQUAL(0, insert(txn, "key1"));
        BFC_ASSERT_EQUAL(0, insert(txn, "key2"));
        BFC_ASSERT_EQUAL(0, insert(txn, "key3"));

        /* find the first key (with a nil cursor) */
        BFC_ASSERT_EQUAL(0, moveCursor(&cursor, "key1", HAM_CURSOR_FIRST));

        /* now the cursor is coupled to this key */
        BFC_ASSERT_EQUAL((unsigned)TXN_CURSOR_FLAG_COUPLED, 
                    txn_cursor_get_flags(&cursor));
        txn_op_t *op=txn_cursor_get_coupled_op(&cursor);
        ham_key_t *key=txn_opnode_get_key(txn_op_get_node(op));
        BFC_ASSERT_EQUAL(5, key->size);
        BFC_ASSERT_EQUAL(0, strcmp((char *)key->data, "key1"));

        /* do it again with a coupled cursor */
        BFC_ASSERT_EQUAL(0, moveCursor(&cursor, "key1", HAM_CURSOR_FIRST));

        /* reset cursor hack */
        cursor_set_txn(m_cursor, 0);

        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    }

    void moveFirstInEmptyTreeTest(void)
    {
        ham_txn_t *txn;

        txn_cursor_t cursor;
        initialize(&cursor);

        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));

        /* hack the cursor and attach it to the txn */
        cursor_set_txn(m_cursor, txn);

        /* find the first key */
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, 
                    moveCursor(&cursor, "key1", HAM_CURSOR_FIRST));

        /* now the cursor is nil */
        BFC_ASSERT_EQUAL(HAM_TRUE, txn_cursor_is_nil(&cursor));

        /* reset cursor hack */
        cursor_set_txn(m_cursor, 0);

        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    }

    void findCreateConflictTest(void)
    {
        ham_txn_t *txn, *txn2;

        txn_cursor_t cursor;
        initialize(&cursor);

        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn2, m_db, 0));

        /* hack the cursor and attach it to the txn */
        cursor_set_txn(m_cursor, txn);

        /* insert a key, then erase it */
        BFC_ASSERT_EQUAL(0, insert(txn2, "key1"));
        BFC_ASSERT_EQUAL(HAM_TXN_CONFLICT, findCursor(&cursor, "key1"));

        /* cursor must be nil */
        BFC_ASSERT_EQUAL(HAM_TRUE, txn_cursor_is_nil(&cursor));

        /* reset cursor hack */
        cursor_set_txn(m_cursor, 0);

        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn2, 0));
    }

    void moveNextWithNilCursorTest(void)
    {
        ham_txn_t *txn;

        txn_cursor_t cursor;
        initialize(&cursor);

        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));

        /* hack the cursor and attach it to the txn */
        cursor_set_txn(m_cursor, txn);

        /* make sure that the cursor is nil */
        BFC_ASSERT_EQUAL(HAM_TRUE, txn_cursor_is_nil(&cursor));

        BFC_ASSERT_EQUAL(HAM_CURSOR_IS_NIL, 
                    moveCursor(&cursor, 0, HAM_CURSOR_NEXT));

        /* reset cursor hack */
        cursor_set_txn(m_cursor, 0);

        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    }

    void moveNextTest(void)
    {
        ham_txn_t *txn;

        txn_cursor_t cursor;
        initialize(&cursor);

        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));

        /* hack the cursor and attach it to the txn */
        cursor_set_txn(m_cursor, txn);

        /* insert a few different keys */
        BFC_ASSERT_EQUAL(0, insert(txn, "key1"));
        BFC_ASSERT_EQUAL(0, insert(txn, "key2"));
        BFC_ASSERT_EQUAL(0, insert(txn, "key3"));

        /* find the first key */
        BFC_ASSERT_EQUAL(0, findCursor(&cursor, "key1"));

        /* move next */
        BFC_ASSERT_EQUAL(0, moveCursor(&cursor, "key2", HAM_CURSOR_NEXT));

        /* now the cursor is coupled to this key */
        BFC_ASSERT_EQUAL((unsigned)TXN_CURSOR_FLAG_COUPLED, 
                    txn_cursor_get_flags(&cursor));
        txn_op_t *op=txn_cursor_get_coupled_op(&cursor);
        ham_key_t *key=txn_opnode_get_key(txn_op_get_node(op));
        BFC_ASSERT_EQUAL(5, key->size);
        BFC_ASSERT_EQUAL(0, strcmp((char *)key->data, "key2"));

        /* now the key is coupled; move next once more */
        BFC_ASSERT_EQUAL(0, moveCursor(&cursor, "key3", HAM_CURSOR_NEXT));

        /* and the cursor is still coupled */
        BFC_ASSERT_EQUAL((unsigned)TXN_CURSOR_FLAG_COUPLED, 
                    txn_cursor_get_flags(&cursor));
        op=txn_cursor_get_coupled_op(&cursor);
        key=txn_opnode_get_key(txn_op_get_node(op));
        BFC_ASSERT_EQUAL(5, key->size);
        BFC_ASSERT_EQUAL(0, strcmp((char *)key->data, "key3"));

        /* reset cursor hack */
        cursor_set_txn(m_cursor, 0);

        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    }

    void moveNextAfterEndTest(void)
    {
        ham_txn_t *txn;

        txn_cursor_t cursor;
        initialize(&cursor);

        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));

        /* hack the cursor and attach it to the txn */
        cursor_set_txn(m_cursor, txn);

        /* insert one key */
        BFC_ASSERT_EQUAL(0, insert(txn, "key1"));

        /* find the first key */
        BFC_ASSERT_EQUAL(0, findCursor(&cursor, "key1"));

        /* move next */
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, 
                    moveCursor(&cursor, "key2", HAM_CURSOR_NEXT));

        /* reset cursor hack */
        cursor_set_txn(m_cursor, 0);

        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    }

    void moveNextSkipEraseTest(void)
    {
        ham_txn_t *txn;

        txn_cursor_t cursor;
        initialize(&cursor);

        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));

        /* hack the cursor and attach it to the txn */
        cursor_set_txn(m_cursor, txn);

        /* insert/erase keys */
        BFC_ASSERT_EQUAL(0, insert(txn, "key1"));
        BFC_ASSERT_EQUAL(0, insert(txn, "key2"));
        BFC_ASSERT_EQUAL(0, erase(txn, "key2"));
        BFC_ASSERT_EQUAL(0, insert(txn, "key3"));

        /* find the first key */
        BFC_ASSERT_EQUAL(0, findCursor(&cursor, "key1"));

        /* move next */
        BFC_ASSERT_EQUAL(0, moveCursor(&cursor, "key3", HAM_CURSOR_NEXT));

        /* reached the end */
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, 
                    moveCursor(&cursor, "key3", HAM_CURSOR_NEXT));

        /* reset cursor hack */
        cursor_set_txn(m_cursor, 0);

        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    }

    void moveNextSkipEraseInNodeTest(void)
    {
        ham_txn_t *txn;

        txn_cursor_t cursor;
        initialize(&cursor);

        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));

        /* hack the cursor and attach it to the txn */
        cursor_set_txn(m_cursor, txn);

        /* insert/erase keys */
        BFC_ASSERT_EQUAL(0, insert(txn, "key1"));
        BFC_ASSERT_EQUAL(0, insert(txn, "key2"));
        BFC_ASSERT_EQUAL(0, erase(txn, "key2"));
        BFC_ASSERT_EQUAL(0, insert(txn, "key2"));
        BFC_ASSERT_EQUAL(0, erase(txn, "key2"));
        BFC_ASSERT_EQUAL(0, insert(txn, "key2"));
        BFC_ASSERT_EQUAL(0, erase(txn, "key2"));
        BFC_ASSERT_EQUAL(0, insert(txn, "key3"));

        /* find the first key */
        BFC_ASSERT_EQUAL(0, findCursor(&cursor, "key1"));

        /* move next */
        BFC_ASSERT_EQUAL(0, moveCursor(&cursor, "key3", HAM_CURSOR_NEXT));

        /* reached the end */
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, 
                    moveCursor(&cursor, "key3", HAM_CURSOR_NEXT));

        /* reset cursor hack */
        cursor_set_txn(m_cursor, 0);

        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    }

    void moveLastTest(void)
    {
        ham_txn_t *txn;

        txn_cursor_t cursor;
        initialize(&cursor);

        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));

        /* hack the cursor and attach it to the txn */
        cursor_set_txn(m_cursor, txn);

        /* insert a few different keys */
        BFC_ASSERT_EQUAL(0, insert(txn, "key1"));
        BFC_ASSERT_EQUAL(0, insert(txn, "key2"));
        BFC_ASSERT_EQUAL(0, insert(txn, "key3"));

        /* find the last key (with a nil cursor) */
        BFC_ASSERT_EQUAL(0, moveCursor(&cursor, "key3", HAM_CURSOR_LAST));

        /* now the cursor is coupled to this key */
        BFC_ASSERT_EQUAL((unsigned)TXN_CURSOR_FLAG_COUPLED, 
                    txn_cursor_get_flags(&cursor));
        txn_op_t *op=txn_cursor_get_coupled_op(&cursor);
        ham_key_t *key=txn_opnode_get_key(txn_op_get_node(op));
        BFC_ASSERT_EQUAL(5, key->size);
        BFC_ASSERT_EQUAL(0, strcmp((char *)key->data, "key3"));

        /* do it again with a coupled cursor */
        BFC_ASSERT_EQUAL(0, moveCursor(&cursor, "key3", HAM_CURSOR_LAST));

        /* reset cursor hack */
        cursor_set_txn(m_cursor, 0);

        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    }

    void moveLastInEmptyTreeTest(void)
    {
        ham_txn_t *txn;

        txn_cursor_t cursor;
        initialize(&cursor);

        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));

        /* hack the cursor and attach it to the txn */
        cursor_set_txn(m_cursor, txn);

        /* find the first key */
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, 
                    moveCursor(&cursor, "key1", HAM_CURSOR_LAST));

        /* now the cursor is nil */
        BFC_ASSERT_EQUAL(HAM_TRUE, txn_cursor_is_nil(&cursor));

        /* reset cursor hack */
        cursor_set_txn(m_cursor, 0);

        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    }

    void movePrevWithNilCursorTest(void)
    {
        ham_txn_t *txn;

        txn_cursor_t cursor;
        initialize(&cursor);

        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));

        /* hack the cursor and attach it to the txn */
        cursor_set_txn(m_cursor, txn);

        /* make sure that the cursor is nil */
        BFC_ASSERT_EQUAL(HAM_TRUE, txn_cursor_is_nil(&cursor));

        BFC_ASSERT_EQUAL(HAM_CURSOR_IS_NIL, 
                    moveCursor(&cursor, 0, HAM_CURSOR_PREVIOUS));

        /* reset cursor hack */
        cursor_set_txn(m_cursor, 0);

        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    }

    void movePrevTest(void)
    {
        ham_txn_t *txn;

        txn_cursor_t cursor;
        initialize(&cursor);

        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));

        /* hack the cursor and attach it to the txn */
        cursor_set_txn(m_cursor, txn);

        /* insert a few different keys */
        BFC_ASSERT_EQUAL(0, insert(txn, "key1"));
        BFC_ASSERT_EQUAL(0, insert(txn, "key2"));
        BFC_ASSERT_EQUAL(0, insert(txn, "key3"));

        /* find the last key */
        BFC_ASSERT_EQUAL(0, findCursor(&cursor, "key3"));

        /* move previous */
        BFC_ASSERT_EQUAL(0, moveCursor(&cursor, "key2", HAM_CURSOR_PREVIOUS));

        /* now the cursor is coupled to this key */
        BFC_ASSERT_EQUAL((unsigned)TXN_CURSOR_FLAG_COUPLED, 
                    txn_cursor_get_flags(&cursor));
        txn_op_t *op=txn_cursor_get_coupled_op(&cursor);
        ham_key_t *key=txn_opnode_get_key(txn_op_get_node(op));
        BFC_ASSERT_EQUAL(5, key->size);
        BFC_ASSERT_EQUAL(0, strcmp((char *)key->data, "key2"));

        /* now the key is coupled; move previous once more */
        BFC_ASSERT_EQUAL(0, moveCursor(&cursor, "key1", HAM_CURSOR_PREVIOUS));

        /* and the cursor is still coupled */
        BFC_ASSERT_EQUAL((unsigned)TXN_CURSOR_FLAG_COUPLED, 
                    txn_cursor_get_flags(&cursor));
        op=txn_cursor_get_coupled_op(&cursor);
        key=txn_opnode_get_key(txn_op_get_node(op));
        BFC_ASSERT_EQUAL(5, key->size);
        BFC_ASSERT_EQUAL(0, strcmp((char *)key->data, "key1"));

        /* reset cursor hack */
        cursor_set_txn(m_cursor, 0);

        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    }

    void movePrevAfterEndTest(void)
    {
        ham_txn_t *txn;

        txn_cursor_t cursor;
        initialize(&cursor);

        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));

        /* hack the cursor and attach it to the txn */
        cursor_set_txn(m_cursor, txn);

        /* insert one key */
        BFC_ASSERT_EQUAL(0, insert(txn, "key1"));

        /* find the first key */
        BFC_ASSERT_EQUAL(0, findCursor(&cursor, "key1"));

        /* move previous */
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, 
                    moveCursor(&cursor, "key2", HAM_CURSOR_PREVIOUS));

        /* reset cursor hack */
        cursor_set_txn(m_cursor, 0);

        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    }

    void movePrevSkipEraseTest(void)
    {
        ham_txn_t *txn;

        txn_cursor_t cursor;
        initialize(&cursor);

        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));

        /* hack the cursor and attach it to the txn */
        cursor_set_txn(m_cursor, txn);

        /* insert/erase keys */
        BFC_ASSERT_EQUAL(0, insert(txn, "key1"));
        BFC_ASSERT_EQUAL(0, insert(txn, "key2"));
        BFC_ASSERT_EQUAL(0, erase(txn, "key2"));
        BFC_ASSERT_EQUAL(0, insert(txn, "key3"));

        /* find the first key */
        BFC_ASSERT_EQUAL(0, findCursor(&cursor, "key3"));

        /* move previous */
        BFC_ASSERT_EQUAL(0, moveCursor(&cursor, "key1", HAM_CURSOR_PREVIOUS));

        /* reached the end */
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, 
                    moveCursor(&cursor, "key1", HAM_CURSOR_PREVIOUS));

        /* reset cursor hack */
        cursor_set_txn(m_cursor, 0);

        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    }

    void movePrevSkipEraseInNodeTest(void)
    {
        ham_txn_t *txn;

        txn_cursor_t cursor;
        initialize(&cursor);

        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));

        /* hack the cursor and attach it to the txn */
        cursor_set_txn(m_cursor, txn);

        /* insert/erase keys */
        BFC_ASSERT_EQUAL(0, insert(txn, "key1"));
        BFC_ASSERT_EQUAL(0, insert(txn, "key2"));
        BFC_ASSERT_EQUAL(0, erase(txn, "key2"));
        BFC_ASSERT_EQUAL(0, insert(txn, "key2"));
        BFC_ASSERT_EQUAL(0, erase(txn, "key2"));
        BFC_ASSERT_EQUAL(0, insert(txn, "key2"));
        BFC_ASSERT_EQUAL(0, erase(txn, "key2"));
        BFC_ASSERT_EQUAL(0, insert(txn, "key3"));

        /* find the last key */
        BFC_ASSERT_EQUAL(0, findCursor(&cursor, "key3"));

        /* move previous */
        BFC_ASSERT_EQUAL(0, moveCursor(&cursor, "key1", HAM_CURSOR_PREVIOUS));

        /* reached the end */
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, 
                    moveCursor(&cursor, "key1", HAM_CURSOR_PREVIOUS));

        /* reset cursor hack */
        cursor_set_txn(m_cursor, 0);

        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    }

    bool cursorIsCoupled(txn_cursor_t *cursor, const char *k)
    {
        BFC_ASSERT_EQUAL((unsigned)TXN_CURSOR_FLAG_COUPLED, 
                    txn_cursor_get_flags(cursor));
        txn_op_t *op=txn_cursor_get_coupled_op(cursor);
        ham_key_t *key=txn_opnode_get_key(txn_op_get_node(op));
        if (strlen(k)+1!=key->size)
            return (false);
        if (strcmp((char *)key->data, k))
            return (false);
        return (true);
    }

    void insertKeysTest(void)
    {
        ham_txn_t *txn;

        txn_cursor_t cursor;
        initialize(&cursor);

        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));

        /* hack the cursor and attach it to the txn */
        cursor_set_txn(m_cursor, txn);

        /* insert a few different keys */
        BFC_ASSERT_EQUAL(0, insertCursor(&cursor, "key1"));
        BFC_ASSERT_EQUAL(0, insertCursor(&cursor, "key2"));
        BFC_ASSERT_EQUAL(0, insertCursor(&cursor, "key3"));

        /* make sure that the keys exist and that the cursor is coupled */
        BFC_ASSERT_EQUAL(0, findCursor(&cursor, "key1"));
        BFC_ASSERT_EQUAL(true, cursorIsCoupled(&cursor, "key1"));
        BFC_ASSERT_EQUAL(0, findCursor(&cursor, "key2"));
        BFC_ASSERT_EQUAL(true, cursorIsCoupled(&cursor, "key2"));
        BFC_ASSERT_EQUAL(0, findCursor(&cursor, "key3"));
        BFC_ASSERT_EQUAL(true, cursorIsCoupled(&cursor, "key3"));

        /* reset cursor hack */
        cursor_set_txn(m_cursor, 0);

        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    }

    void negativeInsertKeysTest(void)
    {
        ham_txn_t *txn;

        txn_cursor_t cursor;
        initialize(&cursor);

        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));

        /* hack the cursor and attach it to the txn */
        cursor_set_txn(m_cursor, txn);

        /* insert a key twice - creates a duplicate key */
        BFC_ASSERT_EQUAL(0, insertCursor(&cursor, "key1"));
        BFC_ASSERT_EQUAL(HAM_DUPLICATE_KEY, insertCursor(&cursor, "key1"));

        /* reset cursor hack */
        cursor_set_txn(m_cursor, 0);

        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    }

    void insertOverwriteKeysTest(void)
    {
        ham_txn_t *txn;

        txn_cursor_t cursor;
        initialize(&cursor);

        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));

        /* hack the cursor and attach it to the txn */
        cursor_set_txn(m_cursor, txn);

        /* insert/overwrite keys */
        BFC_ASSERT_EQUAL(0, insertCursor(&cursor, "key1"));
        BFC_ASSERT_EQUAL(0, insertCursor(&cursor, "key1", 0, HAM_OVERWRITE));
        BFC_ASSERT_EQUAL(0, insertCursor(&cursor, "key1", 0, HAM_OVERWRITE));

        /* make sure that the key exists and that the cursor is coupled */
        BFC_ASSERT_EQUAL(0, findCursor(&cursor, "key1"));
        BFC_ASSERT_EQUAL(true, cursorIsCoupled(&cursor, "key1"));

        /* reset cursor hack */
        cursor_set_txn(m_cursor, 0);

        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    }

    void insertCreateConflictTest(void)
    {
        ham_txn_t *txn, *txn2;

        txn_cursor_t cursor;
        initialize(&cursor);

        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn2, m_db, 0));

        /* hack the cursor and attach it to the txn */
        cursor_set_txn(m_cursor, txn);

        /* insert/overwrite keys */
        BFC_ASSERT_EQUAL(0, insert(txn2, "key1"));
        BFC_ASSERT_EQUAL(HAM_TXN_CONFLICT, insertCursor(&cursor, "key1"));

        /* cursor must be nil */
        BFC_ASSERT_EQUAL(HAM_TRUE, txn_cursor_is_nil(&cursor));

        /* reset cursor hack */
        cursor_set_txn(m_cursor, 0);

        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn2, 0));
    }

    void eraseKeysTest(void)
    {
        ham_txn_t *txn;

        txn_cursor_t cursor;
        initialize(&cursor);

        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));

        /* hack the cursor and attach it to the txn */
        cursor_set_txn(m_cursor, txn);

        /* insert/erase a few different keys */
        BFC_ASSERT_EQUAL(0, insertCursor(&cursor, "key1"));
        BFC_ASSERT_EQUAL(true, cursorIsCoupled(&cursor, "key1"));
        BFC_ASSERT_EQUAL(false, txn_cursor_is_nil(&cursor));
        BFC_ASSERT_EQUAL(0, txn_cursor_erase(&cursor));

        /* make sure that the keys do not exist and that the cursor is nil */
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, findCursor(&cursor, "key1"));
        BFC_ASSERT_EQUAL(true, txn_cursor_is_nil(&cursor));

        BFC_ASSERT_EQUAL(0, insertCursor(&cursor, "key2"));
        BFC_ASSERT_EQUAL(0, txn_cursor_erase(&cursor));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, findCursor(&cursor, "key2"));
        BFC_ASSERT_EQUAL(true, txn_cursor_is_nil(&cursor));

        /* reset cursor hack */
        cursor_set_txn(m_cursor, 0);

        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    }

    void negativeEraseKeysTest(void)
    {
        ham_txn_t *txn;

        txn_cursor_t cursor;
        initialize(&cursor);

        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));

        /* hack the cursor and attach it to the txn */
        cursor_set_txn(m_cursor, txn);

        /* erase a key that does not exist */
        BFC_ASSERT_EQUAL(0, insertCursor(&cursor, "key1"));
        BFC_ASSERT_EQUAL(0, erase(txn, "key1"));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, txn_cursor_erase(&cursor));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, txn_cursor_erase(&cursor));

        /* reset cursor hack */
        cursor_set_txn(m_cursor, 0);

        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    }

    void negativeEraseKeysNilTest(void)
    {
        ham_txn_t *txn;

        txn_cursor_t cursor;
        initialize(&cursor);

        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));

        /* hack the cursor and attach it to the txn */
        cursor_set_txn(m_cursor, txn);

        /* erase a key with a cursor that is nil */
        BFC_ASSERT_EQUAL(HAM_CURSOR_IS_NIL, txn_cursor_erase(&cursor));

        /* reset cursor hack */
        cursor_set_txn(m_cursor, 0);

        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    }

    void overwriteRecordsTest(void)
    {
        ham_txn_t *txn;

        txn_cursor_t cursor;
        initialize(&cursor);

        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));

        /* hack the cursor and attach it to the txn */
        cursor_set_txn(m_cursor, txn);

        /* insert a key and overwrite the record */
        BFC_ASSERT_EQUAL(0, insertCursor(&cursor, "key1", "rec1"));
        BFC_ASSERT_EQUAL(true, cursorIsCoupled(&cursor, "key1"));
        BFC_ASSERT_EQUAL(0, findCursor(&cursor, "key1", "rec1"));
        BFC_ASSERT_EQUAL(true, cursorIsCoupled(&cursor, "key1"));
        BFC_ASSERT_EQUAL(0, overwriteCursor(&cursor, "rec2"));
        BFC_ASSERT_EQUAL(true, cursorIsCoupled(&cursor, "key1"));
        BFC_ASSERT_EQUAL(0, findCursor(&cursor, "key1", "rec2"));
        BFC_ASSERT_EQUAL(true, cursorIsCoupled(&cursor, "key1"));
        BFC_ASSERT_EQUAL(0, overwriteCursor(&cursor, "rec3"));
        BFC_ASSERT_EQUAL(true, cursorIsCoupled(&cursor, "key1"));
        BFC_ASSERT_EQUAL(0, findCursor(&cursor, "key1", "rec3"));
        BFC_ASSERT_EQUAL(true, cursorIsCoupled(&cursor, "key1"));

        /* reset cursor hack */
        cursor_set_txn(m_cursor, 0);

        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    }

    void overwriteRecordsNilCursorTest(void)
    {
        ham_txn_t *txn;

        txn_cursor_t cursor;
        initialize(&cursor);

        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));

        /* hack the cursor and attach it to the txn */
        cursor_set_txn(m_cursor, txn);

        BFC_ASSERT_EQUAL(HAM_CURSOR_IS_NIL, overwriteCursor(&cursor, "rec2"));

        /* reset cursor hack */
        cursor_set_txn(m_cursor, 0);

        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    }

    void flushCoupledOpTest(void)
    {
        ham_txn_t *txn;

        txn_cursor_t cursor[5]={{0}};
        for (int i=0; i<5; i++)
            initialize(&cursor[i]);

        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));

        /* hack the cursor and attach it to the txn */
        cursor_set_txn(m_cursor, txn);

        BFC_ASSERT_EQUAL(0, insertCursor(&cursor[0], "key1", "rec1"));
        for (int i=1; i<5; i++)
            BFC_ASSERT_EQUAL(0, findCursor(&cursor[i], "key1"));

        /* all cursors are now coupled */
        for (int i=0; i<5; i++)
            BFC_ASSERT_EQUAL(false, txn_cursor_is_nil(&cursor[i]));

        /* reset cursor hack */
        cursor_set_txn(m_cursor, 0);

        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));

        /* all cursors are now uncoupled */
        for (int i=0; i<5; i++)
            BFC_ASSERT_EQUAL(true, txn_cursor_is_nil(&cursor[i]));
    }
};

BFC_REGISTER_FIXTURE(TxnCursorTest);

