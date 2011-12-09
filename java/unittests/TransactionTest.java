/**
 * Copyright (C) 2005-2010 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See file COPYING.GPL2 and COPYING.GPL3 for License information.
 *
 */

import de.crupp.hamsterdb.*;
import junit.framework.TestCase;

public class TransactionTest extends TestCase {

    public void testBeginAbort() {
        Database db=new Database();
        Transaction txn;
        try {
            db.create("jtest.db", Const.HAM_ENABLE_TRANSACTIONS);
            txn=db.begin();
            txn.abort();
            db.close();
        }
        catch (DatabaseException err) {
            fail("Exception "+err);
        }
    }

    public void testBeginInsertAbort() {
        byte[] k=new byte[5];
        byte[] r=new byte[5];
        Database db=new Database();
        Transaction txn;
        try {
            db.create("jtest.db", Const.HAM_ENABLE_TRANSACTIONS);
            txn=db.begin();
            db.insert(txn, k, r);
            db.find(txn, k); // ok
            txn.abort();
            try {
                db.find(k); // fail
            }
            catch (DatabaseException err) {
                assertEquals(Const.HAM_KEY_NOT_FOUND, err.getErrno());
            }    
            db.close();
        }
        catch (DatabaseException err) {
            fail("Exception "+err);
        }
    }

    public void testBeginEraseAbort() {
        byte[] k=new byte[5];
        byte[] r=new byte[5];
        Database db=new Database();
        Transaction txn;
        try {
            db.create("jtest.db", Const.HAM_ENABLE_TRANSACTIONS);
            db.insert(k, r);
            txn=db.begin();
            // db.find(k); // ok, but why? -> LIMITS_REACHED???
            db.erase(txn, k);
            txn.abort();
            //db.find(k); // XXX throws KEY_NOT_FOUND - why??? 
            db.close();
        }
        catch (DatabaseException err) {
            fail("Exception "+err);
        }
    }

    public void testBeginCommit() {
        Database db=new Database();
        Transaction txn;
        try {
            db.create("jtest.db", Const.HAM_ENABLE_TRANSACTIONS);
            txn=db.begin();
            txn.commit();
            db.close();
        }
        catch (DatabaseException err) {
            fail("Exception "+err);
        }
    }

    public void testBeginInsertCommit() {
        byte[] k=new byte[5];
        byte[] r=new byte[5];
        Database db=new Database();
        Transaction txn;
        try {
            db.create("jtest.db", Const.HAM_ENABLE_TRANSACTIONS);
            txn=db.begin();
            db.insert(txn, k, r);
            db.find(txn, k); // ok
            txn.commit();
            db.find(k); // ok
            db.close();
        }
        catch (DatabaseException err) {
            fail("Exception "+err);
        }
    }

    public void testCursor() {
        Database db=new Database();
        Transaction txn;
        byte[] key=new byte[10];
        byte[] record=new byte[10];
        try {
            db.create("jtest.db", Const.HAM_ENABLE_TRANSACTIONS);
            txn=db.begin();
            Cursor c=new Cursor(db, txn);
            db.insert(txn, key, record);
            c.moveFirst();
        }
        catch (DatabaseException err) {
            fail("DatabaseException "+err.getMessage());
        }
    }
}
