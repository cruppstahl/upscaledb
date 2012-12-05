/**
 * Copyright (C) 2005-2012 Christoph Rupp (chris@crupp.de).
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
    Environment env = new Environment();
    Transaction txn;
    try {
      env.create("jtest.db", Const.HAM_ENABLE_TRANSACTIONS);
      txn = env.begin();
      txn.abort();
      env.close();
    }
    catch (DatabaseException err) {
      fail("Exception " + err);
    }
  }

  public void testBeginInsertAbort() {
    byte[] k = new byte[5];
    byte[] r = new byte[5];
    Environment env = new Environment();
    Database db;
    Transaction txn;
    try {
      env.create("jtest.db", Const.HAM_ENABLE_TRANSACTIONS);
      db = env.createDatabase((short)1);
      txn = env.begin();
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
      env.close();
    }
    catch (DatabaseException err) {
      fail("Exception " + err);
    }
  }

  public void testBeginEraseAbort() {
    byte[] k = new byte[5];
    byte[] r = new byte[5];
    Environment env = new Environment();
    Transaction txn;
    try {
      env.create("jtest.db", Const.HAM_ENABLE_TRANSACTIONS);
      Database db = env.createDatabase((short)1);
      db.insert(k, r);
      txn = env.begin();
      db.erase(txn, k);
      txn.abort();
      env.close();
    }
    catch (DatabaseException err) {
      fail("Exception " + err);
    }
  }

  public void testBeginCommit() {
    Environment env = new Environment();
    Transaction txn;
    try {
      env.create("jtest.db", Const.HAM_ENABLE_TRANSACTIONS);
      txn = env.begin();
      txn.commit();
      env.close();
    }
    catch (DatabaseException err) {
      fail("Exception " + err);
    }
  }

  public void testBeginInsertCommit() {
    byte[] k = new byte[5];
    byte[] r = new byte[5];
    Environment env = new Environment();
    Transaction txn;
    try {
      env.create("jtest.db", Const.HAM_ENABLE_TRANSACTIONS);
      Database db = env.createDatabase((short)1);
      txn = env.begin();
      db.insert(txn, k, r);
      db.find(txn, k); // ok
      txn.commit();
      db.find(k); // ok
      env.close();
    }
    catch (DatabaseException err) {
      fail("Exception " + err);
    }
  }

  public void testCursor() {
    Environment env = new Environment();
    Transaction txn;
    byte[] key = new byte[10];
    byte[] record = new byte[10];
    try {
      env.create("jtest.db", Const.HAM_ENABLE_TRANSACTIONS);
      Database db = env.createDatabase((short)1);
      txn = env.begin();
      Cursor c = new Cursor(db, txn);
      db.insert(txn, key, record);
      c.moveFirst();
    }
    catch (DatabaseException err) {
      fail("DatabaseException " + err.getMessage());
    }
  }
}
