/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
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
      c.close();
      db.close();
      env.close();
    }
    catch (DatabaseException err) {
      fail("DatabaseException " + err.getMessage());
    }
  }
}
