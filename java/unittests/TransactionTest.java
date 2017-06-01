/*
 * Copyright (C) 2005-2017 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * See the file COPYING for License information.
 */

import de.crupp.upscaledb.*;
import junit.framework.TestCase;

public class TransactionTest extends TestCase {

  public void testBeginAbort() {
    Environment env = new Environment();
    Transaction txn;
    try {
      env.create("jtest.db", Const.UPS_ENABLE_TRANSACTIONS);
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
      env.create("jtest.db", Const.UPS_ENABLE_TRANSACTIONS);
      db = env.createDatabase((short)1);
      txn = env.begin();
      db.insert(txn, k, r);
      db.find(txn, k); // ok
      txn.abort();
      try {
        db.find(k); // fail
      }
      catch (DatabaseException err) {
        assertEquals(Const.UPS_KEY_NOT_FOUND, err.getErrno());
      }
      db.close();
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
      env.create("jtest.db", Const.UPS_ENABLE_TRANSACTIONS);
      txn = env.begin();
      txn.commit();
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
      env.create("jtest.db", Const.UPS_ENABLE_TRANSACTIONS);
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

  public void testBeginInsertCommit() {
    byte[] k = new byte[5];
    byte[] r = new byte[5];
    Environment env = new Environment();
    Transaction txn;
    try {
      env.create("jtest.db", Const.UPS_ENABLE_TRANSACTIONS);
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
      env.create("jtest.db", Const.UPS_ENABLE_TRANSACTIONS);
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
