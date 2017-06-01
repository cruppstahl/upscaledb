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

public class DatabaseTest extends TestCase {

  public void assertByteArrayEquals(byte[] r1, byte[] r2) {
    assertEquals(r1.length, r2.length);

    for (int i = 0; i < r1.length; i++) {
      assertEquals(r1[i], r2[i]);
    }
  }

  public void testGetVersion() {
    Version v = Database.getVersion();
    assertEquals(2, v.major);
    assertEquals(2, v.minor);
  }

  public void testDatabase() {
    Database db = new Database(0);
    db.close();
  }

  public void testGetDatabaseException() {
    Environment env = new Environment();
    try {
      env.create("jtest.db");
      Database db = env.createDatabase((short)1, 1234);
    }
    catch (DatabaseException err) {
      assertEquals(Const.UPS_INV_PARAMETER, err.getErrno());
    }
    env.close();
  }

  private class MyComparator implements CompareCallback
  {
    public int m_counter;

    public int compare(byte[] b1, byte[] b2) {
      m_counter++;
      if (b1.length < b2.length)
        return (-1);
      if (b1.length > b2.length)
        return (+1);
      for (int i = b1.length; --i >= 0; ) {
        if (b1[i] < b2[i])
          return (-1);
        if (b1[i] > b2[i])
          return (+1);
      }
      return 0;
    }
  }

  public void testSetComparator1() {
    byte[] k = new byte[5];
    byte[] r = new byte[5];
    Environment env = new Environment();
    Database db;
    MyComparator cmp = new MyComparator();
    Database.registerCompare("cmp", cmp);
    try {
      env.create("jtest.db");
      Parameter[] params = new Parameter[2];
      params[0] = new Parameter(Const.UPS_PARAM_KEY_TYPE,
                                Const.UPS_TYPE_CUSTOM);
      params[1] = new Parameter(Const.UPS_PARAM_CUSTOM_COMPARE_NAME,
                                "cmp");
      db = env.createDatabase((short)1, 0, params);
      db.insert(k, r);
      k[0] = 1;
      db.insert(k, r);
      k[0] = 2;
      db.insert(k, r);
      db.close();
      env.close();

      k = new byte[5];
      r = new byte[5];
      env.open("jtest.db");
      db = env.openDatabase((short)1);
      assertByteArrayEquals(db.find(k), r);
      k[0] = 1;
      assertByteArrayEquals(db.find(k), r);
      k[0] = 2;
      assertByteArrayEquals(db.find(k), r);
      db.close();
    }
    catch (DatabaseException err) {
      fail("Exception " + err);
    }
    env.close();
  }

  public void testSetComparator2() {
    byte[] k = new byte[5];
    byte[] r = new byte[5];
    Environment env = new Environment();
    Database db;
    MyComparator cmp = new MyComparator();
    try {
      env.create("jtest.db");
      Parameter[] params = new Parameter[1];
      params[0] = new Parameter();
      params[0].name = Const.UPS_PARAM_KEY_TYPE;
      params[0].value = Const.UPS_TYPE_CUSTOM;
      db = env.createDatabase((short)1, 0, params);
      db.setComparator(cmp);
      db.insert(k, r);
      k[0] = 1;
      db.insert(k, r);
      k[0] = 2;
      db.insert(k, r);
      db.close();
    }
    catch (DatabaseException err) {
      env.close();
      fail("Exception " + err);
    }
    assertEquals(2, cmp.m_counter);
    env.close();
  }

  public void testGetParameters() {
    byte[] k = new byte[5];
    byte[] r = new byte[5];
    Parameter[] params = new Parameter[10];
    for (int i = 0; i < params.length; i++) {
      params[i] = new Parameter();
    }
    params[0].name = Const.UPS_PARAM_KEYSIZE;
    params[1].name = Const.UPS_PARAM_DATABASE_NAME;
    params[2].name = Const.UPS_PARAM_FLAGS;
    params[3].name = Const.UPS_PARAM_MAX_KEYS_PER_PAGE;
    Database db;
    Environment env = new Environment();
    try {
      env.create("jtest.db");
      db = env.createDatabase((short)1);
      db.getParameters(params);
      db.close();
    }
    catch (DatabaseException err) {
      fail("Exception " + err);
    }
    assertEquals(Const.UPS_KEY_SIZE_UNLIMITED, params[0].value);
    assertEquals(1, params[1].value);
    assertEquals(0, params[2].value);
    assertEquals(441, params[3].value);
    env.close();
  }

  public void testGetKeyCount() {
    byte[] k = new byte[5];
    byte[] r = new byte[5];
    Database db;
    Environment env = new Environment();
    try {
      env.create("jtest.db");
      db = env.createDatabase((short)1);
      assertEquals(0, db.getCount());
      db.insert(k, r);
      assertEquals(1, db.getCount());
      k[0] = 1;
      db.insert(k, r);
      assertEquals(2, db.getCount());
      k[0] = 2;
      db.insert(k, r);
      assertEquals(3, db.getCount());
      db.close();
    }
    catch (DatabaseException err) {
      fail("Exception "+err);
    }
    env.close();
  }

  public void testBulkOperations() {
    byte[] k1 = new byte[] {0x11};
    byte[] r1 = new byte[] {0x11};

    byte[] k2 = new byte[] {0x22};
    byte[] r2 = new byte[] {0x22};

    byte[] k3 = new byte[] {0x33};
    byte[] r3 = new byte[] {0x33};

    byte[] r4 = null;
    byte[] r5 = null;

    Operation[] operations = new Operation[] {
      new Operation(Const.UPS_OP_INSERT, k1, r1, 0),
      new Operation(Const.UPS_OP_INSERT, k2, r2, 0),
      new Operation(Const.UPS_OP_INSERT, k3, r3, 0),
      new Operation(Const.UPS_OP_FIND,   k3, r4, 0),
      new Operation(Const.UPS_OP_ERASE,  k3, null, 0),
      new Operation(Const.UPS_OP_FIND,   k3, r5, 0)
    };

    Database db;
    Environment env = new Environment();
    try {
      env.create("jtest.db");
      db = env.createDatabase((short)1);
      assertEquals(0, db.getCount());
      db.bulkOperations(null, operations);
      assertByteArrayEquals(operations[3].record, r3);
      assertEquals(0, operations[3].result);
      assertEquals(0, operations[4].result);
      assertEquals(Const.UPS_KEY_NOT_FOUND, operations[5].result);
      assertEquals(2, db.getCount());
      db.close();
    }
    catch (DatabaseException err) {
      fail("Exception "+err);
    }
    env.close();
  }

  public void testBulkApproxMatching() {
    byte[] k1 = new byte[] {0x11};
    byte[] r1 = new byte[] {0x11};

    byte[] k2 = new byte[] {0x22};
    byte[] r2 = new byte[] {0x22};

    byte[] k3 = new byte[] {0x33};
    byte[] r3 = new byte[] {0x33};

    byte[] r4 = null;
    byte[] r5 = null;

    Operation[] operations = new Operation[] {
      new Operation(Const.UPS_OP_INSERT, k1, r1, 0),
      new Operation(Const.UPS_OP_INSERT, k2, r2, 0),
      new Operation(Const.UPS_OP_INSERT, k3, r3, 0),
      new Operation(Const.UPS_OP_FIND,   k2, r4, Const.UPS_FIND_LT_MATCH),
      new Operation(Const.UPS_OP_FIND,   k2, r5, Const.UPS_FIND_GT_MATCH),
    };

    Database db;
    Environment env = new Environment();
    try {
      env.create("jtest.db");
      db = env.createDatabase((short)1);
      assertEquals(0, db.getCount());
      db.bulkOperations(null, operations);
      assertByteArrayEquals(operations[3].record, r1);
      assertByteArrayEquals(operations[4].record, r3);
      assertEquals(0, operations[3].result);
      assertEquals(0, operations[4].result);
      assertEquals(3, db.getCount());
      db.close();
    }
    catch (DatabaseException err) {
      fail("Exception "+err);
    }
    env.close();
  }
}
