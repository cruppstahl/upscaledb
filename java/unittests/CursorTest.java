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

public class CursorTest extends TestCase {

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

  private Environment m_env;
  private Database m_db;

  protected void setUp() throws Exception {
    super.setUp();

    try {
      m_env = new Environment();
      m_env.create("jtest.db");
      m_db = m_env.createDatabase((short)1, Const.UPS_ENABLE_DUPLICATE_KEYS);
    }
    catch (DatabaseException err) {
      fail("DatabaseException " + err.getMessage());
    }
  }

  protected void tearDown() throws Exception {
    super.tearDown();
    m_db.close();
    m_env.close();
  }

  public void testCursorDatabaseLong() {
    Cursor c = new Cursor(m_db, 0x13);
    assertEquals(0x13, c.getHandle());
  }

  public void testCreate() {
    Cursor c;
    try {
      c = new Cursor(m_db);
      c.close();
    }
    catch (DatabaseException err) {
      fail("DatabaseException " + err.getMessage());
    }
  }

  public void testCloneCursor() {
    Cursor c1, c2;
    try {
      c1 = new Cursor(m_db);
      c2 = c1.cloneCursor();
      c1.close();
      c2.close();
    }
    catch (DatabaseException err) {
      fail("DatabaseException " + err.getMessage());
    }
  }

  public void testMove() {
    byte[] key = new byte[10];
    byte[] record = new byte[10];
    try {
      Cursor c = new Cursor(m_db);
      key[0] = 0;
      m_db.insert(key, record);
      key[0] = 1;
      m_db.insert(key, record);
      key[0] = 2;
      m_db.insert(key, record);
      key[0] = 3;
      m_db.insert(key, record);
      key[0] = 4;
      m_db.insert(key, record);
      c.move(Const.UPS_CURSOR_NEXT);
      c.move(Const.UPS_CURSOR_NEXT);
      c.move(Const.UPS_CURSOR_PREVIOUS);
      c.move(Const.UPS_CURSOR_LAST);
      c.move(Const.UPS_CURSOR_FIRST);
    }
    catch (DatabaseException err) {
      fail("DatabaseException " + err.getMessage());
    }
  }

  public void testMoveNegative() {
    try {
      Cursor c = new Cursor(m_db);
      c.move(Const.UPS_CURSOR_NEXT);
    }
    catch (DatabaseException err) {
      assertEquals(Const.UPS_KEY_NOT_FOUND, err.getErrno());
    }
  }

  public void testMoveFirst() {
    byte[] key = new byte[10];
    byte[] record = new byte[10];
    try {
      Cursor c = new Cursor(m_db);
      m_db.insert(key, record);
      c.moveFirst();
    }
    catch (DatabaseException err) {
      fail("DatabaseException "+err.getMessage());
    }
  }

  public void testMoveLast() {
    byte[] key = new byte[10];
    byte[] record = new byte[10];
    try {
      Cursor c = new Cursor(m_db);
      m_db.insert(key, record);
      c.moveLast();
    }
    catch (DatabaseException err) {
      fail("DatabaseException "+err.getMessage());
    }
  }

  public void testMoveNext() {
    byte[] key = new byte[10];
    byte[] record = new byte[10];
    try {
      Cursor c = new Cursor(m_db);
      m_db.insert(key, record);
      c.moveNext();
    }
    catch (DatabaseException err) {
      fail("DatabaseException "+err.getMessage());
    }
  }

  public void testMovePrevious() {
    byte[] key = new byte[10];
    byte[] record = new byte[10];
    try {
      Cursor c = new Cursor(m_db);
      m_db.insert(key, record);
      c.movePrevious();
    }
    catch (DatabaseException err) {
      fail("DatabaseException "+err.getMessage());
    }
  }

  public void assertByteArrayEquals(byte[] r1, byte[] r2) {
    assertEquals(r1.length, r2.length);

    for (int i = 0; i < r1.length; i++) {
      assertEquals(r1[i], r2[i]);
    }
  }

  public void testGetKey() {
    byte[] key = new byte[10];
    key[0] = 0x13;
    byte[] record = new byte[10];
    try {
      Cursor c = new Cursor(m_db);
      m_db.insert(key, record);
      c.moveFirst();
      byte[] k = c.getKey();
      assertByteArrayEquals(key, k);
    }
    catch (DatabaseException err) {
      fail("DatabaseException " + err.getMessage());
    }
  }

  public void testGetRecord() {
    byte[] key = new byte[10];
    byte[] record = new byte[10];
    record[0] = 0x14;
    try {
      Cursor c = new Cursor(m_db);
      m_db.insert(key, record);
      c.moveFirst();
      byte[] r = c.getRecord();
      assertByteArrayEquals(record, r);
    }
    catch (DatabaseException err) {
      fail("DatabaseException " + err.getMessage());
    }
  }

  public void testOverwrite() {
    byte[] key = new byte[10];
    byte[] record1 = new byte[10];
    byte[] record2 = new byte[10];
    record1[0] = 0x14;
    record2[0] = 0x15;
    try {
      Cursor c = new Cursor(m_db);
      m_db.insert(key, record1);
      c.moveFirst();
      c.overwrite(record2);
      byte[] r2 = c.getRecord();
      assertByteArrayEquals(record2, r2);
    }
    catch (DatabaseException err) {
      fail("DatabaseException "+err.getMessage());
    }
  }

  public void testFind() {
    byte[] key = new byte[10];
    byte[] record = new byte[10];
    key[0] = 0x14;
    try {
      Cursor c = new Cursor(m_db);
      m_db.insert(key, record);
      c.find(key);
      byte[] k = c.getKey();
      byte[] r = c.getRecord();
      assertByteArrayEquals(key, k);
      assertByteArrayEquals(record, r);
    }
    catch (DatabaseException err) {
      fail("DatabaseException " + err.getMessage());
    }
  }

  public void testInsertByteArrayByteArray() {
    byte[] key = new byte[10];
    byte[] record = new byte[10];
    key[0] = 0x14;
    try {
      Cursor c = new Cursor(m_db);
      c.insert(key, record);
      c.find(key);
      byte[] k = c.getKey();
      byte[] r = c.getRecord();
      assertByteArrayEquals(key, k);
      assertByteArrayEquals(record, r);
    }
    catch (DatabaseException err) {
      fail("DatabaseException "+err.getMessage());
    }
  }

  public void testInsertByteArrayByteArrayInt() {
    byte[] key = new byte[10];
    byte[] record = new byte[10];
    key[0] = 0x14;
    try {
      Cursor c = new Cursor(m_db);
      c.insert(key, record);
      record[0] = 0x14;
      c.insert(key, record, Const.UPS_OVERWRITE);
      record[0] = 0x15;
      c.insert(key, record, Const.UPS_OVERWRITE);
      byte[] r = c.getRecord();
      assertByteArrayEquals(record, r);
    }
    catch (DatabaseException err) {
      fail("DatabaseException " + err.getMessage());
    }
  }

  public void testErase() {
    byte[] key = new byte[10];
    byte[] record = new byte[10];
    Cursor c = null;
    try {
      c = new Cursor(m_db);
      m_db.insert(key, record);
      c.find(key);
      c.erase();
    }
    catch (DatabaseException err) {
      fail("DatabaseException " + err.getMessage());
    }
    try {
      c.find(key);
    }
    catch (DatabaseException err) {
      assertEquals(Const.UPS_KEY_NOT_FOUND, err.getErrno());
    }
  }

  public void testGetDuplicateCount() {
    byte[] key = new byte[10];
    byte[] record = new byte[10];
    try {
      Cursor c = new Cursor(m_db);
      c.insert(key, record, Const.UPS_DUPLICATE);
      assertEquals(1, c.getDuplicateCount());
      record[0] = 0x14;
      c.insert(key, record, Const.UPS_DUPLICATE);
      assertEquals(2, c.getDuplicateCount());
      record[0] = 0x15;
      c.insert(key, record, Const.UPS_DUPLICATE);
      assertEquals(3, c.getDuplicateCount());
    }
    catch (DatabaseException err) {
      fail("DatabaseException " + err.getMessage());
    }
  }

  public void testSetComparator() throws Exception {
    byte[] k = new byte[5];
    byte[] r = new byte[5];
    MyComparator cmp = new MyComparator();
    Cursor c;
    Parameter[] params = new Parameter[1];
    params[0] = new Parameter();
    params[0].name = Const.UPS_PARAM_KEY_TYPE;
    params[0].value = Const.UPS_TYPE_CUSTOM;

    try {
      tearDown();
      m_env = new Environment();
      m_env.create("jtest.db");
      m_db = m_env.createDatabase((short)1, Const.UPS_ENABLE_DUPLICATE_KEYS,
                      params);
      c = new Cursor(m_db);
      m_db.setComparator(cmp);
      c.insert(k, r);
      k[0] = 1;
      c.insert(k, r);
      k[0] = 2;
      c.insert(k, r);
      k[0] = 3;
      c.insert(k, r);
      k[0] = 4;
    }
    catch (DatabaseException err) {
      fail("Exception " + err);
    }
    assertEquals(4, cmp.m_counter);
  }
}
