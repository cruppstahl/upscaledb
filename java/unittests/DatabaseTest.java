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

public class DatabaseTest extends TestCase {

  private class MyErrorHandler implements ErrorHandler {
    public int m_counter;

    public void handleMessage(int level, String message) {
      m_counter++;
    }
  }

  public void testSetErrorHandler() {
    Environment env = new Environment();
    MyErrorHandler eh = new MyErrorHandler();
    try {
      Database.setErrorHandler(eh);
      env.create(null);
    }
    catch (DatabaseException err) {
      assertEquals(Const.HAM_INV_PARAMETER, err.getErrno());
      assertEquals(1, eh.m_counter);
    }
    Database.setErrorHandler(null);
  }

  public void testGetVersion() {
    Version v=Database.getVersion();
    assertEquals(2, v.major);
    assertEquals(0, v.minor);
  }

  public void testGetLicense() {
    License l = Database.getLicense();
    assertEquals("", l.licensee); /* this fails if you have
                    a licensed version */
    assertEquals("hamsterdb embedded storage", l.product);
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
      assertEquals(Const.HAM_INV_PARAMETER, err.getErrno());
    }
    env.close();
  }

  private class MyComparator implements CompareCallback
  {
    public int m_counter;

    public int compare(byte[] lhs, byte[] rhs) {
      m_counter++;
      return m_counter; /* need to return different values, or
                ham_insert thinks we're inserting
                duplicates */
    }
  }

  private class MyDupeComparator implements DuplicateCompareCallback
  {
    public int m_counter;

    public int compare(byte[] lhs, byte[] rhs) {
      m_counter++;
      return m_counter; /* need to return different values, or
                ham_insert thinks we're inserting
                duplicates */
    }
  }

  public void testSetComparator() {
    byte[] k = new byte[5];
    byte[] r = new byte[5];
    Environment env = new Environment();
    Database db;
    MyComparator cmp = new MyComparator();
    try {
      env.create("jtest.db");
      db = env.createDatabase((short)1);
      db.setComparator(cmp);
      db.insert(k, r);
      k[0] = 1;
      db.insert(k, r);
      k[0] = 2;
      db.insert(k, r);
      db.close();
    }
    catch (DatabaseException err) {
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
    params[0].name = Const.HAM_PARAM_KEYSIZE;
    params[1].name = Const.HAM_PARAM_DATABASE_NAME;
    params[2].name = Const.HAM_PARAM_FLAGS;
    params[3].name = Const.HAM_PARAM_MAX_KEYS_PER_PAGE;
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
    assertEquals(21, params[0].value);
    assertEquals(1, params[1].value);
    assertEquals(0, params[2].value);
    assertEquals(510, params[3].value);
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
      assertEquals(0, db.getKeyCount());
      db.insert(k, r);
      assertEquals(1, db.getKeyCount());
      k[0] = 1;
      db.insert(k, r);
      assertEquals(2, db.getKeyCount());
      k[0] = 2;
      db.insert(k, r);
      assertEquals(3, db.getKeyCount());
      db.close();
    }
    catch (DatabaseException err) {
      fail("Exception "+err);
    }
    env.close();
  }

  private class MyPrefixComparator implements PrefixCompareCallback
  {
    public int m_counter;

    public int compare(byte[] lhs, int lhs_real_size,
        byte[] rhs, int rhs_real_size) {
      return ++m_counter; /* need to return different values,
        or ham_insert thinks we're inserting
        duplicates */
    }
  }

  public void testSetPrefixComparator() {
    byte[] k = new byte[25];
    byte[] r = new byte[5];
    Database db;
    Environment env = new Environment();
    MyPrefixComparator cmp = new MyPrefixComparator();
    try {
      env.create("jtest.db");
      db = env.createDatabase((short)1);
      db.setPrefixComparator(cmp);
      db.insert(k, r);
      k[0] = 1;
      db.insert(k, r);
      k[0] = 2;
      db.insert(k, r);
      k[0] = 3;
      db.insert(k, r);
      db.close();
    }
    catch (DatabaseException err) {
      fail("Exception " + err);
    }
    assertEquals(4, cmp.m_counter);
    env.close();
  }
}
