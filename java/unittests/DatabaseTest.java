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

public class DatabaseTest extends TestCase {
    
    private class MyErrorHandler implements ErrorHandler {
        public int m_counter;
        
        public void handleMessage(int level, String message) {
            m_counter++;
        }
    }

    public void testSetErrorHandler() {
        Database db=new Database();
        MyErrorHandler eh=new MyErrorHandler();
        try {
            Database.setErrorHandler(eh);
            db.create(null);
        }
        catch (DatabaseException err) {
            assertEquals(Const.HAM_INV_PARAMETER, err.getErrno());
            assertEquals(1, eh.m_counter);
        }
        Database.setErrorHandler(null);
    }

    public void testGetVersion() {
        Version v=Database.getVersion();
        assertEquals(1, v.major);
        assertEquals(1, v.minor);
    }

    public void testGetLicense() {
        License l=Database.getLicense();
        assertEquals("", l.licensee); /* this fails if you have 
                                        a licensed version */
        assertEquals("hamsterdb embedded storage", l.product);
    }

    public void testDatabase() {
        Database db=new Database();
        db.close();
    }

    public void testCreateString() {
        Database db=new Database();
        try {
            db.create("jtest.db");
            db.close();
            db.open("jtest.db");
            db.close();
        }
        catch (DatabaseException err) {
            fail("Exception "+err);
        }
    }
    
    public void testCreateInvalidParameters() {
        Parameter[] params=new Parameter[3];
        params[1]=new Parameter();
        params[2]=new Parameter();
        Database db=new Database();
        try {
            db.create("jtest.db", 0, 0, params);
        }
        catch (NullPointerException e) {
        }
        catch (DatabaseException err) {
            fail("Exception "+err);
        }
    }

    public void testCreateStringIntIntParametersArray() {
        Database db=new Database();
        try {
            db.create("jtest.db", 0, 0644, null);
            db.close();
            db.open("jtest.db");
            db.close();
        }
        catch (DatabaseException err) {
            fail("Exception "+err);
        }
    }

    public void testGetDatabaseException() {
        Database db=new Database();
        try {
            db.create("jtest.db", 1234, 0); /* Triggers INV_PARAMETER */
        }
        catch (DatabaseException err) {
            assertEquals(Const.HAM_INV_PARAMETER, err.getErrno());
        }
        assertEquals(Const.HAM_INV_PARAMETER, db.getError());
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
        byte[] k=new byte[5];
        byte[] r=new byte[5];
        Database db=new Database();
        MyComparator cmp=new MyComparator();
        try {
            db.create("jtest.db");
            db.setComparator(cmp);
            db.insert(k, r);
            k[0]=1;
            db.insert(k, r);
            k[0]=2;
            db.insert(k, r);
        }
        catch (DatabaseException err) {
            fail("Exception "+err);
        }
        assertEquals(2, cmp.m_counter);
        db.close();
    }

    public void testSetDuplicateComparator() {
        byte[] k=new byte[5];
        byte[] r=new byte[5];
        Database db=new Database();
        MyDupeComparator cmp=new MyDupeComparator();
        try {
            db.create("jtest.db", Const.HAM_ENABLE_DUPLICATES
                                  |Const.HAM_SORT_DUPLICATES);
            db.setDuplicateComparator(cmp);
            db.insert(k, r, Const.HAM_DUPLICATE);
            r[0]=1;
            db.insert(k, r, Const.HAM_DUPLICATE);
            r[0]=2;
            db.insert(k, r, Const.HAM_DUPLICATE);
        }
        catch (DatabaseException err) {
            fail("Exception "+err);
        }
        assertEquals(3, cmp.m_counter);
        db.close();
    }

    public void testSetDefaultDuplicateComparator() {
        byte[] k=new byte[5];
        byte[] r=new byte[5];
        Database db=new Database();
        try {
            db.create("jtest.db", Const.HAM_ENABLE_DUPLICATES
                                  |Const.HAM_SORT_DUPLICATES);
            db.setDuplicateComparator(null);
            db.insert(k, r, Const.HAM_DUPLICATE);
            r[0]=1;
            db.insert(k, r, Const.HAM_DUPLICATE);
            r[0]=2;
            db.insert(k, r, Const.HAM_DUPLICATE);
        }
        catch (DatabaseException err) {
            fail("Exception "+err);
        }
        db.close();
    }

    public void testGetParameters() {
        byte[] k=new byte[5];
        byte[] r=new byte[5];
        Parameter[] params=new Parameter[10];
        for (int i=0; i<params.length; i++) {
            params[i]=new Parameter();
        }
        params[0].name=Const.HAM_PARAM_CACHESIZE;
        params[1].name=Const.HAM_PARAM_KEYSIZE;
        params[2].name=Const.HAM_PARAM_PAGESIZE;
        params[3].name=Const.HAM_PARAM_MAX_ENV_DATABASES;
        params[4].name=Const.HAM_PARAM_DBNAME;
        params[5].name=Const.HAM_PARAM_GET_FLAGS;
        params[6].name=Const.HAM_PARAM_GET_FILEMODE;
        params[7].name=Const.HAM_PARAM_GET_FILENAME;
        params[8].name=Const.HAM_PARAM_GET_KEYS_PER_PAGE;
        params[9].name=Const.HAM_PARAM_GET_DAM;
        Database db=new Database();
        try {
            db.create("jtest.db");
            db.getParameters(params);
        }
        catch (DatabaseException err) {
            fail("Exception "+err);
        }
        assertEquals(2*1024*1024, params[0].value);
        assertEquals(21, params[1].value);
        if (params[2].value!=1024*16 && params[2].value!=1024*64)
            assertEquals(16*1024, params[2].value);
        assertEquals(16, params[3].value);
        assertEquals(61440, params[4].value);
        assertEquals(524288, params[5].value);
        assertEquals(420, params[6].value);
        assertEquals("jtest.db", params[7].stringValue);
        assertEquals(510, params[8].value);
        assertEquals(1, params[9].value);
        db.close();
    }

    public void testGetKeyCount() {
        byte[] k=new byte[5];
        byte[] r=new byte[5];
        Database db=new Database();
        try {
            db.create("jtest.db");
            assertEquals(0, db.getKeyCount());
            db.insert(k, r);
            assertEquals(1, db.getKeyCount());
            k[0]=1;
            db.insert(k, r);
            assertEquals(2, db.getKeyCount());
            k[0]=2;
            db.insert(k, r);
            assertEquals(3, db.getKeyCount());
        }
        catch (DatabaseException err) {
            fail("Exception "+err);
        }
        db.close();
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
        byte[] k=new byte[25];
        byte[] r=new byte[5];
        Database db=new Database();
        MyPrefixComparator cmp=new MyPrefixComparator();
        try {
            db.create("jtest.db");
            db.setPrefixComparator(cmp);
            db.insert(k, r);
            k[0]=1;
            db.insert(k, r);
            k[0]=2;
            db.insert(k, r);
            k[0]=3;
            db.insert(k, r);
        }
        catch (DatabaseException err) {
            fail("Exception "+err);
        }
        assertEquals(4, cmp.m_counter);
        db.close();
    }

}
