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

public class EnvironmentTest extends TestCase {

    public void testCreateString() {
        Environment env=new Environment();
        try {
            env.create("jtest.db");
            env.close();
        }
        catch (DatabaseException err) {
            fail("Unexpected error "+err.getMessage());
        }
    }

    public void testCreateStringNegative() {
        Environment env=new Environment();
        try {
            env.create(null);
        }
        catch (DatabaseException err) {
            assertEquals(Const.HAM_INV_PARAMETER, err.getErrno());
        }
        env.close();
    }

    public void testCreateStringInt() {
        Environment env=new Environment();
        try {
            env.create(null, Const.HAM_IN_MEMORY_DB);
            env.close();
        }
        catch (DatabaseException err) {
            fail("Exception "+err);
        }
    }

    public void testCreateStringIntInt() {
        Environment env=new Environment();
        try {
            env.create("jtest.db", 0, 0664);
            env.close();
        }
        catch (DatabaseException err) {
            fail("Exception "+err);
        }
    }

    public void testCreateStringIntIntParametersArray() {
        Parameter[] params=new Parameter[1];
        params[0]=new Parameter();
        params[0].name=Const.HAM_PARAM_CACHESIZE;
        params[0].value=1000;
        Environment env=new Environment();
        try {
            env.create("jtest.db", 0, 0, params);
        }
        catch (DatabaseException err) {
            fail("Exception "+err);
        }
        env.close();
    }

    public void testCreateStringIntIntParametersArrayNegative() {
        Parameter[] params=new Parameter[3];
        params[1]=new Parameter();
        params[2]=new Parameter();
        Environment env=new Environment();
        try {
            env.create("jtest.db", 0, 0, params);
        }
        catch (NullPointerException e) {
        }
        catch (DatabaseException err) {
            fail("Exception "+err);
        }
        env.close();
    }

    public void testOpenString() {
        Environment env=new Environment();
        try {
            env.create("jtest.db");
            env.close();
            env.open("jtest.db");
        }
        catch (DatabaseException err) {
            fail("Unexpected error "+err.getMessage());
        }
        env.close();
    }

    public void testOpenStringNegative() {
        Environment env=new Environment();
        try {
            env.open("jtest.db");
        }
        catch (DatabaseException err) {
            assertEquals(Const.HAM_FILE_NOT_FOUND, err.getErrno());
        }
        env.close();
    }

    public void testOpenStringIntParametersArray() {
        Parameter[] params=new Parameter[1];
        params[0]=new Parameter();
        params[0].name=Const.HAM_PARAM_CACHESIZE;
        params[0].value=1000;
        Environment env=new Environment();
        try {
            env.create("jtest.db");
            env.close();
            env.open("jtest.db", 0, params);
            env.close();
        }
        catch (DatabaseException err) {
            fail("Exception "+err);
        }
        env.close();
    }

    public void assertByteArrayEquals(byte[] r1, byte[] r2) {
        assertEquals(r1.length, r2.length);

        for (int i=0; i<r1.length; i++) {
            assertEquals(r1[i], r2[i]);
        }
    }

    public void testCreateDatabaseShort() {
        Environment env=new Environment();
        byte[] key=new byte[10];
        byte[] rec=new byte[10];
        rec[0]=0x13;
        try {
            env.create("jtest.db");
            Database db=env.createDatabase((short)13);
            db.insert(key, rec);
            env.close();
            env.open("jtest.db");
            db=env.openDatabase((short)13);
            byte[] r=db.find(key);
            assertByteArrayEquals(rec, r);
            env.close();
        }
        catch (DatabaseException err) {
            fail("Exception "+err);
        }
        env.close();
    }

    public void testFlushEnvironment() {
        Environment env=new Environment();
        byte[] key=new byte[10];
        byte[] rec=new byte[10];
        rec[0]=0x13;
        try {
            env.create("jtest.db");
            Database db=env.createDatabase((short)13);
            db.insert(key, rec);
            env.flush();
        }
        catch (DatabaseException err) {
            fail("Exception "+err);
        }
        env.close();
    }

    public void testCreateDatabaseNegative() {
        Environment env=new Environment();
        try {
            env.create("jtest.db");
            Database db=env.createDatabase((short)0);
            db.close();
        }
        catch (DatabaseException err) {
            assertEquals(Const.HAM_INV_PARAMETER, err.getErrno());
        }
        env.close();
    }

    public void testOpenDatabaseNegative() {
        Environment env=new Environment();
        try {
            env.create("jtest.db");
            Database db=env.openDatabase((short)100);
            db.close();
        }
        catch (DatabaseException err) {
            assertEquals(Const.HAM_DATABASE_NOT_FOUND, err.getErrno());
        }
        env.close();
    }

    public void testRenameDatabase() {
        Environment env=new Environment();
        byte[] key=new byte[10];
        byte[] rec=new byte[10];
        rec[0]=0x13;
        try {
            env.create("jtest.db");
            Database db=env.createDatabase((short)13);
            db.insert(key, rec);
            env.close();
            env.open("jtest.db");
            env.renameDatabase((short)13, (short)15);
            db=env.openDatabase((short)15);
            byte[] r=db.find(key);
            assertByteArrayEquals(rec, r);
            env.close();
        }
        catch (DatabaseException err) {
            fail("Exception "+err);
        }
    }

    public void testEraseDatabase() {
        Environment env=new Environment();
        byte[] key=new byte[10];
        byte[] rec=new byte[10];
        rec[0]=0x13;
        try {
            env.create("jtest.db");
            Database db=env.createDatabase((short)13);
            db.insert(key, rec);
            db.close();
            env.eraseDatabase((short)13);
        }
        catch (DatabaseException err) {
            fail("Exception "+err);
        }
        try {
            Database db=env.openDatabase((short)13);
            db.close();
        }
        catch (DatabaseException err) {
            assertEquals(Const.HAM_DATABASE_NOT_FOUND, err.getErrno());
        }
        env.close();
    }

/* TODO fails...
    public void testEnableEncryption() {
        Environment env=new Environment();
        byte[] key=new byte[10];
        byte[] rec=new byte[10];
        byte[] aeskey=new byte[16];
        rec[0]=0x13;
        try {
            env.create("jtest.db");
            env.enableEncryption(aeskey);
            Database db=env.createDatabase((short)13);
            db.insert(key, rec);
            db.close();
            db=env.openDatabase((short)13);
            byte[] r=db.find(key);
            assertByteArrayEquals(r, rec);
            env.close();
        }
        catch (DatabaseException err) {
            fail("Exception "+err);
        }
    }
*/

    public void testGetDatabaseNames() {
        Environment env=new Environment();
        short names[];
        Database db;
        try {
            env.create("jtest.db");
            names=env.getDatabaseNames();
            assertEquals(0, names.length);
            db=env.createDatabase((short)13);
            names=env.getDatabaseNames();
            assertEquals(1, names.length);
            assertEquals(13, names[0]);
            db=env.createDatabase((short)14);
            names=env.getDatabaseNames();
            assertEquals(2, names.length);
            assertEquals(13, names[0]);
            assertEquals(14, names[1]);
        }
        catch (DatabaseException err) {
            fail("Exception "+err);
        }
        env.close();
    }

    public void testGetParameters() {
        Environment env=new Environment();
        byte[] k=new byte[5];
        byte[] r=new byte[5];
        Parameter[] params=new Parameter[10];
        for (int i=0; i<params.length; i++) {
            params[i]=new Parameter();
        }
        params[0].name=Const.HAM_PARAM_CACHESIZE;
        params[1].name=Const.HAM_PARAM_PAGESIZE;
        params[2].name=Const.HAM_PARAM_MAX_ENV_DATABASES;
        params[3].name=Const.HAM_PARAM_GET_FLAGS;
        params[4].name=Const.HAM_PARAM_GET_FILEMODE;
        params[5].name=Const.HAM_PARAM_GET_FILENAME;
        try {
            env.create("jtest.db");
            env.getParameters(params);
        }
        catch (DatabaseException err) {
            fail("Exception "+err);
        }
        assertEquals(2*1024*1024, params[0].value);
        if (params[1].value!=1024*16 && params[1].value!=1024*64)
            assertEquals(16*1024, params[1].value);
        assertEquals(16, params[2].value);
        assertEquals(0, params[3].value);
        assertEquals(420, params[4].value);
        assertEquals("jtest.db", params[5].stringValue);
        env.close();
    }
}
