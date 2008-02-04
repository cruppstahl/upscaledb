package de.crupp.hamsterdb;

import junit.framework.TestCase;

public class EnvironmentTest extends TestCase {

	public void testCreateString() {
		Environment env=new Environment();
		try {
			env.create("jtest.db");
			env.close();
		}
		catch (Error err) {
			fail("Unexpected error "+err.getMessage());
		}
	}

	public void testCreateStringNegative() {
		Environment env=new Environment();
		try {
			env.create(null);
			env.close();
		}
		catch (Error err) {
			assertEquals(Const.HAM_INV_PARAMETER, err.getErrno());
		}
	}

	public void testCreateStringInt() {
		Environment env=new Environment();
		try {
			env.create(null, Const.HAM_IN_MEMORY_DB);
			env.close();
		}
		catch (Error err) {
			fail("Exception "+err);
		}
	}

	public void testCreateStringIntInt() {
		Environment env=new Environment();
		try {
			env.create("jtest.db", 0, 0664);
			env.close();
		}
		catch (Error err) {
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
		catch (Error err) {
			fail("Exception "+err);
		}
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
		catch (Error err) {
			fail("Exception "+err);
		}
	}

	public void testOpenString() {
		Environment env=new Environment();
		try {
			env.create("jtest.db");
			env.close();
			env.open("jtest.db");
			env.close();
		}
		catch (Error err) {
			fail("Unexpected error "+err.getMessage());
		}
	}

	public void testOpenStringNegative() {
		Environment env=new Environment();
		try {
			env.open("jtest.dbxxxx");
		}
		catch (Error err) {
			assertEquals(Const.HAM_FILE_NOT_FOUND, err.getErrno());
		}
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
		catch (Error err) {
			fail("Exception "+err);
		}
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
		catch (Error err) {
			fail("Exception "+err);
		}
	}

	public void testCreateDatabaseNegative() {
		Environment env=new Environment();
		try {
			env.create("jtest.db");
			Database db=env.createDatabase((short)0);
			db.close();
		}
		catch (Error err) {
			assertEquals(Const.HAM_INV_PARAMETER, err.getErrno());
		}
	}

	public void testOpenDatabaseNegative() {
		Environment env=new Environment();
		try {
			env.create("jtest.db");
			Database db=env.openDatabase((short)100);
			db.close();
		}
		catch (Error err) {
			assertEquals(Const.HAM_DATABASE_NOT_FOUND, err.getErrno());
		}
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
		catch (Error err) {
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
		catch (Error err) {
			fail("Exception "+err);
		}			
		try {
			Database db=env.openDatabase((short)13);
			db.close();
		}
		catch (Error err) {
			assertEquals(Const.HAM_DATABASE_NOT_FOUND, err.getErrno());
		}
	}

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
		}
		catch (Error err) {
			fail("Exception "+err);
		}			
	}

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
		catch (Error err) {
			fail("Exception "+err);
		}			
	}

}
