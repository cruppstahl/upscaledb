/**
 * Copyright (C) 2005-2008 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See file COPYING.GPL2 and COPYING.GPL3 for License information.
 *
 */

package de.crupp.hamsterdb;

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
		catch (Error err) {
			assertEquals(Const.HAM_INV_PARAMETER, err.getErrno());
			assertEquals(1, eh.m_counter);
		}
		Database.setErrorHandler(null);
	}

	public void testGetVersion() {
		Version v=Database.getVersion();
		assertEquals(1, v.major);
		assertEquals(0, v.minor);
	}

	public void testGetLicense() {
		License l=Database.getLicense();
		assertEquals("", l.licensee); /* this fails if you have 
										a licensed version */
		assertEquals("hamsterdb storage", l.product);
	}

	public void testDatabase() {
		Database db=new Database();
	}

	public void testDatabaseLong() {
		Database db=new Database(15);
		assertEquals(15, db.getHandle());
	}

	public void testCreateString() {
		Database db=new Database();
		try {
			db.create("jtest.db");
			db.close();
			db.open("jtest.db");
			db.close();
		}
		catch (Error err) {
			fail("Exception "+err);
		}
	}
	
	public void testCreateInvalidParameters() {
		Parameters[] params=new Parameters[3];
		params[1]=new Parameters();
		params[2]=new Parameters();
		Database db=new Database();
		try {
			db.create("jtest.db", 0, 0, params);
		}
		catch (NullPointerException e) {
		}
		catch (Error err) {
			fail("Exception "+err);
		}

	}

	public void testCreateStringIntIntParametersArray() {
		Database db=new Database();
		try {
			db.create("jtest.db", 0, 0644, null);
			db.close();
			db.open("jtest.db", 0);
			db.close();
		}
		catch (Error err) {
			fail("Exception "+err);
		}
	}

	public void testGetError() {
		Database db=new Database();
		try {
			db.create("jtest.db", 1234, 0); /* Triggers INV_PARAMETER */
		}
		catch (Error err) {
			assertEquals(Const.HAM_INV_PARAMETER, err.getErrno());
		}
		assertEquals(Const.HAM_INV_PARAMETER, db.getError());
	}
	
	private class MyComparator implements Comparable
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
		}
		catch (Error err) {
			fail("Exception "+err);
		}
		assertEquals(2, cmp.m_counter);
	}

	private class MyPrefixComparator implements PrefixComparable
	{
		public int m_counter;
		
		public int compare(byte[] lhs, int lhs_real_size, 
				byte[] rhs, int rhs_real_size) {
			m_counter++;
			return m_counter; /* need to return different values, or 
								ham_insert thinks we're inserting 
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
		}
		catch (Error err) {
			fail("Exception "+err);
		}
		assertEquals(4, cmp.m_counter);
	}

	public void testEnableCompressionWithoutHandle() {
		Database db=new Database();
		try {
			db.enableCompression();
		}
		catch (Error err) {
			assertEquals(Const.HAM_INV_PARAMETER, err.getErrno());
		}
	}

	public void testEnableCompression() {
		Database db=new Database();
		try {
			db.create("jtest.db");
			db.enableCompression();
			db.close();
			db.open("jtest.db");
			db.close();
		}
		catch (Error err) {
			fail("Exception "+err);
		}
	}

	public void testEnableCompressionInt() {
		Database db=new Database();
		try {
			db.enableCompression(999);
			db.create("jtest.db");
			db.close();
			db.open("jtest.db");
			db.close();
		}
		catch (Error err) {
			assertEquals(Const.HAM_INV_PARAMETER, err.getErrno());
		}
	}
	
	public void assertByteArrayEquals(byte[] r1, byte[] r2) {
		assertEquals(r1.length, r2.length);
		
		for (int i=0; i<r1.length; i++) {
			assertEquals(r1[i], r2[i]);
		}
	}

	public void testFindByteArrayInt() {
		byte[] k=new byte[5];
		byte[] r=new byte[5];
		r[1]=0x14; r[2]=0x15;
		Database db=new Database();
		try {
			db.create("jtest.db");
			db.insert(k, r);
			byte[]r2=db.find(k);
			assertByteArrayEquals(r, r2);
		}
		catch (Error err) {
			fail("Exception "+err);
		}
	}
	
	public void testFindNegativeKey() {
		byte[] k=new byte[5];
		byte[] r=new byte[5];
		r[1]=0x14; r[2]=0x15;
		Database db=new Database();
		try {
			db.create("jtest.db");
			db.insert(k, r);
			byte[]r2=db.find(null);
			assertByteArrayEquals(r, r2);
		}
		catch (Error err) {
			fail("Exception "+err);
		}
		catch (NullPointerException ex) {
		}
	}
	
	public void testFindUnknownKey() {
		byte[] k=new byte[5];
		Database db=new Database();
		try {
			db.create("jtest.db");
			db.find(k);
		}
		catch (Error err) {
			assertEquals(Const.HAM_KEY_NOT_FOUND, err.getErrno());
		}	
	}

	public void testInsertByteArrayByteArrayInt() {
		byte[] k=new byte[5];
		byte[] r=new byte[5];
		r[1]=0x14; r[2]=0x15;
		Database db=new Database();
		try {
			db.create("jtest.db", Const.HAM_ENABLE_DUPLICATES);
			db.insert(k, r);
			db.insert(k, r, Const.HAM_OVERWRITE);
			db.insert(k, r, Const.HAM_DUPLICATE);
			byte[]r2=db.find(k);
			assertByteArrayEquals(r, r2);
		}
		catch (Error err) {
			fail("Exception "+err);
		}
	}
	
	public void testInsertNegativeKey() {
		byte[] r=new byte[5];
		Database db=new Database();
		try {
			db.create("jtest.db");
			db.insert(null, r);
		}
		catch (NullPointerException ex) {
		}
		catch (Error err) {
			fail("Exception "+err);
		}	
	}

	public void testInsertNegativeRecord() {
		byte[] k=new byte[5];
		Database db=new Database();
		try {
			db.create("jtest.db");
			db.insert(k, null);
		}
		catch (NullPointerException ex) {
		}
		catch (Error err) {
			fail("Exception "+err);
		}	
	}

	public void testInsertNegativeOverwrite() {
		byte[] r=new byte[5];
		byte[] k=new byte[5];
		Database db=new Database();
		try {
			db.create("jtest.db");
			db.insert(k, r);
			db.insert(k, r);
		}
		catch (Error err) {
			assertEquals(Const.HAM_DUPLICATE_KEY, err.getErrno());
		}	
	}

	public void testEraseByteArrayInt() {
		byte[] r=new byte[5];
		byte[] k=new byte[5];
		Database db=new Database();
		try {
			db.create("jtest.db");
			db.insert(k, r);
			db.erase(k);
		}
		catch (Error err) {
			fail("Exception "+err);
		}
	}
	
	public void testEraseTwice() {
		byte[] r=new byte[5];
		byte[] k=new byte[5];
		Database db=new Database();
		try {
			db.create("jtest.db");
			db.insert(k, r);
			db.erase(k);
			db.erase(k);
		}
		catch (Error err) {
			assertEquals(Const.HAM_KEY_NOT_FOUND, err.getErrno());
		}
	}

	public void testEraseNegativeKey() {
		Database db=new Database();
		try {
			db.create("jtest.db");
			db.erase(null);
		}
		catch (Error err) {
			fail("Exception "+err);
		}
		catch (NullPointerException ex) {
		}
	}

	public void testFlush() {
		Database db=new Database();
		try {
			db.create("jtest.db");
			db.flush();
		}
		catch (Error err) {
			fail("Exception "+err);
		}
	}

	public void testCloseInt() {
		Database db=new Database();
		try {
			db.create("jtest.db");
			db.close();
		}
		catch (Error err) {
			fail("Exception "+err);
		}
	}
}
