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
			System.out.println(level+": "+message);		
		}
		
		public void finalize() {
			System.out.println("gone!");
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
			fail("Exception "+err);
		}
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

	public void testSetComparator() {
		fail("Not yet implemented");
	}

	public void testSetPrefixComparator() {
		fail("Not yet implemented");
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

	public void testFindByteArrayInt() {
		fail("Not yet implemented");
	}

	public void testInsertByteArrayByteArrayInt() {
		fail("Not yet implemented");
	}

	public void testEraseByteArrayInt() {
		fail("Not yet implemented");
	}

	public void testFlush() {
		fail("Not yet implemented");
	}

	public void testCloseInt() {
		fail("Not yet implemented");
	}

	public void testGetHandle() {
		fail("Not yet implemented");
	}

	public void testMain() {
		fail("Not yet implemented");
	}

}
