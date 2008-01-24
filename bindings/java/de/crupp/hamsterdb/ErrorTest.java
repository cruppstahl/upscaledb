package de.crupp.hamsterdb;

import junit.framework.TestCase;

public class ErrorTest extends TestCase {

	public void testGetErrno() {
		Error e=new Error(13);
		assertEquals(13, e.getErrno());
	}

	public void testGetMessage() {
		Error e=new Error(-8);
		assertEquals(-8, e.getErrno());
		assertEquals("Invalid parameter", e.getMessage());
	}

	public void testToString() {
		Error e=new Error(0);
		assertEquals(0, e.getErrno());
		assertEquals("Success", e.toString());
	}

}
