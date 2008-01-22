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

public class Error extends java.lang.Throwable {

    private native String ham_strerror(int errno);

	/**
	 * Constructor
	 */
	public Error(int errno) {
		m_errno=errno;
	}

	/**
	 * Getter for the error number
	 */
	public int getErrno() {
		return m_errno;
	}

	/**
	 * Returns an english error description
	 * 
	 * Wraps the ham_strerror function.
	 */
	public String getMessage() {
		return ham_strerror(m_errno);
	}
	
	public String toString() {
		return getMessage();
	}
	
	/**
	 * The error number
	 */
	private int m_errno;
	
	private final static long serialVersionUID = 1L;
	
	static {
		System.loadLibrary("hamsterdb");
	}
}
