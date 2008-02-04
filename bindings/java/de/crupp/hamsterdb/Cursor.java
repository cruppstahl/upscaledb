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

public class Cursor {
	
	private native long ham_cursor_create(long dbhandle);

	private native long ham_cursor_clone(long handle);
	
	private native int ham_cursor_move_to(long handle, int flags);
	
	private native byte[] ham_cursor_get_key(long handle, int flags);
	
	private native byte[] ham_cursor_get_record(long handle, int flags);
	
	private native int ham_cursor_overwrite(long handle, 
			byte[] record, int flags);
	
	private native int ham_cursor_find(long handle, 
			byte[] key, int flags);

	private native int ham_cursor_insert(long handle, 
			byte[] key, byte[] record, int flags);

	private native int ham_cursor_erase(long handle, 
			int flags);
	
	private native int ham_cursor_get_duplicate_count(long handle,
			int flags);
	
	private native int ham_cursor_close(long handle);

	/**
	 * Constructor
	 */
	public Cursor(Database db, long handle) {
		m_db=db;
		m_handle=handle;
	}
	
	public Cursor(Database db)
			throws Error {
		m_db=db;
		create(db);
	}
	
	/**
	 * Destructor - automatically closes the Cursor
	 */
	public void finalize()
			throws Error {
		close();
	}
	
	/**
	 * Creates a new Cursor
	 * 
	 * Wraps the ham_cursor_create function.
	 */
	public void create(Database db) 
			throws Error {
		synchronized (m_db) {
			m_handle=ham_cursor_create(db.getHandle());
		}
		if (m_handle==0)
			throw new Error(m_db.getError());
	}

	/**
	 * Clones the Cursor
	 * 
	 * Wraps the ham_cursor_clone function.
	 */
	public Cursor cloneCursor() 
			throws Error {
		long newhandle;
		if (m_handle==0)
			throw new Error(Const.HAM_INV_PARAMETER);
		synchronized (m_db) {
			newhandle=ham_cursor_clone(m_handle);
		}
		if (newhandle==0)
			throw new Error(m_db.getError());
		return new Cursor(m_db, newhandle);
	}
	
	/**
	 * Moves the Cursor to the direction specified in the flags.
	 * 
	 * Wraps the ham_cursor_move function.
	 */
	public void move(int flags) 
			throws Error {
		int status;
		synchronized (m_db) {
			status=ham_cursor_move_to(m_handle, flags);
		}
		if (status!=0)
			throw new Error(status);
	}

	/**
	 * Moves the Cursor to the first Database element
	 */
	public void moveFirst()
			throws Error {
		move(Const.HAM_CURSOR_FIRST);
	}
	
	/**
	 * Moves the Cursor to the last Database element
	 */
	public void moveLast() 
			throws Error {
		move(Const.HAM_CURSOR_LAST);
	}
	
	/**
	 * Moves the Cursor to the next Database element
	 */
	public void moveNext() 
			throws Error {
		move(Const.HAM_CURSOR_NEXT);
	}
	
	/**
	 * Moves the Cursor to the previous Database element
	 */
	public void movePrevious() 
			throws Error {
		move(Const.HAM_CURSOR_PREVIOUS);
	}
	
	/**
	 * Retrieve the Key of the current item.
	 */
	public byte[] getKey() 
			throws Error {
		byte[] ret;
		synchronized (m_db) {
			ret=ham_cursor_get_key(m_handle, 0);
		}
		if (ret==null)
			throw new Error(m_db.getError());
		return ret;
	}

	/**
	 * Retrieve the Record of the current item.
	 */
	public byte[] getRecord() 
			throws Error {
		byte[] ret;
		synchronized (m_db) {
			ret=ham_cursor_get_record(m_handle, 0);
		}
		if (ret==null)
			throw new Error(m_db.getError());
		return ret;
	}

	/**
	 * Overwrites the current Record
	 * 
	 * Wraps the ham_cursor_overwrite function.
	 */
	public void overwrite(byte[] record) 
			throws Error {
		if (record==null)
			throw new NullPointerException();
		int status;
		synchronized (m_db) {
			status=ham_cursor_overwrite(m_handle, record, 0);
		}
		if (status!=0)
			throw new Error(status);
	}
	
	/**
	 * Finds a Key
	 * 
	 * Wraps the ham_cursor_find function.
	 */
	public void find(byte[] key) 
			throws Error {
		if (key==null)
			throw new NullPointerException();
		int status;
		synchronized (m_db) {
			status=ham_cursor_find(m_handle, key, 0);
		}
		if (status!=0)
			throw new Error(status);	
	}
	
	/**
	 * Inserts a Key/Record pair.
	 * 
	 * Wraps the ham_cursor_insert function.
	 */
	public void insert(byte[] key, byte[] record) 
			throws Error {
		insert(key, record, 0);
	}
	
	public void insert(byte[] key, byte[] record, int flags) 
			throws Error {
		if (key==null || record==null)
			throw new NullPointerException();
		int status;
		synchronized (m_db) {
			status=ham_cursor_insert(m_handle, key, record, flags);
		}
		if (status!=0)
			throw new Error(status);
	}
	
	/**
	 * Erases the current entry.
	 * 
	 * Wraps the ham_cursor_erase function.
	 */
	public void erase() 
			throws Error {
		int status;
		synchronized (m_db) {
			status=ham_cursor_erase(m_handle, 0);
		}
		if (status!=0)
			throw new Error(status);
	}
	
	/**
	 * Returns the number of duplicate elements of the current Key.
	 * 
	 * Wraps the ham_cursor_get_duplicate_count function.
	 */
	public int getDuplicateCount() 
			throws Error {
		int count;
		synchronized (m_db) {
			count=ham_cursor_get_duplicate_count(m_handle, 0);
		}
		if (count==0)
			throw new Error(m_db.getError());
		return count;
	}
	
	/**
	 * Closes the Cursor.
	 * 
	 * Wraps the ham_cursor_close function.
	 */
	public void close() 
			throws Error {
		int status;
		if (m_handle==0)
			return;
		synchronized (m_db) {
			status=ham_cursor_close(m_handle);
		}
		if (status!=0)
			throw new Error(status);
		m_handle=0;
	}
	
	/**
	 * Retrieves the Cursor handle
	 */
	public long getHandle() {
		return m_handle;
	}
	
	private long m_handle;
	private Database m_db;
}
