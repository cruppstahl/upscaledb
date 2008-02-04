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

public class Environment {
	
	private native long ham_env_new();
	
	private native void ham_env_delete(long handle);
			
	private native int ham_env_create_ex(long handle, String filename, int flags,
			int mode, Parameter[] params);
	
	private native int ham_env_open_ex(long handle, String filename, int flags, 
			Parameter[] params);
	
	private native long ham_env_create_db(long handle, short name, int flags,
			Parameter[] params);
	
	private native long ham_env_open_db(long handle, short name, int flags,
			Parameter[] params);
	
	private native int ham_env_rename_db(long handle, short oldname,
			short newname, int flags);

	private native int ham_env_erase_db(long handle, short name, int flags);
	
	private native int ham_env_enable_encryption(long handle, byte[] key, 
			int flags);
	
	private native short[] ham_env_get_database_names(long handle);
	
	private native int ham_env_close(long handle, int flags);

	/**
	 * Constructor
	 */
	public Environment() {
	}
	
	/**
	 * Destructor - closes the Environment
	 */
	public void finalize()
			throws Error {
		close();
	}
	
	/**
	 * Creates a new Environment
	 * 
	 * Wraps the ham_env_create_ex function.
	 */
	public void create(String filename)
			throws Error {
		create(filename, 0, 0644, null);
	}
	
	public void create(String filename, int flags) 
			throws Error {
		create(filename, flags, 0644, null);
	}
	
	public void create(String filename, int flags, int mode) 
			throws Error {
		create(filename, flags, mode, null);
	}

	public synchronized void create(String filename, int flags, int mode, 
			Parameter[] params)
			throws Error {
		// make sure that the parameters don't have a NULL-element
		if (params!=null) {
			for (int i=0; i<params.length; i++) 
				if (params[i]==null)
					throw new NullPointerException();
		}
		int status;
		if (m_handle==0) {
			m_handle=ham_env_new();
			if (m_handle==0)
				throw new Error(Const.HAM_OUT_OF_MEMORY);
		}
		status=ham_env_create_ex(m_handle, filename, flags, mode, params);
		if (status!=0)
			throw new Error(status);
	}
	
	/**
	 * Opens an existing Environment
	 * 
	 * Wraps the ham_env_open_ex function.
	 */
	public void open(String filename) 
			throws Error {
		open(filename, 0, null);
	}
	
	public void open(String filename, int flags) 
			throws Error {
		open(filename, flags, null);
	}
	
	public synchronized void open(String filename, int flags, 
			Parameter[] params) 
			throws Error {
		// make sure that the parameters don't have a NULL-element
		if (params!=null) {
			for (int i=0; i<params.length; i++) 
				if (params[i]==null)
					throw new NullPointerException();
		}
		int status;
		if (m_handle==0) {
			m_handle=ham_env_new();
			if (m_handle==0)
				throw new Error(Const.HAM_OUT_OF_MEMORY);
		}
		status=ham_env_open_ex(m_handle, filename, flags, params);
		if (status!=0)
			throw new Error(status);
	}
	
	/**
	 * Create a new Database in this Environment
	 * 
	 * Wraps the ham_env_create_db function.
	 */
	public Database createDatabase(short name) 
			throws Error {
		return createDatabase(name, 0, null);
	}
	
	public Database createDatabase(short name, int flags) 
			throws Error {
		return createDatabase(name, flags, null);
	}
	
	public synchronized Database createDatabase(short name, int flags, 
			Parameter[] params) 
			throws Error {
		// make sure that the parameters don't have a NULL-element
		if (params!=null) {
			for (int i=0; i<params.length; i++) 
				if (params[i]==null)
					throw new NullPointerException();
		}
		// ham_env_create_db will throw an Error if it fails
		return new Database(ham_env_create_db(m_handle, name, flags, params));
	}
	
	/**
	 * Opens an existing Database in this Environment
	 * 
	 * Wraps the ham_env_open_db function.
	 */
	public Database openDatabase(short name) 
			throws Error {
		return openDatabase(name, 0, null);
	}
	
	public Database openDatabase(short name, int flags) 
			throws Error {
		return openDatabase(name, flags, null);
	}

	public synchronized Database openDatabase(short name, int flags,
			Parameter[] params) 
			throws Error {
		// make sure that the parameters don't have a NULL-element
		if (params!=null) {
			for (int i=0; i<params.length; i++) 
				if (params[i]==null)
					throw new NullPointerException();
		}
		// ham_env_open_db will throw an Error if it fails
		return new Database(ham_env_open_db(m_handle, name, flags, params));
	}
	
	/**
	 * Renames an existing Database
	 * 
	 * Wraps the ham_env_rename_db function.
	 */
	public synchronized void renameDatabase(short oldname, short newname)
			throws Error {
		int status=ham_env_rename_db(m_handle, oldname, newname, 0);
		if (status!=0)
			throw new Error(status);
	}
	 
	/**
	 * Deletes a Database from this Environment
	 * 
	 * Wraps the ham_env_erase_db function.
	 */
	public synchronized void eraseDatabase(short name)
		throws Error {
			int status=ham_env_erase_db(m_handle, name, 0);
			if (status!=0)
				throw new Error(status);
	}
	
	/**
	 * Enables AES encryption
	 * 
	 * Wraps the ham_env_enable_encryption function.
	 */
	public synchronized void enableEncryption(byte[] aeskey)
			throws Error {
		if (aeskey==null || aeskey.length!=16)
			throw new Error(Const.HAM_INV_PARAMETER);
		int status=ham_env_enable_encryption(m_handle, aeskey, 0);
		if (status!=0)
			throw new Error(status);
	}
	
	/**
	 * Get all Database names
	 */
	public synchronized short[] getDatabaseNames()
			throws Error {
		/* the native library throws an exception, if necessary */
		return ham_env_get_database_names(m_handle);
	}
	
	/**
	 * Closes the Environment
	 */
	public void close() 
			throws Error {
		close(0);
	}
	
	public synchronized void close(int flags) 
			throws Error {
		if (m_handle==0)
			return;
		int status=ham_env_close(m_handle, flags);
		if (status!=0)
			throw new Error(status);
		ham_env_delete(m_handle);
		m_handle=0;
	}
	
	static {
		System.loadLibrary("hamsterdb");
	}
	
	private long m_handle;

}
