package de.crupp.hamsterdb;

public class Environment {
	/**
	 * Constructor
	 */
	public Environment() {
	}
	
	/**
	 * Destructor - closes the Environment
	 */
	public void finalize() {
		close();
	}
	
	/**
	 * Creates a new Environment
	 */
	public void create(String filename) {
		create(filename, 0, 0644, null);
	}
	
	public void create(String filename, int flags) {
		create(filename, flags, 0644, null);
	}
	
	public void create(String filename, int flags, int mode) {
		create(filename, flags, mode, null);
	}

	public synchronized void create(String filename, int flags, int mode, 
			Parameters[] param) {
		// TODO
	}
	
	/**
	 * Opens an existing Environment
	 * 
	 * Wraps the ham_env_open_ex function.
	 */
	public void open(String filename) {
		open(filename, 0, null);
	}
	
	public void open(String filename, int flags) {
		open(filename, flags, null);
	}
	
	public synchronized void open(String filename, int flags, 
			Parameters[] params) {
		// TODO
	}
	
	/**
	 * Create a new Database in this Environment
	 */
	public Database createDatabase(short name) {
		return createDatabase(name, 0, null);
	}
	
	public Database createDatabase(short name, int flags) {
		return createDatabase(name, flags, null);
	}
	
	public synchronized Database createDatabase(short name, int flags, 
			Parameters[] params) {
		// TODO
		return null;
	}
	
	/**
	 * Opens an existing Database in this Environment
	 */
	public Database openDatabase(short name) {
		return openDatabase(name, 0, null);
	}
	
	public Database openDatabase(short name, int flags) {
		return openDatabase(name, flags, null);
	}

	public synchronized Database openDatabase(short name, int flags,
			Parameters[] params) {
		// TODO
		return null;
	}
	
	/**
	 * Renames an existing Database
	 */
	public synchronized void renameDatabase(short oldname, short newname) {
		// TODO
	}
	 
	/**
	 * Deletes a Database from this Environment
	 */
	public synchronized void eraseDatabase(short name) {
		// TODO
	}
	
	/**
	 * Enables AES encryption
	 */
	public synchronized void enableEncryption(byte[] aeskey) {
		// TODO
	}
	
	/**
	 * Get all Database names
	 */
	public synchronized short[] getDatabaseNames() {
		// TODO
		return null;
	}
	
	/**
	 * Closes the Environment
	 */
	public void close() {
		close(0);
	}
	
	public synchronized void close(int flags) {
		// TODO
	}

}
