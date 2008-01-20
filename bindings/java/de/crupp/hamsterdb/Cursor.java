package de.crupp.hamsterdb;

public class Cursor {
	
	// TODO fix the constants!
	public final static int HAM_CURSOR_FIRST = 1;
	public final static int HAM_CURSOR_LAST = 1;
	public final static int HAM_CURSOR_NEXT = 1;
	public final static int HAM_CURSOR_PREVIOUS = 1;
	
	private native long ham_cursor_create(long dbhandle);

	private native long ham_cursor_clone(long handle);
	
	private native int ham_cursor_move(long handle, 
			Key key, Record record, int flags);
	
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
	public Cursor(long handle) {
		m_handle=handle;
	}
	
	public Cursor(Database db)
			throws Error {
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
	public synchronized void create(Database db) 
			throws Error {
		// TODO losing the error code!! throw exception 
		// from native code??
		m_handle=ham_cursor_create(db.getHandle());
		if (m_handle==0)
			throw new Error(Error.HAM_OUT_OF_MEMORY);	
	}

	/**
	 * Clones the Cursor
	 * 
	 * Wraps the ham_cursor_clone function.
	 */
	public synchronized Cursor cloneCursor() 
			throws Error {
		if (m_handle==0)
			throw new Error(Error.HAM_INV_PARAMETER);
		long newhandle=ham_cursor_clone(m_handle);
		if (newhandle==0)
			throw new Error(Error.HAM_OUT_OF_MEMORY);
		return new Cursor(newhandle);
	}
	
	/**
	 * Moves the Cursor, and retrieves the Key/Record pair
	 * 
	 * Wraps the ham_cursor_move function.
	 */
	public synchronized void move(Key key, Record record, int flags) 
			throws Error {
		int status=ham_cursor_move(m_handle, key, record, flags);
		if (status!=0)
			throw new Error(status);
	}
	
	/**
	 * Moves the Cursor to the first Database element
	 */
	public void moveFirst(Key key, Record record)
			throws Error {
		move(key, record, HAM_CURSOR_FIRST);
	}
	
	/**
	 * Moves the Cursor to the last Database element
	 */
	public void moveLast(Key key, Record record) 
			throws Error {
		move(key, record, HAM_CURSOR_LAST);
	}
	
	/**
	 * Moves the Cursor to the next Database element
	 */
	public void moveNext(Key key, Record record) 
			throws Error {
		move(key, record, HAM_CURSOR_NEXT);
	}
	
	/**
	 * Moves the Cursor to the previous Database element
	 */
	public void movePrevious(Key key, Record record) 
			throws Error {
		move(key, record, HAM_CURSOR_PREVIOUS);
	}
	
	/**
	 * Overwrites the current Record
	 * 
	 * Wraps the ham_cursor_overwrite function.
	 */
	public synchronized void overwrite(Record record) 
			throws Error {
		if (record==null)
			throw new NullPointerException();
		int status=ham_cursor_overwrite(m_handle, record.data, 0);
		if (status!=0)
			throw new Error(status);
	}
	
	/**
	 * Finds a Key
	 * 
	 * Wraps the ham_cursor_find function.
	 */
	public synchronized void find(Key key) 
			throws Error {
		if (key==null)
			throw new NullPointerException();
		int status=ham_cursor_find(m_handle, key.data, 0);
		if (status!=0)
			throw new Error(status);	
	}
	
	/**
	 * Inserts a Key/Record pair.
	 * 
	 * Wraps the ham_cursor_insert function.
	 */
	public void insert(Key key, Record record) 
			throws Error {
		insert(key, record, 0);
	}
	
	public synchronized void insert(Key key, Record record, int flags) 
			throws Error {
		if (key==null || record==null)
			throw new NullPointerException();
		int status=ham_cursor_insert(m_handle, key.data, record.data, flags);
		if (status!=0)
			throw new Error(status);
	}
	
	/**
	 * Erases the current entry.
	 * 
	 * Wraps the ham_cursor_erase function.
	 */
	public synchronized void erase() 
			throws Error {
		int status=ham_cursor_erase(m_handle, 0);
		if (status!=0)
			throw new Error(status);
	}
	
	/**
	 * Returns the number of duplicate elements of the current Key.
	 * 
	 * Wraps the ham_cursor_get_duplicate_count function.
	 */
	public synchronized int getDuplicateCount() 
			throws Error {
		// TODO status code is missing! throw exception 
		// from native code??
		return ham_cursor_get_duplicate_count(m_handle, 0);
	}
	
	/**
	 * Closes the Cursor.
	 * 
	 * Wraps the ham_cursor_close function.
	 */
	public synchronized void close() 
			throws Error {
		int status=ham_cursor_close(m_handle);
		if (status!=0)
			throw new Error(status);
		m_handle=0;
	}
	
	private long m_handle;
}
