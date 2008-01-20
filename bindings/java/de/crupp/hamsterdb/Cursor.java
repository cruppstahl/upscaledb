package de.crupp.hamsterdb;

public class Cursor {
	
	// TODO fix the constants!
	public final int HAM_CURSOR_FIRST = 1;
	public final int HAM_CURSOR_LAST = 1;
	public final int HAM_CURSOR_NEXT = 1;
	public final int HAM_CURSOR_PREVIOUS = 1;
	
	/**
	 * Constructor
	 */
	public Cursor(Database db) {
		create(db);
	}
	
	/**
	 * Destructor - automatically closes the Cursor
	 */
	public void finalize() {
		close();
	}
	
	/**
	 * Creates a new Cursor
	 */
	public synchronized void create(Database db) {
		// TODO
	}

	/**
	 * Clones the Cursor
	 */
	public synchronized Cursor cloneCursor() {
		return null; // TODO
	}
	
	/**
	 * Moves the Cursor, and retrieves the Key/Record pair
	 */
	public synchronized void move(Key key, Record record, int flags) {
		// TODO
	}
	
	/**
	 * Moves the Cursor to the first Database element
	 */
	public void moveFirst(Key key, Record record) {
		move(key, record, HAM_CURSOR_FIRST);
	}
	
	/**
	 * Moves the Cursor to the last Database element
	 */
	public void moveLast(Key key, Record record) {
		move(key, record, HAM_CURSOR_LAST);
	}
	
	/**
	 * Moves the Cursor to the next Database element
	 */
	public void moveNext(Key key, Record record) {
		move(key, record, HAM_CURSOR_NEXT);
	}
	
	/**
	 * Moves the Cursor to the previous Database element
	 */
	public void movePrevious(Key key, Record record) {
		move(key, record, HAM_CURSOR_PREVIOUS);
	}
	
	/**
	 * Overwrites the current Record
	 */
	public synchronized void overwrite(Record record) {
		// TODO
	}
	
	/**
	 * Finds a Key
	 */
	public synchronized void find(Key key) {
		// TODO
	}
	
	/**
	 * Inserts a Key/Record pair.
	 */
	public void insert(Key key, Record record) {
		insert(key, record, 0);
	}
	
	public synchronized void insert(Key key, Record record, int flags) {
		// TODO
	}
	
	/**
	 * Erases the current entry.
	 */
	public synchronized void erase() {
		// TODO
	}
	
	/**
	 * Returns the number of duplicate elements of the current Key.
	 */
	public synchronized int getDuplicateCount() {
		// TODO
		return 0;
	}
	
	/**
	 * Closes the Cursor.
	 */
	public synchronized void close() {
		// TODO
	}
	
}
