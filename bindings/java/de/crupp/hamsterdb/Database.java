package de.crupp.hamsterdb;

class Parameters {
	public int name;
	public long value;
}

interface Comparable {
	
	/**
	 * The compare method compares two keys - the "left-hand side"
	 * (lhs) and the "right-hand side (rhs).
	 * 
	 * @param lhs The first key
	 * @param rhs The second key
	 * @return -1 if the first key is smaller, +1 if the first key
	 *     is larger, 0 if both keys are equal
	 */
	public int compare(byte[] lhs, byte[] rhs);
}

interface PrefixComparable {
	public int compare(byte[] lhs, int lhs_realsize, 
			byte[] rhs, int rhs_realsize);
}

interface ErrorHandler {
	
	/**
	 * The handleMessage method is called whenever a message
	 * is emitted.
	 * 
	 * @param level Debug level (0 = Debug, 1 = Normal, 3 = Fatal)
	 * @param message The message
	 */
	public void handleMessage(int level, String message);
}

public class Database {

	/**
	 * Set the error handler
	 * 
	 * Wraps the ham_set_error_handler function.
	 */
	public static void setErrorHandler(ErrorHandler handler) {
		// TODO
	}

	/**
	 * Get the hamsterdb library version
	 * 
	 * Wraps the ham_get_version function.
	 */
	public static void getVersion(Integer major, Integer minor,
			Integer revision) {
		// TODO
	}

	/**
	 * Get name of licensee and the licensed product name
	 * 
	 * Wraps the ham_get_license function.
	 */
	public static void getLicense(String licensee, String product) {
		// TODO
	}
	
	/**
	 * Constructor
	 */
	public Database() {
	}
	
	/**
	 * Destructor - automatically closes the database
	 */
	public void finalize() {
		close();
	}

	/**
	 * Creates a Database
	 * 
	 * Wraps the ham_create_ex function.
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

	public synchronized void create(String filename, int flags, int mode, Parameters[] param) {
		// TODO
	}
	
	/**
	 * Opens an existing Database
	 * 
	 * Wraps the ham_open_ex function.
	 */
	public void open(String filename) {
		open(filename, 0, null);
	}
	
	public void open(String filename, int flags) {
		open(filename, flags, null);
	}
	
	public synchronized void open(String filename, int flags, Parameters[] params) {
		// TODO
	}
	
	/**
	 * Returns the last error
	 * 
	 * Wraps the ham_get_error function.
	 */
	public synchronized int getError() {
		// TODO
		return 0;
	}
	
	/**
	 * Sets the comparison function
	 * 
	 * Wraps the ham_set_compare function.
	 */
	public synchronized void setComparator(Comparable cmp) {
		// TODO
	}
	 
	/**
	 * Sets the prefix comparison function
	 * 
	 * Wraps the ham_set_prefix_compare function.
	 */
	public synchronized void setPrefixComparator(PrefixComparable cmp) {
		// TODO
	}
	
	/**
	 * Enable zlib compression
	 * 
	 * Wraps the ham_enable_compression function.
	 */
	public synchronized void enableCompression() {
		enableCompression(0);
	}
	
	public synchronized void enableCompression(int level) {
		// TODO
	}

	/**
	 * Finds a Record by looking up the Key
	 * 
	 * Wraps the ham_find function.
	 */
	public Record find(Key key) {
		return find(key, 0);
	}
	
	public synchronized Record find(Key key, int flags) {
		// TODO
		return null;
	}
	
	/**
	 * Inserts a Key/Record pair.
	 * 
	 * Wraps the ham_insert function.
	 */
	public void insert(Key key, Record record) {
		insert(key, record, 0);
	}
	
	public synchronized void insert(Key key, Record record, int flags) {
		// TODO
	}
	
	/**
	 * Erases a Key
	 * 
	 * Wraps the ham_erase function.
	 */
	public void erase(Key key) {
		erase(key, 0);
	}
	 
	public synchronized void erase(Key key, int flags) {
		// TODO
	}
	
	/**
	 * Flush the Database
	 * 
	 * Wraps the ham_flush function.
	 */
	public synchronized void flush() {
		// TODO
	}
	
	/**
	 * Closes the Database
	 * 
	 * Wraps the ham_close function.
	 */
	public void close() {
		close(0);
	}
	
	public synchronized void close(int flags) {
		// TODO
	}

}
