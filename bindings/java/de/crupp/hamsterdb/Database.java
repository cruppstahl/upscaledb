package de.crupp.hamsterdb;

class Parameters {
	public Parameters() {
	}
	
	public Parameters(int name, long value) {
		this.name=name;
		this.value=value;
	}
	
	public int name;
	public long value;
}

class Version {
	public int major;
	public int minor;
	public int revision;
}

class License {
	public String licensee;
	public String product;
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
	
	/**
	 * The compare method compares the prefixes of two keys - the 
	 * "left-hand side" (lhs) and the "right-hand side (rhs).
	 * 
	 * @param lhs The prefix of the first key
	 * @param lhs_realsize The real size of the first key
	 * @param rhs The prefix of the second key
	 * @param rhs_realsize The real size of the second key
	 * @return -1 if the first key is smaller, +1 if the first key
	 *     is larger, 0 if both keys are equal, or 
	 *     Error.HAM_PREFIX_REQUEST_FULLKEY if the Prefixes are
	 *     not sufficient for the comparison 
	 */
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

	private native static int ham_get_version(int which);

	private native static String ham_get_license(int which);
	
	private native static void ham_set_errhandler(ErrorHandler eh);
	
	private native long ham_new();
	
	private native void ham_delete(long handle);
			
	private native int ham_create_ex(long handle, String filename, int flags,
			int mode, Parameters[] params);
	
	private native int ham_open_ex(long handle, String filename, int flags, 
			Parameters[] params);

	private native int ham_get_error(long handle);
	
	private native void ham_set_compare_func(long handle, 
			Comparable cmp);
	
	private native void ham_set_prefix_compare_func(long handle, 
			PrefixComparable cmp);
	
	private native int ham_enable_compression(long handle, int level, 
			int flags);
	
	private native byte[] ham_find(long handle, byte[] key, int flags);
	
	private native int ham_flush(long handle, int flags);
	
	private native int ham_insert(long handle, byte[] key, byte[] record, 
			int flags);

	private native int ham_erase(long handle, byte[] key, int flags);

	private native int ham_close(long handle, int flags); 
	
	/**
	 * Set the error handler
	 * 
	 * Wraps the ham_set_error_handler function.
	 */
	public static void setErrorHandler(ErrorHandler eh) {
		ham_set_errhandler(eh);
	}

	/**
	 * Get the hamsterdb library version
	 * 
	 * Wraps the ham_get_version function.
	 */
	public static Version getVersion() {
		Version v=new Version();
		v.major=ham_get_version(0);
		v.minor=ham_get_version(1);
		v.revision=ham_get_version(2);
		return v;
	}
	
	/**
	 * Get name of licensee and the licensed product name
	 * 
	 * Wraps the ham_get_license function.
	 */
	public static License getLicense() {
		License l=new License();
		l.licensee=ham_get_license(0);
		l.product=ham_get_license(1);
		return l;
	}
	
	/**
	 * Constructor
	 */
	public Database() {
	}
	
	public Database(long handle) {
		m_handle=handle;
	}
	
	/**
	 * Destructor
	 */
	protected void finalize() 
			throws Error {
		System.out.println("finalize");
		close();
	}

	/**
	 * Creates a Database
	 * 
	 * Wraps the ham_create_ex function.
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
			Parameters[] params) throws Error {
		// make sure that the parameters don't have a NULL-element
		if (params!=null) {
			for (int i=0; i<params.length; i++) 
				if (params[i]==null)
					throw new NullPointerException();
		}
		int status;
		if (m_handle==0) {
			m_handle=ham_new();
			if (m_handle==0)
				throw new Error(Error.HAM_OUT_OF_MEMORY);
		}
		status=ham_create_ex(m_handle, filename, flags, mode, params);
		if (status!=0)
			throw new Error(status);
	}
	
	/**
	 * Opens an existing Database
	 * 
	 * Wraps the ham_open_ex function.
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
			Parameters[] params) throws Error {
		// make sure that the parameters don't have a NULL-element
		if (params!=null) {
			for (int i=0; i<params.length; i++) 
				if (params[i]==null)
					throw new NullPointerException();
		}
		int status;
		if (m_handle==0) {
			m_handle=ham_new();
			if (m_handle==0)
				throw new Error(Error.HAM_OUT_OF_MEMORY);
		}
		status=ham_open_ex(m_handle, filename, flags, params);
		if (status!=0)
			throw new Error(status);
	}
	
	/**
	 * Returns the last error
	 * 
	 * Wraps the ham_get_error function.
	 */
	public synchronized int getError() {
		return ham_get_error(m_handle);
	}
	
	/**
	 * Sets the comparison function
	 * 
	 * Wraps the ham_set_compare function.
	 */
	public synchronized void setComparator(Comparable cmp) {
		m_cmp=cmp;
		ham_set_compare_func(m_handle, cmp);
	}
	 
	/**
	 * Sets the prefix comparison function
	 * 
	 * Wraps the ham_set_prefix_compare function.
	 */
	public synchronized void setPrefixComparator(PrefixComparable cmp) {
		m_prefix_cmp=cmp;
		ham_set_prefix_compare_func(m_handle, cmp);
	}
	
	/**
	 * Enable zlib compression
	 * 
	 * Wraps the ham_enable_compression function.
	 */
	public synchronized void enableCompression()
			throws Error {
		enableCompression(0);
	}
	
	public synchronized void enableCompression(int level)
			throws Error {
		int status=ham_enable_compression(m_handle, level, 0);
		if (status!=0)
			throw new Error(status);
	}

	/**
	 * Finds a Record by looking up the Key
	 * 
	 * Wraps the ham_find function.
	 */
	public Record find(Key key) 
			throws Error {
		return find(key, 0);
	}
	
	public synchronized Record find(Key key, int flags)
			throws Error {
		if (key==null)
			throw new NullPointerException();
		Record r=new Record();
		r.data=ham_find(m_handle, key.data, flags);
		if (r.data==null)
			throw new Error(getError());
		return r;
	}
	
	/**
	 * Inserts a Key/Record pair.
	 * 
	 * Wraps the ham_insert function.
	 */
	public void insert(Key key, Record record)
			throws Error {
		insert(key, record, 0);
	}
	
	public synchronized void insert(Key key, Record record, int flags)
			throws Error {
		if (key==null || record==null)
			throw new NullPointerException();
		int status=ham_insert(m_handle, key.data, record.data, flags);
		if (status!=0)
			throw new Error(status);
	}
	
	/**
	 * Erases a Key
	 * 
	 * Wraps the ham_erase function.
	 */
	public void erase(Key key)
			throws Error {
		erase(key, 0);
	}
	 
	public synchronized void erase(Key key, int flags)
			throws Error {
		if (key==null)
			throw new NullPointerException();
		int status=ham_erase(m_handle, key.data, flags);
		if (status!=0)
			throw new Error(status);
	}
	
	/**
	 * Flush the Database
	 * 
	 * Wraps the ham_flush function.
	 */
	public synchronized void flush()
			throws Error {
		int status=ham_flush(m_handle, 0);
		if (status!=0)
			throw new Error(status);
	}
	
	/**
	 * Closes the Database
	 * 
	 * Wraps the ham_close function.
	 */
	public void close()
			throws Error {
		close(0);
	}
	
	public synchronized void close(int flags)
			throws Error {
		if (m_handle==0)
			return;
		int status=ham_close(m_handle, flags);
		if (status!=0)
			throw new Error(status);
		ham_delete(m_handle);
		m_handle=0;
	}
	
	/**
	 * Retrieves the database handle
	 */
	public long getHandle() {
		return m_handle;
	}

	/**
	 * The database handle
	 */
	private long m_handle;
	
	public static void main(String[] args) {
		Version v=getVersion();
		System.out.println("version major: "+v.major);
		System.out.println("version minor: "+v.minor);
		System.out.println("version rev: "+v.revision);
		
		License l=getLicense();
		System.out.println("license licensee: "+l.licensee);
		System.out.println("license product: "+l.product);
		
		Database db=new Database();
		try {
			db.create("jtest.db", 0, 0644);
			db.close();
			db.open("jtest.db", 0);
			db.close();
		}
		catch (Error err) {
			System.out.println("Exception: "+err.getMessage());
		}
	}
	
	/*
	 * Don't remove these! They are used in the callback function,
	 * which is implemented by the native library
	 */
	private Comparable m_cmp;
	private PrefixComparable m_prefix_cmp;
	
	static {
		System.loadLibrary("hamsterdb");
	}
}
