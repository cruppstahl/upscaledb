package de.crupp.hamsterdb;

public class Error extends java.lang.Throwable {
	
	public final static int HAM_SUCCESS                  = 0;
	public final static int HAM_INV_KEYSIZE              = -3;
	public final static int HAM_INV_PAGESIZE             = -4;
	public final static int HAM_OUT_OF_MEMORY            = -6;
	public final static int HAM_NOT_INITIALIZED          = -7;
	public final static int HAM_INV_PARAMETER            = -8;
	public final static int HAM_INV_FILE_HEADER          = -9;
	public final static int HAM_INV_FILE_VERSION         =-10;
	public final static int HAM_KEY_NOT_FOUND            =-11;
	public final static int HAM_DUPLICATE_KEY            =-12;
	public final static int HAM_INTEGRITY_VIOLATED       =-13;
	public final static int HAM_INTERNAL_ERROR           =-14;
	public final static int HAM_DB_READ_ONLY             =-15;
	public final static int HAM_BLOB_NOT_FOUND           =-16;
	public final static int HAM_PREFIX_REQUEST_FULLKEY   =-17;
	public final static int HAM_IO_ERROR                 =-18;
	public final static int HAM_CACHE_FULL               =-19;
	public final static int HAM_NOT_IMPLEMENTED          =-20;
	public final static int HAM_FILE_NOT_FOUND           =-21;
	public final static int HAM_WOULD_BLOCK              =-22;
	public final static int HAM_ACCESS_DENIED            =-25;
	public final static int HAM_NOT_READY                =-23;
	public final static int HAM_LIMITS_REACHED           =-24;
	public final static int HAM_ALREADY_INITIALIZED      =-27;
	public final static int HAM_CURSOR_IS_NIL           =-100;
	public final static int HAM_DATABASE_NOT_FOUND      =-200;
	public final static int HAM_DATABASE_ALREADY_EXISTS =-201;
	public final static int HAM_DATABASE_ALREADY_OPEN   =-202;

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
