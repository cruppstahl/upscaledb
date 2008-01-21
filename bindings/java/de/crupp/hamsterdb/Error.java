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
