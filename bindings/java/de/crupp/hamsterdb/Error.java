package de.crupp.hamsterdb;

public class Error {

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
	 */
	public String getString() {
		return "TODO"; // TODO call native ham_strerror
	}
	
	/**
	 * The error number
	 */
	private int m_errno;
}
