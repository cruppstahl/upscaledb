package de.crupp.hamsterdb;

public class Key {
	
	public final int HAM_KEY_USER_ALLOC = 1;

	/**
	 * Constructor
	 * @param data The Key data as a byte array
	 */
	public Key(byte[] data) {
		m_data=data;
	}
	
	/**
	 * Constructor
	 * @param data The Key data as a byte array
	 * @param flags Flags for this Key
	 */
	public Key(byte[] data, int flags) {
		m_data=data;
		m_flags=flags;
	}
	
	/**
	 * Getter for the Key data
	 */
	public byte[] getData() {
		return m_data;
	}
	
	/**
	 * Setter for the Key data
	 */
	public void setData(byte[] data) {
		m_data=data;
	}
	
	/**
	 * Getter for the Key flags
	 */
	public int getFlags() {
		return m_flags;
	}
	
	/**
	 * Setter for the Key flags
	 */
	public void setFlags(int flags) {
		m_flags=flags;
	}
		
	/**
	 * The Key data 
	 */
	private byte[] m_data;
	
	/**
	 * The Key flags
	 */
	private int m_flags;
}
