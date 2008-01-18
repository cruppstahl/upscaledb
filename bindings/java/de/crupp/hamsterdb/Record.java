package de.crupp.hamsterdb;

public class Record {

	public final int HAM_RECORD_USER_ALLOC = 1;
	
	/**
	 * Constructor
	 * @param data The Record data as a byte array
	 */
	public Record(byte[] data) {
		m_data=data;
	}
	
	/**
	 * Constructor
	 * @param data The Record data as a byte array
	 * @param flags Flags for this Record
	 */
	public Record(byte[] data, int flags) {
		m_data=data;
		m_flags=flags;
	}
	
	/**
	 * Getter for the Record data
	 */
	public byte[] getData() {
		return m_data;
	}
	
	/**
	 * Setter for the Record data
	 */
	public void setData(byte[] data) {
		m_data=data;
	}
	
	/**
	 * Getter for the Record flags
	 */
	public int getFlags() {
		return m_flags;
	}
	
	/**
	 * Setter for the Record flags
	 */
	public void setFlags(int flags) {
		m_flags=flags;
	}
		
	/**
	 * The Record data 
	 */
	private byte[] m_data;
	
	/**
	 * The Record flags
	 */
	private int m_flags;
}
