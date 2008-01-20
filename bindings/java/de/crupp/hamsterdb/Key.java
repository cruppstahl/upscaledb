package de.crupp.hamsterdb;

public class Key {
	
	public final static int HAM_KEY_USER_ALLOC = 1;

	/**
	 * Constructor
	 * @param data The Key data as a byte array
	 */
	public Key(byte[] data) {
		this.data=data;
	}
	
	/**
	 * Getter for the Key data
	 */
	public byte[] getData() {
		return this.data;
	}
	
	/**
	 * Setter for the Key data
	 */
	public void setData(byte[] data) {
		this.data=data;
	}

	/**
	 * The Key data 
	 */
	public byte[] data;
}
