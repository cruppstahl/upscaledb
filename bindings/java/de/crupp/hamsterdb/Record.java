package de.crupp.hamsterdb;

public class Record {

	/**
	 * Constructor
	 */
	public Record() {
	}
	
	/**
	 * Constructor
	 * @param data The Record data as a byte array
	 */
	public Record(byte[] data) {
		this.data=data;
	}
	
	/**
	 * Getter for the Record data
	 */
	public byte[] getData() {
		return this.data;
	}
	
	/**
	 * Setter for the Record data
	 */
	public void setData(byte[] data) {
		this.data=data;
	}
	
	/**
	 * The Record data 
	 */
	public byte[] data;
}
