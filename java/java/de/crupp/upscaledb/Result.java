/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
 * All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * See the file COPYING for License information.
 */

package de.crupp.upscaledb;

public class Result {

  private native int uqi_result_get_row_count(long handle);

  private native int uqi_result_get_key_type(long handle);

  private native byte[] uqi_result_get_key(long handle, int row);

  private native byte[] uqi_result_get_key_data(long handle);

  private native int uqi_result_get_record_type(long handle);

  private native byte[] uqi_result_get_record(long handle, int row);

  private native byte[] uqi_result_get_record_data(long handle);

  private native void uqi_result_close(long handle);

  /**
   * Constructor - assigns a Result handle and initializes a few fields
   */
  public Result(long handle) {
    m_handle = handle;
  }

  /**
   * Returns the row count
   */
  public int getRowCount() {
    return uqi_result_get_row_count(m_handle);
  }

  /**
   * Returns the key type.
   *
   * This is the same type as specified when creating the queried database
   * (i.e. Const.UPS_TYPE_UINT32, Const.UPS_TYPE_BINARY etc).
   */
  public int getKeyType() {
    return uqi_result_get_key_type(m_handle);
  }

  /**
   * Returns a byte array with all keys.
   *
   * This is the fastest way of accessing the results, especially if there
   * are many of them. Only use this if the keys have a fixed length.
   * I.e. if key size is 4 (bytes), then key 0 starts at offset 0,
   * key 1 starts at offset 4, key 2 at offset 8 etc.
   */
  public byte[] getKeyData()
      throws DatabaseException {
    return uqi_result_get_key_data(m_handle);
  }

  /**
   * Returns data of a key of a specific row.
   *
   * @sa Result.getRowCount
   */
  public byte[] getKey(int row)
      throws DatabaseException {
    return uqi_result_get_key(m_handle, row);
  }

  /**
   * Returns the record type.
   *
   * This is the same type as specified when creating the queried database
   * (i.e. Const.UPS_TYPE_UINT32, Const.UPS_TYPE_BINARY etc).
   */
  public int getRecordType() {
    return uqi_result_get_record_type(m_handle);
  }

  /**
   * Returns an array with all records
   *
   * This is the fastest way of accessing the results, especially if there
   * are many of them. Only use this if the records have a fixed length.
   * I.e. if record size is 4 (bytes), then record 0 starts at offset 0,
   * record 1 starts at offset 4, record 2 at offset 8 etc.
   */
  public byte[] getRecordData()
      throws DatabaseException {
    return uqi_result_get_record_data(m_handle);
  }

  /**
   * Returns the record of a specific row.
   *
   * @sa Result.getRowCount
   */
  public byte[] getRecord(int row)
      throws DatabaseException {
    return uqi_result_get_record(m_handle, row);
  }

  /**
   * Destructor - automatically closes the Result
   */
  public void finalize()
      throws DatabaseException {
    close();
  }

  /**
   * Closes the Result.
   * <p>
   * This method wraps the native uqi_result_close function.
   */
  public void close()
      throws DatabaseException {
    if (m_handle == 0)
      return;
    uqi_result_close(m_handle);
    m_handle = 0;
  }

  private long m_handle;
}
