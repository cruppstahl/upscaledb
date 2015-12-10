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

  private native int uqi_result_get_key_size(long handle);

  private native byte[] uqi_result_get_key(long handle, int row);

  private native byte[] uqi_result_get_key_data(long handle);

  private native int uqi_result_get_record_type(long handle);

  private native byte[] uqi_result_get_record(long handle, int row);

  private native byte[] uqi_result_get_record_data(long handle);

  private native void uqi_result_close(long handle);

  /**
   * Constructor - assigns a Result handle
   */
  public Result(long handle) {
    m_handle = handle;
    m_row_count = uqi_result_get_row_count(handle);
    m_key_type = uqi_result_get_key_type(handle);
    m_record_type = uqi_result_get_record_type(handle);
  }

  /**
   * Returns an array with all keys
   */
  public byte getKeyData()
      throws DatabaseException {
    return uqi_result_get_key_data(m_handle);
  }

  /**
   * Returns data of a specific key
   */
  public byte getKeyData(int row)
      throws DatabaseException {
    return uqi_result_get_key(m_handle, row);
  }

  /**
   * Returns an array with all records
   */
  public byte getRecordData()
      throws DatabaseException {
    return uqi_result_get_record_data(m_handle);
  }

  /**
   * Returns data of a specific record
   */
  public byte getRecordData(int row)
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
  private int m_key_type;
  private int m_record_type;
  private int m_row_count;
}
