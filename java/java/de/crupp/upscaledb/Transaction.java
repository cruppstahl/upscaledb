/*
 * Copyright (C) 2005-2016 Christoph Rupp (chris@crupp.de).
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

public class Transaction {

  private native int ups_txn_commit(long handle, int flags);

  private native int ups_txn_abort(long handle, int flags);

  /**
   * Constructor - assigns an Environment object and a Transaction handle
   */
  public Transaction(Environment env, long handle) {
    m_env = env;
    m_handle = handle;
  }

  /**
   * Destructor - automatically aborts the Transaction
   */
  public void finalize()
      throws DatabaseException {
    abort();
  }

  /**
   * Aborts the Transaction
   * <p>
   * This method wraps the native ups_txn_abort function.
   * <p>
   * More information: <a href="http://files.upscaledb.com/documentation/html/group__ups__txn.html#ga3a1c2a2a3efa859f9966f53d019598c9">C documentation</a>
   */
  public void abort()
      throws DatabaseException {
    if (m_handle == 0)
      return;
    int status = ups_txn_abort(m_handle, 0);
    if (status != 0)
      throw new DatabaseException(status);
    m_handle = 0;
  }

  /**
   * Commits the Transaction
   * <p>
   * This method wraps the native ups_txn_commit function.
   * <p>
   * More information: <a href="http://files.upscaledb.com/documentation/html/group__ups__txn.html#gada266b6c9cdc883b77adf0268129076c">C documentation</a>
   */
  public void commit()
      throws DatabaseException {
    if (m_handle == 0)
      return;
    int status = ups_txn_commit(m_handle, 0);
    if (status != 0)
      throw new DatabaseException(status);
    m_handle = 0;
  }

  /**
   * Sets the Transaction handle
   */
  public void setHandle(long h) {
    m_handle = h;
  }

  /**
   * Returns the Transaction handle
   */
  public long getHandle() {
    return m_handle;
  }

  private long m_handle;
  private Environment m_env;
}
