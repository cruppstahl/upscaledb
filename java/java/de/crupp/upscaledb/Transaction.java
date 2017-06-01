/*
 * Copyright (C) 2005-2017 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
