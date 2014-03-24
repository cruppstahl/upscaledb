/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 */

package de.crupp.hamsterdb;

import java.util.HashSet;
import java.util.Iterator;

public class Database {

  private native static int ham_get_version(int which);

  private native static String ham_get_license(int which);

  private native static void ham_set_errhandler(ErrorHandler eh);

  private native int ham_db_get_error(long handle);

  private native void ham_db_set_compare_func(long handle,
      CompareCallback cmp);

  private native byte[] ham_db_find(long handle, long txnhandle,
      byte[] key, int flags);

  private native int ham_db_get_parameters(long handle, Parameter[] params);

  private native int ham_db_insert(long handle, long txnhandle,
      byte[] key, byte[] record, int flags);

  private native int ham_db_erase(long handle, long txnhandle,
      byte[] key, int flags);

  private native long ham_db_get_key_count(long handle, long txnhandle,
      int flags);

  private native int ham_db_close(long handle, int flags);

  /**
   * Sets the global error handler.
   * <p>
   * This handler will receive all debug messages that are emitted
   * by hamsterdb. You can install the default handler by setting
   * <code>f</code> to null.
   * <p>
   * The default error handler prints all messages to stderr. To install a
   * different logging facility, you can provide your own error handler.
   * <p>
   * This method wraps the native ham_set_errhandler function.
   * <p>
   * More information: <a href="http://hamsterdb.com/public/scripts/html_www/group__ham__static.html#gac295ec63c4c258b3820006cb2b369d8f">C documentation</a>
   *
   * @param eh ErrorHandler object which is called whenever an error message
   *      is emitted; use null to set the default error handler.
   */
  public static void setErrorHandler(ErrorHandler eh) {
    // set a reference to eh to avoid that it's garbage collected
    m_eh = eh;
    ham_set_errhandler(eh);
  }

  /**
   * Returns the version of the hamsterdb library.
   * <p>
   * This method wraps the native ham_get_version function.
   * <p>
   * More information: <a href="http://hamsterdb.com/public/scripts/html_www/group__ham__static.html#gafdbeaa3c3be6812.1.7d5470f7e984ca">C documentation</a>
   *
   * @return the hamsterdb version tuple
   */
  public static Version getVersion() {
    Version v = new Version();
    v.major = ham_get_version(0);
    v.minor = ham_get_version(1);
    v.revision = ham_get_version(2);
    return v;
  }

  /**
   * Returns the name of the licensee and the name of the licensed product.
   * <p>
   * The licensee name will be empty for non-commercial versions.
   * <p>
   * This method wraps the native ham_get_license function.
   * <p>
   * More information: <a href="http://hamsterdb.com/public/scripts/html_www/group__ham__static.html#gaf4067983dbb50dc2f8127bc90d1bc09f">C documentation</a>
   *
   * @return the hamsterdb licensee information
   */
  public static License getLicense() {
    License l = new License();
    l.licensee = ham_get_license(0);
    l.product = ham_get_license(1);
    return l;
  }

  /**
   * Constructor - creates a Database object
   */
  public Database() {
  }

  /**
   * Constructor - creates a Database object and binds it to a
   * native hamsterdb handle.
   */
  public Database(long handle) {
    m_handle = handle;
  }

  /**
   * Destructor - closes the Database, if it is still open.
   */
  protected void finalize()
      throws DatabaseException {
    close();
  }

  /**
   * Returns the last error code
   * <p>
   * This method wraps the native ham_db_get_error function.
   * <p>
   * More information: <a href="http://hamsterdb.com/public/scripts/html_www/group__ham__Database__cfg__parameters.html#gad7d007973f398906a822a5f58f22d801">C documentation</a>
   *
   * @return the error code of the last operation
   */
  public synchronized int getError() {
    return ham_db_get_error(m_handle);
  }

  /**
   * Sets the comparison function
   * <p>
   * This method wraps the native ham_db_set_compare_func function.
   * <p>
   * The <code>CompareCallback.compare</code> method compares two index
   * keys. It returns -1 if the first key is smaller, +1 if the second
   * key is smaller or 0 if both keys are equal.
   * <p>
   * If <code>cmp</code> is null, hamsterdb will use the default compare
   * function (which is based on memcmp(3)).
   * <p>
   * More information: <a href="http://hamsterdb.com/public/scripts/html_www/group__ham__Database__cfg__parameters.html#ga0fa5d7a6c42.1.7d07075cbfa157834d">C documentation</a>
   * <p>
   * @param cmp an object implementing the CompareCallback interface, or null
   * <p>
   * @see Database#setPrefixComparator
   */
  public synchronized void setComparator(CompareCallback cmp) {
    m_cmp = cmp;
    ham_db_set_compare_func(m_handle, m_cmp);
  }

  /**
   * Searches an item in the Database, returns the record
   * <p>
   * This method wraps the native ham_db_find function.
   * <p>
   * This function searches the Database for a key. If the key
   * is found, the method will return the record of this item.
   * <p>
   * <code>Database.find</code> can not search for duplicate keys. If the
   * key has multiple duplicates, only the first duplicate is returned.
   * <p>
   * More information: <a href="http://hamsterdb.com/public/scripts/html_www/group__ham__Database__cfg__parameters.html#ga1385b79dab227fda11bbc80ceb929233">C documentation</a>
   * <p>
   * @param txn the (optional) Transaction
   * @param key the key of the item
   * <p>
   * @return the record of the item
   */
  public byte[] find(Transaction txn, byte[] key)
      throws DatabaseException {
    if (key == null)
      throw new NullPointerException();
    byte[] r;
    r = ham_db_find(m_handle, txn!=null ? txn.getHandle() : 0, key, 0);
    if (r == null)
      throw new DatabaseException(getError());
    return r;
  }

  /**
   * Searches an item in the Database, returns the record
   *
   * @see Database#find(Transaction, byte[])
   */
  public byte[] find(byte[] key)
      throws DatabaseException {
    return find(null, key);
  }

  /**
   * Inserts a Database item
   *
   * @see Database#insert(Transaction, byte[], byte[], int)
   */
  public void insert(byte[] key, byte[] record)
      throws DatabaseException {
    insert(key, record, 0);
  }

  /**
   * Inserts a Database item
   *
   * @see Database#insert(Transaction, byte[], byte[], int)
   */
  public synchronized void insert(byte[] key, byte[] record, int flags)
      throws DatabaseException {
    insert(null, key, record, flags);
  }

  /**
   * Inserts a Database item
   *
   * @see Database#insert(Transaction, byte[], byte[], int)
   */
  public synchronized void insert(Transaction txn, byte[] key,
    byte[] record)
      throws DatabaseException {
    insert(txn, key, record, 0);
  }

  /**
   * Inserts a Database item
   * <p>
   * This method wraps the native ham_db_insert function.
   * <p>
   * This function inserts a key/record pair as a new Database item.
   * <p>
   * If the key already exists in the Database, error code
   * <code>Const.HAM_DUPLICATE_KEY</code> is thrown.
   * <p>
   * If you wish to overwrite an existing entry specify the
   * flag <code>Const.HAM_OVERWRITE</code>.
   * <p>
   * If you wish to insert a duplicate key specify the flag
   * <code>Const.HAM_DUPLICATE</code>. (Note that the Database has to
   * be created with <code>Const.HAM_ENABLE_DUPLICATE_KEYS</code> in order
   * to use duplicate keys.) <br>
   * The duplicate key is inserted after all other duplicate keys (see
   * <code>Const.HAM_DUPLICATE_INSERT_LAST</code>).
   * <p>
   * @param txn the (optional) Transaction
   * @param key the key of the new item
   * @param record the record of the new item
   * @param flags optional flags for inserting. Possible flags are:
   *    <ul>
   *    <li><code>Const.HAM_OVERWRITE</code>. If the key already
   *      exists, the record is overwritten. Otherwise, the key is
   *      inserted.
   *    <li><code>Const.HAM_DUPLICATE</code>. If the key already
   *      exists, a duplicate key is inserted. The key is inserted
   *      before the already existing duplicates.
   *    </ul>
   * <p>
   * More information: <a href="http://hamsterdb.com/public/scripts/html_www/group__ham__Database__cfg__parameters.html#ga5bb99ca3c41f069db310123253c1c1fb">C documentation</a>
   */
  public synchronized void insert(Transaction txn, byte[] key,
      byte[] record, int flags)
      throws DatabaseException {
    if (key == null || record == null)
      throw new NullPointerException();
    int status = ham_db_insert(m_handle, txn != null ? txn.getHandle() : 0,
                key, record, flags);
    if (status != 0)
      throw new DatabaseException(status);
  }

  /**
   * Erases a Database item
   *
   * @see Database#erase(Transaction, byte[])
   */
  public synchronized void erase(byte[] key)
      throws DatabaseException {
    erase(null, key);
  }

  /**
   * Erases a Database item
   * <p>
   * This method wraps the native ham_db_erase function.
   * <p>
   * This function erases a Database item. If the item with the specified key
   * does not exist, <code>Const.HAM_KEY_NOT_FOUND</code> is thrown.
   * <p>
   * Note that this method can not erase a single duplicate key. If the key
   * has multiple duplicates, all duplicates of this key will be erased.
   * Use <code>Cursor.erase</code> to erase a specific duplicate key.
   * <p>
   * @param txn the (optional) Transaction
   * @param key the key to delete
   * <p>
   * More information: <a href="http://hamsterdb.com/public/scripts/html_www/group__ham__Database__cfg__parameters.html#ga79acbb3f8c06f28b089b9d86cae707db">C documentation</a>
   *
   * @see Cursor#erase
   */
  public synchronized void erase(Transaction txn, byte[] key)
      throws DatabaseException {
    if (key == null)
      throw new NullPointerException();
    int status = ham_db_erase(m_handle, txn != null ? txn.getHandle() : 0,
          key, 0);
    if (status != 0)
      throw new DatabaseException(status);
  }

  /**
   * Retrieve the current value for a given Database setting
   * <p>
   * Only those values requested by the parameter array will be stored.
   * <p>
   * @param params A Parameter list of all values that should be retrieved
   * <p>
   * More information: <a href="http://hamsterdb.com/public/scripts/html_www/group__ham__Database__cfg__parameters.html#gadbcbed98c301c6e6d76c84ebe3a147dd">C documentation</a>
   */
  public synchronized void getParameters(Parameter[] params)
      throws DatabaseException {
    int status = ham_db_get_parameters(m_handle, params);
    if (status != 0)
      throw new DatabaseException(status);
  }

  /**
   * Get key count
   *
   * @see Database#getKeyCount(Transaction, int)
   */
  public synchronized long getKeyCount(Transaction txn)
      throws DatabaseException {
    return getKeyCount(txn, 0);
  }

  /**
   * Get key count
   *
   * @see Database#getKeyCount(Transaction, int)
   */
  public synchronized long getKeyCount()
      throws DatabaseException {
    return getKeyCount(null, 0);
  }

  /**
   * Get key count
   *
   * @see Database#getKeyCount(Transaction, int)
   */
  public synchronized long getKeyCount(int flags)
      throws DatabaseException {
    return getKeyCount(null, flags);
  }

  /**
   * Calculates the number of keys stored in the Database
   * <p>
   * More information: <a href="http://hamsterdb.com/public/scripts/html_www/group__ham__Database__cfg__parameters.html#ga11d238e331daf01b520fadaa7d77a9df">C documentation</a>
   */
  public synchronized long getKeyCount(Transaction txn, int flags)
      throws DatabaseException {
    // native function will throw exception
    return ham_db_get_key_count(m_handle, txn != null ? txn.getHandle() : 0,
                flags);
  }

  /**
   * Closes the Database
   */
  public void close() {
    close(0);
  }

  /**
   * Closes the Database
   * <p>
   * This method wraps the native ham_db_close function.
   * <p>
   * This function flushes the Database and then closes the file handle.
   * <p>
   * More information: <a href="http://hamsterdb.com/public/scripts/html_www/group__ham__Database__cfg__parameters.html#gac0e1e492c2b36e2ae0e87d0c0ff6e04e">C documentation</a>
   */
  public synchronized void close(int flags) {
    if (m_handle != 0) {
      closeCursors();
      ham_db_close(m_handle, flags);
      m_handle = 0;
    }
  }

  private HashSet<Cursor> m_cursor_set;

  /**
   * Retrieves the database handle
   */
  public long getHandle() {
    return m_handle;
  }

  /**
   * Adds a cursor - used by Cursor.java
   */
  public void addCursor(Cursor cursor) {
    if (m_cursor_set == null)
      m_cursor_set = new HashSet<Cursor>();
    m_cursor_set.add(cursor);
  }

  /**
   * Removes a cursor - used by Cursor.java
   */
  public void removeCursor(Cursor cursor) {
    m_cursor_set.remove(cursor);
  }

  /**
   * closes all cursors of this database
   */
  public void closeCursors() {
    if (m_cursor_set == null)
      return;
    Iterator iterator = m_cursor_set.iterator();
    while (iterator.hasNext()) {
      Cursor c = (Cursor)iterator.next();
      try {
        c.closeNoLock();
      }
      catch (Exception e) {
      }
    }
    m_cursor_set.clear();
  }

  /**
   * The database handle
   */
  private long m_handle;

  /*
   * Don't remove these! They are used in the callback function,
   * which is implemented in the native library
   */
  private CompareCallback m_cmp;
  private DuplicateCompareCallback m_dupe_cmp;
  private static ErrorHandler m_eh;

  static {
    System.loadLibrary("hamsterdb-java");
  }
}
