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

public class Cursor {

  private native long ups_cursor_create(long dbhandle, long txnhandle);

  private native long ups_cursor_clone(long handle);

  private native int ups_cursor_move_to(long handle, int flags);

  private native byte[] ups_cursor_get_key(long handle, int flags);

  private native byte[] ups_cursor_get_record(long handle, int flags);

  private native int ups_cursor_overwrite(long handle,
                        byte[] record, int flags);

  private native int ups_cursor_find(long handle,
                        byte[] key, int flags);

  private native int ups_cursor_insert(long handle,
                        byte[] key, byte[] record, int flags);

  private native int ups_cursor_erase(long handle, int flags);

  private native int ups_cursor_get_duplicate_count(long handle, int flags);

  private native long ups_cursor_get_record_size(long handle);

  private native int ups_cursor_close(long handle);

  /**
   * Constructor - assigns a Database object and a Cursor handle
   */
  public Cursor(Database db, long handle) {
    m_db = db;
    m_handle = handle;
  }

  /**
   * Constructor - creates a new Cursor of a Database
   *
   * @see Cursor#create(Database)
   */
  public Cursor(Database db)
      throws DatabaseException {
    create(db);
  }

  /**
   * Constructor - creates a new Cursor of a Database in an
   * existing Transaction
   *
   * @see Cursor#create(Database)
   */
  public Cursor(Database db, Transaction txn)
      throws DatabaseException {
    create(db, txn);
  }

  /**
   * Creates a new Cursor
   * <p>
   * This method wraps the native ups_cursor_create function.
   * <p>
   * Creates a new Database Cursor. Cursors can be used to
   * traverse the Database from start to end or vice versa. Cursors
   * can also be used to insert, delete or search Database items.
   * <p>
   * More information: <a href="http://files.upscaledb.com/documentation/html/group__ups__cursor.html#gae1e390f43321120c360135f90d3687c5">C documentation</a>
   *
   * @param db the Database object
   * @param txn an optional Transaction object
   */
  public void create(Database db, Transaction txn)
      throws DatabaseException {
    m_db = db;
    m_handle = ups_cursor_create(db.getHandle(),
                txn != null ? txn.getHandle() : 0);
    if (m_handle == 0)
      throw new DatabaseException(Const.UPS_INTERNAL_ERROR);
    m_db.addCursor(this);
  }

  /*
   * Constructor - creates a new Cursor of a Database
   *
   * @see Cursor#create(Database, Transaction)
   */
  public void create(Database db)
      throws DatabaseException {
    create(db, null);
  }

  /**
   * Clones a Database Cursor
   * <p>
   * This method wraps the native ups_cursor_clone function.
   * <p>
   * Clones an existing Cursor. The new Cursor will point to exactly the
   * same item as the old Cursor. If the old Cursor did not point
   * to any item, so will the new Cursor.
   * <p>
   * More information: <a href="http://files.upscaledb.com/documentation/html/group__ups__cursor.html#gaa3573d6c5e8da3667b78025a0b1b8f98">C documentation</a>
   *
   * @return the new Cursor object
   */
  public Cursor cloneCursor()
      throws DatabaseException {
    if (m_handle == 0)
      throw new DatabaseException(Const.UPS_INV_PARAMETER);
    long newhandle = ups_cursor_clone(m_handle);
    if (newhandle == 0)
      throw new DatabaseException(Const.UPS_INTERNAL_ERROR);
    Cursor c = new Cursor(m_db, newhandle);
    m_db.addCursor(c);
    return c;
  }

  /**
   * Moves the Cursor to the direction specified in the flags.
   * <p>
   * This method wraps the native ups_cursor_move function.
   * <p>
   * More information: <a href="http://files.upscaledb.com/documentation/html/group__ups__cursor.html#gaac3c7fa55e3a156bb91381af13bd33c9">C documentation</a>
   *
   * @param flags the direction for the move. If no direction is specified,
   *      the Cursor will remain on the current position.
   *      Possible flags are:
   *    <ul>
   *    <li><code>Const.UPS_CURSOR_FIRST</code> positions the Cursor
   *      on the first item in the Database
   *    <li><code>Const.UPS_CURSOR_LAST</code> positions the Cursor
   *      on the last item in the Database
   *    <li><code>Const.UPS_CURSOR_NEXT</code> positions the Cursor
   *      on the next item in the Database; if the Cursor does not point
   *      to any item, the function behaves as if direction was
   *      <code>Const.UPS_CURSOR_FIRST</code>.
   *    <li><code>Const.UPS_CURSOR_PREVIOUS</code> positions the Cursor
   *      on the previous item in the Database; if the Cursor does not
   *      point to any item, the function behaves as if direction was
   *      <code>Const.UPS_CURSOR_LAST</code>.
   *    <li><code>Const.UPS_SKIP_DUPLICATES</code> skip duplicate keys of
   *      the current key. Not allowed in combination with
   *      <code>Const.UPS_ONLY_DUPLICATES</code>.
   *    <li><code>Const.UPS_ONLY_DUPLICATES</code> only move through
   *      duplicate keys of the current key. Not allowed in combination
   *      with <code>Const.UPS_SKIP_DUPLICATES</code>.
   *    </ul>
   */
  public void move(int flags)
      throws DatabaseException {
    int status = ups_cursor_move_to(m_handle, flags);
    if (status != 0)
      throw new DatabaseException(status);
  }

  /**
   * Moves the Cursor to the first Database element
   *
   * @see Cursor#move(int)
   */
  public void moveFirst()
      throws DatabaseException {
    move(Const.UPS_CURSOR_FIRST);
  }

  /**
   * Moves the Cursor to the last Database element
   *
   * @see Cursor#move(int)
   */
  public void moveLast()
      throws DatabaseException {
    move(Const.UPS_CURSOR_LAST);
  }

  /**
   * Moves the Cursor to the next Database element
   *
   * @see Cursor#moveNext(int)
   * @see Cursor#move(int)
   */
  public void moveNext()
      throws DatabaseException {
    move(Const.UPS_CURSOR_NEXT);
  }

  /**
   * Moves the Cursor to the next Database element
   *
   * @see Cursor#move(int)
   */
  public void moveNext(int flags)
      throws DatabaseException {
    move(Const.UPS_CURSOR_NEXT | flags);
  }

  /**
   * Moves the Cursor to the previous Database element
   *
   * @see Cursor#movePrevious(int)
   * @see Cursor#move(int)
   */
  public void movePrevious()
      throws DatabaseException {
    move(Const.UPS_CURSOR_PREVIOUS);
  }

  /**
   * Moves the Cursor to the previous Database element
   *
   * @see Cursor#move(int)
   */
  public void movePrevious(int flags)
      throws DatabaseException {
    move(Const.UPS_CURSOR_PREVIOUS | flags);
  }

  /**
   * Retrieves the Key of the current item
   * <p>
   * This method wraps the native ups_cursor_move function.
   * <p>
   * Returns the key of the current Database item. Throws
   * <code>Const.UPS_CURSOR_IS_NIL</code> if the Cursor does not point to
   * any item.
   * <p>
   * More information: <a href="http://files.upscaledb.com/documentation/html/group__ups__cursor.html#gaac3c7fa55e3a156bb91381af13bd33c9">C documentation</a>
   *
   * @return the key of the current item
   */
  public byte[] getKey()
      throws DatabaseException {
    byte[] ret = ups_cursor_get_key(m_handle, 0);
    if (ret == null)
      throw new DatabaseException(Const.UPS_CURSOR_IS_NIL);
    return ret;
  }

  /**
   * Retrieves the Record of the current item
   * <p>
   * This method wraps the native ups_cursor_move function.
   * <p>
   * Returns the record of the current Database item. Throws
   * <code>Const.UPS_CURSOR_IS_NIL</code> if the Cursor does not point to
   * any item.
   * <p>
   * More information: <a href="http://files.upscaledb.com/documentation/html/group__ups__cursor.html#gaac3c7fa55e3a156bb91381af13bd33c9">C documentation</a>
   *
   * @return the record of the current item
   */
  public byte[] getRecord()
      throws DatabaseException {
    byte[] ret = ups_cursor_get_record(m_handle, 0);
    if (ret == null)
      throw new DatabaseException(Const.UPS_CURSOR_IS_NIL);
    return ret;
  }

  /**
   * Overwrites the current Record
   * <p>
   * This method wraps the native ups_cursor_overwrite function.
   * <p>
   * This function overwrites the record of the current item.
   * <p>
   * More information: <a href="http://files.upscaledb.com/documentation/html/group__ups__cursor.html#ga75c375bb54df99570b9865a7b4c8d14e">C documentation</a>
   *
   * @param record the new Record of the item
   */
  public void overwrite(byte[] record)
      throws DatabaseException {
    if (record == null)
      throw new NullPointerException();
    int status = ups_cursor_overwrite(m_handle, record, 0);
    if (status != 0)
      throw new DatabaseException(status);
  }

  /**
   * Searches a key and points the Cursor to this key
   * <p>
   * This method wraps the native ups_cursor_find function.
   * <p>
   * Searches for an item in the Database and points the Cursor to this
   * item. If the item could not be found, the Cursor is not modified.
   * <p>
   * If the key has multiple duplicates, the Cursor is positioned on the
   * first duplicate.
   * <p>
   * More information: <a href="http://files.upscaledb.com/documentation/html/group__ups__cursor.html#ga70654fc7be217a62afec3900661bd18b">C documentation</a>
   *
   * @param key the key to search for
   */
  public void find(byte[] key)
      throws DatabaseException {
    if (key == null)
      throw new NullPointerException();
    int status = ups_cursor_find(m_handle, key, 0);
    if (status != 0)
      throw new DatabaseException(status);
  }

  /**
   * Inserts a Database item and points the Cursor to the inserted item
   *
   * @see Cursor#insert(byte[], byte[], int)
   */
  public void insert(byte[] key, byte[] record)
      throws DatabaseException {
    insert(key, record, 0);
  }

  /**
   * Inserts a Database item and points the Cursor to the inserted item
   * <p>
   * This method wraps the native ups_cursor_insert function.
   * <p>
   * This function inserts a key/record pair as a new Database item.
   * If the key already exists in the Database, error
   * <code>Const.UPS_DUPLICATE_KEY</code> is thrown.
   * <p>
   * If you wish to overwrite an existing entry specify the flag
   * <code>Const.UPS_OVERWRITE</code>.
   * <p>
   * If you wish to insert a duplicate key specify the flag
   * <code>Const.UPS_DUPLICATE</code>. (Note that the Database has to
   * be created with <code>Const.UPS_ENABLE_DUPLICATE_KEYS</code>, in order
   * to use duplicate keys.)
   * <p>
   * After inserting, the Cursor will point to the new item. If inserting
   * the item failed, the Cursor is not modified.
   * <p>
   * More information: <a href="http://files.upscaledb.com/documentation/html/group__ups__cursor.html#gab779d0fd20b5df9c92aca9e912fd20b0">C documentation</a>
   *
   * @param key the key of the new item
   * @param record the record of the new item
   * @param flags optional flags for inserting the item, combined with
   *      bitwise OR. Possible flags are:
   *    <ul>
   *    <li><code>Const.UPS_OVERWRITE</code>. If the key already
   *      exists, the record is overwritten. Otherwise, the key is
   *      inserted.
   *    <li><code>Const.UPS_DUPLICATE</code>. If the key already
   *      exists, a duplicate key is inserted. The key is inserted
   *      after the already existing duplicates. Same as
   *      <code>Const.UPS_DUPLICATE_INSERT_LAST</code>.
   *    <li><code>Const.UPS_DUPLICATE_INSERT_BEFORE</code>. If the key
   *      already exists, a duplicate key is inserted before the
   *      duplicate pointed to by this Cursor.
   *    <li><code>Const.UPS_DUPLICATE_INSERT_AFTER</code>. If the key
   *      already exists, a duplicate key is inserted after the
   *      duplicate pointed to by this Cursor.
   *    <li><code>Const.UPS_DUPLICATE_INSERT_FIRST</code>. If the key
   *      already exists, a duplicate key is inserted as the first
   *      duplicate of the current key.
   *    <li><code>Const.UPS_DUPLICATE_INSERT_LAST</code>. If the key
   *      already exists, a duplicate key is inserted as the last
   *      duplicate of the current key.
   *    </ul>
   */
  public void insert(byte[] key, byte[] record, int flags)
      throws DatabaseException {
    if (key == null || record == null)
      throw new NullPointerException();
    int status = ups_cursor_insert(m_handle, key, record, flags);
    if (status != 0)
      throw new DatabaseException(status);
  }

  /**
   * Erases the current key
   * <p>
   * This method wraps the native ups_cursor_erase function.
   * <p>
   * Erases a key from the Database. If the erase was successfull, the
   * Cursor is invalidated, and does no longer point to any item.
   * In case of an error, the Cursor is not modified.
   * <p>
   * If the Database was opened with the flag
   * <code>Const.UPS_ENABLE_DUPLICATE_KEYS</code>, this function erases only
   * the duplicate item to which the Cursor refers.
   * <p>
   * More information: <a href="http://files.upscaledb.com/documentation/html/group__ups__cursor.html#gaa1f88fb3a914a15339666519e4a180e1">C documentation</a>
   */
  public void erase()
      throws DatabaseException {
    int status = ups_cursor_erase(m_handle, 0);
    if (status != 0)
      throw new DatabaseException(status);
  }

  /**
   * Returns the number of duplicate keys
   * <p>
   * This method wraps the native ups_cursor_get_duplicate_count function.
   * <p>
   * Returns the number of duplicate keys of the item to which the
   * Cursor currently refers.<br>
   * Returns 1 if the key has no duplicates.
   * <p>
   * More information: <a href="http://files.upscaledb.com/documentation/html/group__ups__cursor.html#gac80d65ee801bd81723831d81748ffcfd">C documentation</a>
   *
   * @return the number of duplicate keys
   */
  public int getDuplicateCount()
      throws DatabaseException {
    return ups_cursor_get_duplicate_count(m_handle, 0);
  }

  /**
   * Returns the size of the current record
   * <p>
   * Returns the record size of the item to which the Cursor
   * currently refers.
   * <p>
   * More information: <a href="http://files.upscaledb.com/documentation/html/group__ups__cursor.html#ga17c1f98d59cd0da5e094c7d9939c1d36">C documentation</a>
   *
   * @return the size of the record
   */
  public long getRecordCount()
      throws DatabaseException {
    return ups_cursor_get_record_size(m_handle);
  }

  /**
   * Closes the Cursor
   * <p>
   * This method wraps the native ups_cursor_close function.
   * <p>
   * Closes this Cursor and frees allocated memory. All Cursors
   * should be closed before closing the Database.
   * <p>
   * More information: <a href="http://files.upscaledb.com/documentation/html/group__ups__cursor.html#ga1b92b930f157ff2d9e0896e9f639085f">C documentation</a>
   */
  public void close()
      throws DatabaseException {
    int status;
    if (m_handle == 0)
      return;
    closeNoLock();
    m_db.removeCursor(this);
  }

  public void closeNoLock()
      throws DatabaseException {
    int status = ups_cursor_close(m_handle);
    if (status != 0)
      throw new DatabaseException(status);
    m_handle = 0;
  }

  /**
   * Returns the Cursor handle
   */
  public long getHandle() {
    return m_handle;
  }

  private long m_handle;
  private Database m_db;
}
