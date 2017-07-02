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

import de.crupp.upscaledb.Transaction;
import de.crupp.upscaledb.Cursor;
import de.crupp.upscaledb.Result;

public class Environment {

  private native long ups_env_create(String filename, int flags, int mode,
      Parameter[] params);

  private native long ups_env_open(String filename, int flags,
      Parameter[] params);

  private native long ups_env_create_db(long handle, short name, int flags,
      Parameter[] params);

  private native long ups_env_open_db(long handle, short name, int flags,
      Parameter[] params);

  private native int ups_env_get_parameters(long handle, Parameter[] params);

  private native int ups_env_rename_db(long handle, short oldname,
      short newname, int flags);

  private native int ups_env_erase_db(long handle, short name, int flags);

  private native short[] ups_env_get_database_names(long handle);

  private native int ups_env_flush(long handle);

  private native long ups_txn_begin(long handle, int flags);

  private native long ups_env_select_range(long handle, String query,
      long begin, long end);

  private native int ups_env_close(long handle, int flags);

  /**
   * Constructor - creates an empty Environment
   */
  public Environment() {
  }

  /**
   * Destructor - closes the Environment
   */
  public void finalize()
      throws DatabaseException {
    close();
  }

  /**
   * Creates a new Database Environment
   *
   * @see Environment#create(String, int, int, Parameter[])
   */
  public void create(String filename)
      throws DatabaseException {
    create(filename, 0, 0644, null);
  }

  /**
   * Creates a new Database Environment
   *
   * @see Environment#create(String, int, int, Parameter[])
   */
  public void create(String filename, int flags)
      throws DatabaseException {
    create(filename, flags, 0644, null);
  }

  /**
   * Creates a new Database Environment
   *
   * @see Environment#create(String, int, int, Parameter[])
   */
  public void create(String filename, int flags, int mode)
      throws DatabaseException {
    create(filename, flags, mode, null);
  }

  /**
   * Creates a new Database Environment
   * <p>
   * This method wraps the native ups_env_create function.
   * <p>
   * @param filename the filename of the Environment file. If the file
   *      already exists, it is overwritten. Can be null for an In-Memory
   *      Environment.
   * @param flags optional flags for creating the Environment, combined with
   *      bitwise OR. Possible flags are:
   *    <ul>
   *     <li><code>Const.UPS_ENABLE_FSYNC</code></li>
   *      Immediately write modified pages to
   *      the disk. This slows down all Database operations, but may
   *      save the Database integrity in case of a system crash.
   *     <li><code>Const.UPS_IN_MEMORY</code></li>
   *      Creates an In-Memory Environment. No file will be created,
   *      and the Database contents are lost after the Database
   *      is closed. The <code>filename</code> parameter can
   *      be null. Do <b>NOT</b> specify a cache size other than 0.
   *     <li><code>Const.UPS_DISABLE_MMAP</code></li>
   *      Do not use memory mapped files for I/O. By default,
   *      upscaledb checks if it can use mmap, since mmap is
   *      faster than read/write. For performance reasons, this
   *      flag should not be used.
   *     <li><code>Const.UPS_CACHE_UNLIMITED</code></li>
   *     <li><code>Const.UPS_DISABLE_FREELIST_FLUSH</code></li>
   *      This flag is deprecated.
   *     <li><code>Const.UPS_LOCK_EXCLUSIVE</code></li>
   *      This flag is deprecated. This is the default behaviour.
   *      <code>Const.UPS_DISABLE_FREELIST_FLUSH</code> and
   *      <code>Const.UPS_ENABLE_FSYNC</code>.
   *     <li><code>Const.UPS_ENABLE_TRANSACTIONS</code></li>
   *    </ul>
   * @param mode File access rights for the new file. This is the
   *    <code>mode</code>
   *    parameter for creat(2). Ignored on Microsoft Windows.
   * @param params An array of <code>Parameter</code> structures.
   *    The following parameters are available:
   *    <ul>
   *    <li><code>Const.UPS_PARAM_CACHESIZE</code></li>
   *    <li><code>Const.UPS_PARAM_PAGESIZE</code></li>
   *    <li><code>Const.UPS_PARAM_MAX_DATABASES</code></li>
   *    </ul>
   * <p>
   * More information about flags, parameters and possible exceptions:
   * <a href="http://files.upscaledb.com/documentation/html/group__ups__env.html#ga30705470fbae862c47512a33564d72a8">C documentation</a>
   */
  public void create(String filename, int flags, int mode,
      Parameter[] params)
      throws DatabaseException {
    // make sure that the parameters don't have a NULL-element
    if (params != null) {
      for (int i = 0; i < params.length; i++)
        if (params[i] == null)
          throw new NullPointerException();
    }
    m_handle = ups_env_create(filename, flags, mode, params);
  }

  /**
   * Opens an existing Database Environment
   *
   * @see Environment#open(String, int, Parameter[])
   */
  public void open(String filename)
      throws DatabaseException {
    open(filename, 0, null);
  }

  /**
   * Opens an existing Database Environment
   *
   * @see Environment#open(String, int, Parameter[])
   */
  public void open(String filename, int flags)
      throws DatabaseException {
    open(filename, flags, null);
  }

  /**
   * Opens an existing Database Environment
   * <p>
   * This method wraps the native ups_env_open function.
   * <p>
   * @param filename the filename of the Environment file
   * @param flags optional flags for opening the Environment, combined with
   *      bitwise OR. Possible flags are:
   *    <ul>
   *     <li><code>Const.UPS_READ_ONLY</code></li>
   *      Opens the file for reading only. Operations which need
   *      write access (i.e. <code>ups_insert</code>) will return
   *      <code>Const.UPS_WRITE_PROTECTED</code>.
   *     <li><code>Const.UPS_ENABLE_FSYNC</code></li>
   *      Immediately write modified pages to
   *      the disk. This slows down all Database operations, but may
   *      save the Database integrity in case of a system crash.
   *     <li><code>Const.UPS_DISABLE_MMAP</code></li>
   *      Do not use memory mapped files for I/O. By default,
   *      upscaledb checks if it can use mmap, since mmap is
   *      faster than read/write. For performance reasons, this
   *      flag should not be used.
   *     <li><code>Const.UPS_CACHE_UNLIMITED</code></li>
   *     <li><code>Const.UPS_DISABLE_FREELIST_FLUSH</code></li>
   *      This flag is deprecated.
   *     <li><code>Const.UPS_LOCK_EXCLUSIVE</code></li>
   *      This flag is deprecated. This is the default behaviour.
   *     <li><code>Const.UPS_AUTO_RECOVERY</code></li>
   *      Automatically recover the Database, if necessary. This
   *      flag implies <code>Const.UPS_ENABLE_TRANSATIONS</code>.
   *     <li><code>Const.UPS_ENABLE_TRANSACTIONS</code> to
   *      enable Transactions</li>
   *    </ul>
   * @param params An array of <code>Parameter</code> structures.
   *    The following parameters are available:
   *    <ul>
   *    <li><code>Const.UPS_PARAM_CACHESIZE</code></li>
   *    </ul>
   * <p>
   * More information about flags, parameters and possible exceptions:
   * <a href="http://files.upscaledb.com/documentation/html/group__ups__env.html#ga3f347d5c9b11301c45c3f27624b5f55a">C documentation</a>
   */
  public void open(String filename, int flags,
      Parameter[] params)
      throws DatabaseException {
    // make sure that the parameters don't have a NULL-element
    if (params != null) {
      for (int i = 0; i < params.length; i++)
        if (params[i] == null)
          throw new NullPointerException();
    }
    m_handle = ups_env_open(filename, flags, params);
  }

  /**
   * Creates a new Database in this Database Environment
   *
   * @see Environment#createDatabase(short, int, Parameter[])
   */
  public Database createDatabase(short name)
      throws DatabaseException {
    return createDatabase(name, 0, null);
  }

  /**
   * Creates a new Database in this Database Environment
   *
   * @see Environment#createDatabase(short, int, Parameter[])
   */
  public Database createDatabase(short name, int flags)
      throws DatabaseException {
    return createDatabase(name, flags, null);
  }

  /**
   * Creates a new Database in this Database Environment
   * <p>
   * This method wraps the native ups_env_create_db function.
   *
   * See the documentation of ups_env_create_db (C API) for details about
   * supported key types, key sizes and record sizes.
   *
   * @param name the name of the Database. If a Database with this name
   *      already exists, the function will fail with
   *      <code>Const.UPS_DATABASE_ALREADY_EXISTS</code>.
   *      Database names from 0xf000 to 0xffff and 0 are reserved.
   * @param flags optional flags for creating the Database, combined with
   *      bitwise OR. Possible flags are:
   *    <ul>
   *     <li><code>Const.UPS_RECORD_NUMBER</code></li>
   *      Creates an "auto-increment" Database. Keys in Record
   *      Number Databases are automatically assigned an incrementing
   *      64bit value.
   *     <li><code>Const.UPS_ENABLE_DUPLICATE_KEYS</code></li>
   *      Enable duplicate keys for this Database. By default,
   *      duplicate keys are disabled.
   *    </ul>
   * @param params An array of <code>Parameter</code> structures.
   *    The following parameters are available:
   *    <ul>
   *    <li><code>Const.UPS_PARAM_KEYSIZE</code></li>
   *    </ul>
   * <p>
   * More information about flags, parameters and possible exceptions:
   * <a href="http://files.upscaledb.com/documentation/html/group__ups__env.html#ga38fcbdfa52fe22afe2aa3b3cc7395ea5">C documentation</a>
   *
   * @return a Database object
   */
  public Database createDatabase(short name, int flags,
      Parameter[] params)
      throws DatabaseException {
    // make sure that the parameters don't have a NULL-element
    if (params != null) {
      for (int i = 0; i < params.length; i++)
        if (params[i] == null)
          throw new NullPointerException();
    }
    // ups_env_create_db will throw an DatabaseException if it fails
    return new Database(ups_env_create_db(m_handle, name, flags, params));
  }

  /**
   * Opens a Database in a Database Environment
   *
   * @see Environment#openDatabase(short, int, Parameter[])
   */
  public Database openDatabase(short name)
      throws DatabaseException {
    return openDatabase(name, 0, null);
  }

  /**
   * Opens a Database in a Database Environment
   *
   * @see Environment#openDatabase(short, int, Parameter[])
   */
  public Database openDatabase(short name, int flags)
      throws DatabaseException {
    return openDatabase(name, flags, null);
  }

  /**
   * Opens a Database in a Database Environment
   * <p>
   * This method wraps the native ups_env_open_db function.
   *
   * @param name The name of the Database. If a Database with this name
   *        does not exist, the function will throw
   *        <code>Const.UPS_DATABASE_NOT_FOUND</code>.
   * @param flags optional flags for opening the Database, combined with
   *      bitwise OR. Possible flags are:
   *    <ul>
   *     <li><code>Const.UPS_READ_ONLY</code> opens the database for
   *        reading only</li>
   *    </ul>
   * <p>
   * More information about flags, parameters and possible exceptions:
   * <a href="http://files.upscaledb.com/documentation/html/group__ups__env.html#ga2fc332.2.1b4feba84c2e9761c6443a5">C documentation</a>
   *
   * @return a Database object
   */
  public Database openDatabase(short name, int flags,
          Parameter[] params)
      throws DatabaseException {
    // ups_env_open_db will throw an DatabaseException if it fails
    return new Database(ups_env_open_db(m_handle, name, flags, params));
  }

  /**
   * Renames a Database in this Environment
   * <p>
   * This method wraps the native ups_env_rename_db function.
   *
   * @param oldname The old name of the existing Database. If a Database
   *        with this name does not exist, the function will fail with
   *        <code>Const.UPS_DATABASE_NOT_FOUND</code>.
   * @param newname The new name of this Database. If a Database
   *        with this name already exists, the function will fail
   *        with <code>Const.UPS_DATABASE_ALREADY_EXISTS</code>.
   *
   * <p>
   * More information: <a href="http://files.upscaledb.com/documentation/html/group__ups__env.html#gaeff4a7a4c1363bef3c7f0a178ad8fd5b">C documentation</a>
   */
  public void renameDatabase(short oldname, short newname)
      throws DatabaseException {
    int status = ups_env_rename_db(m_handle, oldname, newname, 0);
    if (status != 0)
      throw new DatabaseException(status);
  }

  /**
   * Deletes a Database from this Environment
   * <p>
   * This method wraps the native ups_env_erase_db function.
   *
   * @param name the name of the Database, which is deleted. If a Database
   *        with this name does not exist, the function will fail with
   *        <code>Const.UPS_DATABASE_NOT_FOUND</code>.
   *
   * <p>
   * More information: <a href="http://files.upscaledb.com/documentation/html/group__ups__env.html#gae5ddd4e578a39dc1927d8a6465bcac2a">C documentation</a>
   */
  public void eraseDatabase(short name)
    throws DatabaseException {
      int status = ups_env_erase_db(m_handle, name, 0);
      if (status != 0)
        throw new DatabaseException(status);
  }

  /**
   * Returns the names of all Databases in this Environment
   * <p>
   * This method wraps the native ups_env_get_database_names function.
   * <p>
   * This function returns the names of all Databases and the number of
   * Databases in an Environment.
   * <p>
   * More information: <a href="http://files.upscaledb.com/documentation/html/group__ups__env.html#ga70ee2ba96afc341acd72a3e7a11090ab">C documentation</a>
   *
   * @return an array with all Database names of this Environment
   */
  public short[] getDatabaseNames()
      throws DatabaseException {
    /* the native library throws an exception, if necessary */
    return ups_env_get_database_names(m_handle);
  }

  /**
   * Retrieve the current value for a given Environment setting
   *
   * <p>
   * More information: <a href="http://files.upscaledb.com/documentation/html/group__ups__env.html#ga314c78f6e23b79f23ed7167dd9dc4b7a">C documentation</a>
   */
  public void getParameters(Parameter[] params)
      throws DatabaseException {
    int status = ups_env_get_parameters(m_handle, params);
    if (status != 0)
      throw new DatabaseException(status);
  }

  /**
   * Flush this Environment
   *
   * This function flushes the Environment caches and writes the whole file
   * to disk. All Databases of this Environment are flushed as well.
   * <p>
   * Since In-Memory Databases do not have a file on disk, the function will
   * have no effect and will return UPS_SUCCESS.
   * <p>
   * More information: <a href="http://files.upscaledb.com/documentation/html/group__ups__env.html#ga394c0b85da29e3eaca719b67504f755f">C documentation</a>
   */
  public void flush()
      throws DatabaseException {
    int status = ups_env_flush(m_handle);
    if (status != 0)
      throw new DatabaseException(status);
  }

  /**
   * Begins a new Transaction
   * <p>
   * This method wraps the native ups_txn_begin function.
   * <p>
   * More information: <a href="http://files.upscaledb.com/documentation/html/group__ups__txn.html#gac765d57ce869a6b2a89266fbc20f965e">C documentation</a>
   *
   * @param flags flags for beginning the Transaction
   */
  public Transaction begin(int flags)
      throws DatabaseException {
    return new Transaction(this, ups_txn_begin(m_handle, flags));
  }

  /**
   * Begins a new Transaction
   *
   * @see Environment#begin(int)
   */
  public Transaction begin()
      throws DatabaseException {
    return begin(0);
  }

  /**
   * Performs a "UQI Select" query.
   *
   * @see Environment#selectRange(String, Cursor, Cursor)
   */
  public Result select(String query)
      throws DatabaseException {
    return selectRange(query, null, null);
  }

  /**
   * Performs a paginated "UQI Select" query.
   * This function is similar to Environment::select(), but uses two cursors for
   * specifying the range of the data. Make sure that both cursors are
   * operating on the same database as the query. Both cursors are optional;
   * if `begin' is null then the query will start at the first element (with
   * the lowest key) in the database. If `end' is null then it will run till
   * the last element of the database.
   *
   * If `begin' is not null then it will be moved to the first key behind the
   * processed range.
   *
   * If the specified Database is not yet opened, it will be reopened in
   * background and immediately closed again after the query. Closing the
   * Database can hurt performance. To avoid this, manually open the
   * Database prior to the query. This method will then re-use the existing
   * Database handle.
   */
  public Result selectRange(String query, Cursor begin, Cursor end)
      throws DatabaseException {
    return new Result(ups_env_select_range(m_handle, query,
                                    begin != null ? begin.getHandle() : 0l,
                                    end != null ? end.getHandle() : 0l));
  }

  /**
   * Closes this Database Environment
   * <p>
   * This method wraps the native ups_env_close function.
   * <p>
   * The application has to close all Databases prior to closing the
   * Environment.
   * <p>
   * More information: <a href="http://files.upscaledb.com/documentation/html/group__ups__env.html#gae50d3ad9ebd3f098bdf755b300fa3496">C documentation</a>
   */
  public void close() {
    if (m_handle != 0)
      ups_env_close(m_handle, 0);
    m_handle = 0;
  }

  static {
    System.loadLibrary("upscaledb-java");
  }

  private long m_handle;
}
