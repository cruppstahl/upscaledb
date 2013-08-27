/**
 * Copyright (C) 2005-2013 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See file COPYING.GPL2 and COPYING.GPL3 for License information.
 *
 */

package de.crupp.hamsterdb;

import de.crupp.hamsterdb.Transaction;

public class Environment {

  private native long ham_env_create(String filename, int flags, int mode,
      Parameter[] params);

  private native long ham_env_open(String filename, int flags,
      Parameter[] params);

  private native long ham_env_create_db(long handle, short name, int flags,
      Parameter[] params);

  private native long ham_env_open_db(long handle, short name, int flags,
      Parameter[] params);

  private native int ham_env_get_parameters(long handle, Parameter[] params);

  private native int ham_env_rename_db(long handle, short oldname,
      short newname, int flags);

  private native int ham_env_erase_db(long handle, short name, int flags);

  private native short[] ham_env_get_database_names(long handle);

  private native int ham_env_flush(long handle);

  private native long ham_txn_begin(long handle, int flags);

  private native int ham_env_close(long handle, int flags);

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
   * This method wraps the native ham_env_create function.
   * <p>
   * @param filename the filename of the Environment file. If the file
   *      already exists, it is overwritten. Can be null for an In-Memory
   *      Environment.
   * @param flags optional flags for creating the Environment, combined with
   *      bitwise OR. Possible flags are:
   *    <ul>
   *     <li><code>Const.HAM_ENABLE_FSYNC</code></li>
   *      Immediately write modified pages to
   *      the disk. This slows down all Database operations, but may
   *      save the Database integrity in case of a system crash.
   *     <li><code>Const.HAM_IN_MEMORY</code></li>
   *      Creates an In-Memory Environment. No file will be created,
   *      and the Database contents are lost after the Database
   *      is closed. The <code>filename</code> parameter can
   *      be null. Do <b>NOT</b> use in combination with
   *      <code>Const.HAM_CACHE_STRICT</code> and do <b>NOT</b>
   *      specify a cache size other than 0.
   *     <li><code>Const.HAM_DISABLE_MMAP</code></li>
   *      Do not use memory mapped files for I/O. By default,
   *      hamsterdb checks if it can use mmap, since mmap is
   *      faster than read/write. For performance reasons, this
   *      flag should not be used.
   *     <li><code>Const.HAM_CACHE_STRICT</code></li>
   *      Do not allow the cache to grow larger than the size specified
   *      with <code>Const.HAM_PARAM_CACHESIZE</code>. If a
   *      Database operation needs to resize the cache, it will
   *      fail and return <code>Const.HAM_CACHE_FULL</code>.
   *      If the flag is not set, the cache is allowed to allocate
   *      more pages than the maximum cache size, but only if it's
   *      necessary and only for a short time.
   *     <li><code>Const.HAM_CACHE_UNLIMITED</code></li>
   *     <li><code>Const.HAM_DISABLE_FREELIST_FLUSH</code></li>
   *      This flag is deprecated.
   *     <li><code>Const.HAM_LOCK_EXCLUSIVE</code></li>
   *      This flag is deprecated. This is the default behaviour.
   *     <li><code>Const.HAM_ENABLE_RECOVERY</code></li>
   *      Enables logging/recovery for this Database. Not allowed in
   *      combination with <code>Const.HAM_IN_MEMORY_DB</code>,
   *      <code>Const.HAM_DISABLE_FREELIST_FLUSH</code> and
   *      <code>Const.HAM_ENABLE_FSYNC</code>.
   *     <li><code>Const.HAM_ENABLE_TRANSACTIONS</code></li>
   *    </ul>
   * @param mode File access rights for the new file. This is the
   *    <code>mode</code>
   *    parameter for creat(2). Ignored on Microsoft Windows.
   * @param params An array of <code>Parameter</code> structures.
   *    The following parameters are available:
   *    <ul>
   *    <li><code>Const.HAM_PARAM_CACHESIZE</code></li>
   *    <li><code>Const.HAM_PARAM_PAGESIZE</code></li>
   *    <li><code>Const.HAM_PARAM_MAX_DATABASES</code></li>
   *    </ul>
   * <p>
   * More information about flags, parameters and possible exceptions:
   * <a href="http://hamsterdb.com/public/scripts/html_www/group__ham__env.html#ga4316524c094e12d84f8be7456d2101a8">C documentation</a>
   */
  public synchronized void create(String filename, int flags, int mode,
      Parameter[] params)
      throws DatabaseException {
    // make sure that the parameters don't have a NULL-element
    if (params != null) {
      for (int i = 0; i < params.length; i++)
        if (params[i] == null)
          throw new NullPointerException();
    }
    m_handle = ham_env_create(filename, flags, mode, params);
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
   * This method wraps the native ham_env_open function.
   * <p>
   * @param filename the filename of the Environment file
   * @param flags optional flags for opening the Environment, combined with
   *      bitwise OR. Possible flags are:
   *    <ul>
   *     <li><code>Const.HAM_READ_ONLY</code></li>
   *      Opens the file for reading only. Operations which need
   *      write access (i.e. <code>ham_insert</code>) will return
   *      <code>Const.HAM_WRITE_PROTECTED</code>.
   *     <li><code>Const.HAM_ENABLE_FSYNC</code></li>
   *      Immediately write modified pages to
   *      the disk. This slows down all Database operations, but may
   *      save the Database integrity in case of a system crash.
   *     <li><code>Const.HAM_DISABLE_MMAP</code></li>
   *      Do not use memory mapped files for I/O. By default,
   *      hamsterdb checks if it can use mmap, since mmap is
   *      faster than read/write. For performance reasons, this
   *      flag should not be used.
   *     <li><code>Const.HAM_CACHE_STRICT</code></li>
   *      Do not allow the cache to grow larger than the size specified
   *      with <code>Const.HAM_PARAM_CACHESIZE</code>. If a
   *      Database operation needs to resize the cache, it will
   *      fail and return <code>Const.HAM_CACHE_FULL</code>.
   *      If the flag is not set, the cache is allowed to allocate
   *      more pages than the maximum cache size, but only if it's
   *      necessary and only for a short time.
   *     <li><code>Const.HAM_CACHE_UNLIMITED</code></li>
   *     <li><code>Const.HAM_DISABLE_FREELIST_FLUSH</code></li>
   *      This flag is deprecated.
   *     <li><code>Const.HAM_LOCK_EXCLUSIVE</code></li>
   *      This flag is deprecated. This is the default behaviour.
   *     <li><code>Const.HAM_ENABLE_RECOVERY</code></li>
   *      Enables logging/recovery for this Database. Will return
   *      <code>Const.HAM_NEED_RECOVERY</code>, if the Database
   *      is in an inconsistent state. Not allowed in combination
   *      with <code>Const.HAM_DISABLE_FREELIST_FLUSH</code> and
   *      <code>Const.HAM_ENABLE_FSYNC</code>.
   *     <li><code>Const.HAM_AUTO_RECOVERY</code></li>
   *      Automatically recover the Database, if necessary. This
   *      flag implies <code>Const.HAM_ENABLE_RECOVERY</code>.
   *     <li><code>Const.HAM_ENABLE_TRANSACTIONS</code></li>
   *    </ul>
   * @param params An array of <code>Parameter</code> structures.
   *    The following parameters are available:
   *    <ul>
   *    <li><code>Const.HAM_PARAM_CACHESIZE</code></li>
   *    </ul>
   * <p>
   * More information about flags, parameters and possible exceptions:
   * <a href="http://hamsterdb.com/public/scripts/html_www/group__ham__env.html#ga4dbe3a84324f142a7c2344c33314d9b5">C documentation</a>
   */
  public synchronized void open(String filename, int flags,
      Parameter[] params)
      throws DatabaseException {
    // make sure that the parameters don't have a NULL-element
    if (params != null) {
      for (int i = 0; i < params.length; i++)
        if (params[i] == null)
          throw new NullPointerException();
    }
    m_handle = ham_env_open(filename, flags, params);
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
   * This method wraps the native ham_env_create_db function.
   *
   * @param name the name of the Database. If a Database with this name
   *      already exists, the function will fail with
   *      <code>Const.HAM_DATABASE_ALREADY_EXISTS</code>.
   *      Database names from 0xf000 to 0xffff and 0 are reserved.
   * @param flags optional flags for creating the Database, combined with
   *      bitwise OR. Possible flags are:
   *    <ul>
   *     <li><code>Const.HAM_DISABLE_VAR_KEYLEN</const></li>
   *      Do not allow the use of variable length keys.
   *      Inserting a key, which is larger than the B+Tree index
   *      key size, returns <code>Const.HAM_INV_KEYSIZE</code>.
   *     <li><code>Const.HAM_RECORD_NUMBER</code></li>
   *      Creates an "auto-increment" Database. Keys in Record
   *      Number Databases are automatically assigned an incrementing
   *      64bit value.
   *     <li><code>Const.HAM_ENABLE_DUPLICATE_KEYS</code></li>
   *      Enable duplicate keys for this Database. By default,
   *      duplicate keys are disabled.
   *     <li><code>Const.HAM_ENABLE_EXTENDED_KEYS</code></li>
   *      Enable extended keys for this Database. By default,
   *      extended keys are disabled.
   *    </ul>
   * @param params An array of <code>Parameter</code> structures.
   *    The following parameters are available:
   *    <ul>
   *    <li><code>Const.HAM_PARAM_KEYSIZE</code></li>
   *    </ul>
   * <p>
   * More information about flags, parameters and possible exceptions:
   * <a href="http://hamsterdb.com/public/scripts/html_www/group__ham__env.html#ga5934b6a7c9457afd0ff2c19dd9c6db15">C documentation</a>
   *
   * @return a Database object
   */
  public synchronized Database createDatabase(short name, int flags,
      Parameter[] params)
      throws DatabaseException {
    // make sure that the parameters don't have a NULL-element
    if (params != null) {
      for (int i = 0; i < params.length; i++)
        if (params[i] == null)
          throw new NullPointerException();
    }
    // ham_env_create_db will throw an DatabaseException if it fails
    return new Database(ham_env_create_db(m_handle, name, flags, params));
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
   * This method wraps the native ham_env_open_db function.
   *
   * @param name The name of the Database. If a Database with this name
   *        does not exist, the function will throw
   *        <code>Const.HAM_DATABASE_NOT_FOUND</code>.
   * @param flags optional flags for opening the Database, combined with
   *      bitwise OR. Possible flags are:
   *    <ul>
   *     <li><code>Const.HAM_DISABLE_VAR_KEYLEN</code></li>
   *      Do not allow the use of variable length keys. Inserting
   *      a key, which is larger than the B+Tree index key size,
   *      returns <code>Const.HAM_INV_KEYSIZE</code>.
   *    </ul>
   * <p>
   * More information about flags, parameters and possible exceptions:
   * <a href="http://hamsterdb.com/public/scripts/html_www/group__ham__env.html#ga5fc90a1a4c2e4a69d737c804b5159931">C documentation</a>
   *
   * @return a Database object
   */
  public synchronized Database openDatabase(short name, int flags,
          Parameter[] params)
      throws DatabaseException {
    // ham_env_open_db will throw an DatabaseException if it fails
    return new Database(ham_env_open_db(m_handle, name, flags, params));
  }

  /**
   * Renames a Database in this Environment
   * <p>
   * This method wraps the native ham_env_rename_db function.
   *
   * @param oldname The old name of the existing Database. If a Database
   *        with this name does not exist, the function will fail with
   *        <code>Const.HAM_DATABASE_NOT_FOUND</code>.
   * @param newname The new name of this Database. If a Database
   *        with this name already exists, the function will fail
   *        with <code>Const.HAM_DATABASE_ALREADY_EXISTS</code>.
   *
   * <p>
   * More information: <a href="http://hamsterdb.com/public/scripts/html_www/group__ham__env.html#ga07a3749e3dfa88138c3056e5052bb96a">C documentation</a>
   */
  public synchronized void renameDatabase(short oldname, short newname)
      throws DatabaseException {
    int status = ham_env_rename_db(m_handle, oldname, newname, 0);
    if (status != 0)
      throw new DatabaseException(status);
  }

  /**
   * Deletes a Database from this Environment
   * <p>
   * This method wraps the native ham_env_erase_db function.
   *
   * @param name the name of the Database, which is deleted. If a Database
   *        with this name does not exist, the function will fail with
   *        <code>Const.HAM_DATABASE_NOT_FOUND</code>.
   *
   * <p>
   * More information: <a href="http://hamsterdb.com/public/scripts/html_www/group__ham__env.html#ga4ceb71003291e9eabe2df7140c89610c">C documentation</a>
   */
  public synchronized void eraseDatabase(short name)
    throws DatabaseException {
      int status = ham_env_erase_db(m_handle, name, 0);
      if (status != 0)
        throw new DatabaseException(status);
  }

  /**
   * Returns the names of all Databases in this Environment
   * <p>
   * This method wraps the native ham_env_get_database_names function.
   * <p>
   * This function returns the names of all Databases and the number of
   * Databases in an Environment.
   * <p>
   * More information: <a href="http://hamsterdb.com/public/scripts/html_www/group__ham__env.html#ga8b6e5d0611cb6aba8607ff0824441e8d">C documentation</a>
   *
   * @return an array with all Database names of this Environment
   */
  public synchronized short[] getDatabaseNames()
      throws DatabaseException {
    /* the native library throws an exception, if necessary */
    return ham_env_get_database_names(m_handle);
  }

  /**
   * Retrieve the current value for a given Environment setting
   *
   * <p>
   * More information: <a href="http://hamsterdb.com/public/scripts/html_www/group__ham__env.html#ga1da6fd9eee42c7d0c6e4a23dd7e5c059">C documentation</a>
   */
  public synchronized void getParameters(Parameter[] params)
      throws DatabaseException {
    int status = ham_env_get_parameters(m_handle, params);
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
   * have no effect and will return HAM_SUCCESS.
   * <p>
   * More information: <a href="http://hamsterdb.com/public/scripts/html_www/group__ham__env.html#ga45a4d0ce402d5ed49006da470b31baf6">C documentation</a>
   */
  public synchronized void flush()
      throws DatabaseException {
    int status = ham_env_flush(m_handle);
    if (status != 0)
      throw new DatabaseException(status);
  }

  /**
   * Begins a new Transaction
   * <p>
   * This method wraps the native ham_txn_begin function.
   * <p>
   * More information: <a href="http://hamsterdb.com/public/scripts/html_www/group__ham__txn.html#ga680a26a4ed8fea77a8cafc53d2850055">C documentation</a>
   *
   * @param flags flags for beginning the Transaction
   */
  public synchronized Transaction begin(int flags)
      throws DatabaseException {
    return new Transaction(this, ham_txn_begin(m_handle, flags));
  }

  /**
   * Begins a new Transaction
   *
   * @see Database#begin(int)
   */
  public synchronized Transaction begin()
      throws DatabaseException {
    return begin(0);
  }

  /**
   * Closes this Database Environment
   * <p>
   * This method wraps the native ham_env_close function.
   * <p>
   * The application has to close all Databases prior to closing the
   * Environment.
   * <p>
   * More information: <a href="http://hamsterdb.com/public/scripts/html_www/group__ham__env.html#gaec7ee5ca832fa06438b4806ae5e096c4">C documentation</a>
   */
  public synchronized void close() {
    if (m_handle != 0)
      ham_env_close(m_handle, 0);
    m_handle = 0;
  }

  static {
    System.loadLibrary("hamsterdb-java");
  }

  private long m_handle;
}
