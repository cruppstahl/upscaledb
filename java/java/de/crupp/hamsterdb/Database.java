/**
 * Copyright (C) 2005-2012 Christoph Rupp (chris@crupp.de).
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

import java.util.HashSet;
import java.util.Iterator;

public class Database {

    private native static int ham_get_version(int which);

    private native static String ham_get_license(int which);

    private native static void ham_set_errhandler(ErrorHandler eh);

    private native long ham_new();

    private native void ham_delete(long handle);

    private native int ham_create_ex(long handle, String filename, int flags,
            int mode, Parameter[] params);

    private native int ham_open_ex(long handle, String filename, int flags,
            Parameter[] params);

    private native int ham_get_error(long handle);

    private native void ham_set_compare_func(long handle,
            CompareCallback cmp);

    private native void ham_set_prefix_compare_func(long handle,
            PrefixCompareCallback cmp);

    private native Environment ham_get_env(long handle);

    private native byte[] ham_find(long handle, long txnhandle,
            byte[] key, int flags);

    private native int ham_flush(long handle, int flags);

    private native int ham_get_parameters(long handle, Parameter[] params);

    private native int ham_insert(long handle, long txnhandle,
            byte[] key, byte[] record, int flags);

    private native int ham_erase(long handle, long txnhandle,
            byte[] key, int flags);

    private native long ham_get_key_count(long handle, long txnhandle,
            int flags);

    private native int ham_close(long handle, int flags);

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
     *          is emitted; use null to set the default error handler.
     */
    public static void setErrorHandler(ErrorHandler eh) {
        // set a reference to eh to avoid that it's garbage collected
        m_eh=eh;
        ham_set_errhandler(eh);
    }

    /**
     * Returns the version of the hamsterdb library.
     * <p>
     * This method wraps the native ham_get_version function.
     * <p>
     * More information: <a href="http://hamsterdb.com/public/scripts/html_www/group__ham__static.html#gafdbeaa3c3be6812d1b5d5470f7e984ca">C documentation</a>
     *
     * @return the hamsterdb version tuple
     */
    public static Version getVersion() {
        Version v=new Version();
        v.major=ham_get_version(0);
        v.minor=ham_get_version(1);
        v.revision=ham_get_version(2);
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
        License l=new License();
        l.licensee=ham_get_license(0);
        l.product=ham_get_license(1);
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
        m_handle=handle;
    }

    /**
     * Destructor - closes the Database, if it is still open.
     */
    protected void finalize()
            throws DatabaseException {
        close();
    }

    /**
     * Creates a Database
     *
     * @see Database#create(String, int, int, Parameter[])
     */
    public void create(String filename)
            throws DatabaseException {
        create(filename, 0, 0644, null);
    }

    /**
     * Creates a Database
     *
     * @see Database#create(String, int, int, Parameter[])
     */
    public void create(String filename, int flags)
            throws DatabaseException {
        create(filename, flags, 0644, null);
    }

    /**
     * Creates a Database
     *
     * @see Database#create(String, int, int, Parameter[])
     */
    public void create(String filename, int flags, int mode)
            throws DatabaseException {
        create(filename, flags, mode, null);
    }

    /**
     * Creates a Database
     * <p>
     * This method wraps the native ham_create_ex function.
     * <p>
     * @param filename The filename of the Database file. If the file already
     *          exists, it is overwritten. Can be null if you create an
     *          In-Memory Database
     * @param flags Optional flags for this operation, combined with
     *        bitwise OR. Possible flags are:
     *      <ul>
     *       <li><code>Const.HAM_WRITE_THROUGH</code></li>
     *            Immediately write modified pages to
     *            the disk. This slows down all Database operations, but may
     *            save the Database integrity in case of a system crash.
     *       <li><code>Const.HAM_USE_BTREE</code></li>
     *            Use a B+Tree for the index structure. Currently enabled
     *            by default, but future releases of hamsterdb will
     *            offer additional index structures, i.e. hash tables.
     *       <li><code>Const.HAM_DISABLE_VAR_KEYLEN</const></li>
     *            Do not allow the use of variable length keys.
     *            Inserting a key, which is larger than the B+Tree index
     *            key size, returns <code>Const.HAM_INV_KEYSIZE</code>.
     *       <li><code>Const.HAM_IN_MEMORY_DB</code></li>
     *            Creates an In-Memory Database. No file will be created,
     *            and the Database contents are lost after the Database
     *            is closed. The <code>filename</code> parameter can
     *            be null. Do <b>NOT</b> use in combination with
     *            <code>Const.HAM_CACHE_STRICT</code> and do <b>NOT</b>
     *            specify a cache size other than 0.
     *       <li><code>Const.HAM_RECORD_NUMBER</code></li>
     *            Creates an "auto-increment" Database. Keys in Record
     *            Number Databases are automatically assigned an incrementing
     *            64bit value.
     *       <li><code>Const.HAM_ENABLE_DUPLICATES</code></li>
     *            Enable duplicate keys for this Database. By default,
     *            duplicate keys are disabled.
     *       <li><code>Const.HAM_DISABLE_MMAP</code></li>
     *            Do not use memory mapped files for I/O. By default,
     *            hamsterdb checks if it can use mmap, since mmap is
     *            faster than read/write. For performance reasons, this
     *            flag should not be used.
     *       <li><code>Const.HAM_CACHE_STRICT</code></li>
     *            Do not allow the cache to grow larger than the size specified
     *            with <code>Const.HAM_PARAM_CACHESIZE</code>. If a
     *            Database operation needs to resize the cache, it will
     *            fail and return <code>Const.HAM_CACHE_FULL</code>.
     *            If the flag is not set, the cache is allowed to allocate
     *            more pages than the maximum cache size, but only if it's
     *            necessary and only for a short time.
     *       <li><code>Const.HAM_CACHE_UNLIMITED</code></li>
     *       <li><code>Const.HAM_DISABLE_FREELIST_FLUSH</code></li>
     *            This flag is deprecated.
     *       <li><code>Const.HAM_LOCK_EXCLUSIVE</code></li>
     *            This flag is deprecated.
     *       <li><code>Const.HAM_ENABLE_RECOVERY</code></li>
     *            Enables logging/recovery for this Database. Not allowed in
     *            combination with <code>Const.HAM_IN_MEMORY_DB</code>,
     *            <code>Const.HAM_DISABLE_FREELIST_FLUSH</code> and
     *            <code>Const.HAM_WRITE_THROUGH</code>.
     *       <li><code>Const.HAM_ENABLE_TRANSACTIONS</code></li>
     *            Enables Transactions for this Database.
     *      </ul>
     *
     * @param mode File access rights for the new file. This is the
     *        <code>mode</code>
     *        parameter for creat(2). Ignored on Microsoft Windows.
     * @param params An array of <code>Parameter</code> structures.
     *        The following parameters are available:
     *      <ul>
     *        <li><code>Const.HAM_DEFAULT_CACHESIZE</code></li>
     *        <li><code>Const.HAM_PARAM_PAGESIZE</code></li>
     *        <li><code>Const.HAM_PARAM_KEYSIZE</code></li>
     *        <li><code>Const.HAM_PARAM_DATA_ACCESS_MODE</code></li>
     *      </ul>
     * <p>
     * More information about flags, parameters and possible exceptions:
     * <a href="http://hamsterdb.com/public/scripts/html_www/group__ham__database.html#ga24245a31681e5987a0d71c04ff953ea3">C documentation</a>
     */
    public synchronized void create(String filename, int flags, int mode,
            Parameter[] params) throws DatabaseException {
        // make sure that the parameters don't have a NULL-element
        if (params!=null) {
            for (int i=0; i<params.length; i++)
                if (params[i]==null)
                    throw new NullPointerException();
        }
        if (m_handle==0)
            m_handle=ham_new();
        int status=ham_create_ex(m_handle, filename, flags, mode, params);
        if (status!=0)
            throw new DatabaseException(status);
    }

    /**
     * Opens an existing Database
     *
     * @see Database#open(String, int, Parameter[])
     */
    public void open(String filename)
            throws DatabaseException {
        open(filename, 0, null);
    }

    /**
     * Opens an existing Database
     *
     * @see Database#open(String, int, Parameter[])
     */
    public void open(String filename, int flags)
            throws DatabaseException {
        open(filename, flags, null);
    }

    /**
     * Opens an existing Database
     * <p>
     * This method wraps the native ham_open_ex function.
     * <p>
     * @param filename The filename of the Database file
     * @param flags Optional flags for opening the Database, combined with
     *        bitwise OR. Possible flags are:
     *      <ul>
     *       <li><code>Const.HAM_READ_ONLY</code></li>
     *            Opens the file for reading only. Operations which need
     *            write access (i.e. <code>Database.insert</code>) will return
     *            <code>Const.HAM_DB_READ_ONLY</code>.
     *       <li><code>Const.HAM_WRITE_THROUGH</code></li>
     *            Immediately write modified pages to the disk.
     *            This slows down all Database operations, but could save
     *            the Database integrity in case of a system crash.
     *       <li><code>Const.HAM_DISABLE_VAR_KEYLEN</code></li>
     *            Do not allow the use of variable length keys. Inserting
     *            a key, which is larger than the B+Tree index key size,
     *            returns <code>Const.HAM_INV_KEYSIZE</code>.
     *       <li><code>Const.HAM_DISABLE_MMAP</code></li>
     *            Do not use memory mapped files for I/O. By default,
     *            hamsterdb checks if it can use mmap, since mmap is
     *            faster than read/write. For performance reasons, this
     *            flag should not be used.
     *       <li><code>Const.HAM_CACHE_STRICT</code></li>
     *            Do not allow the cache to grow larger than the cache size.
     *            If a Database operation needs to resize the cache,
     *            it will fail and return <code>Const.HAM_CACHE_FULL</code>.
     *            If the flag is not set, the cache is allowed to allocate
     *            more pages than the maximum cache size, but only if it's
     *            necessary and only for a short time.
     *       <li><code>Const.HAM_CACHE_UNLIMITED</code></li>
     *       <li><code>Const.HAM_DISABLE_FREELIST_FLUSH</code></li>
     *            This flag is deprecated.
     *       <li><code>Const.HAM_LOCK_EXCLUSIVE</code></li>
     *            This is enabled by default.
     *       <li><code>Const.HAM_ENABLE_RECOVERY</code></li>
     *            Enables logging/recovery for this Database. Will return
     *            <code>Const.HAM_NEED_RECOVERY</code>, if the Database
     *            is in an inconsistent state. Not allowed in combination
     *            with <code>Const.HAM_DISABLE_FREELIST_FLUSH</code> and
     *            <code>Const.HAM_WRITE_THROUGH</code>.
     *       <li><code>Const.HAM_AUTO_RECOVERY</code></li>
     *            Automatically recover the Database, if necessary. This
     *            flag implies <code>Const.HAM_ENABLE_RECOVERY</code>.
     *       <li><code>Const.HAM_ENABLE_DUPLICATES</code></li>
     *       <li><code>Const.HAM_ENABLE_TRANSACTIONS</code></li>
     *      </ul>
     * @param params An array of <code>Parameter</code> structures. The
     *        following parameters are available:
     *      <ul>
     *        <li><code>Const.HAM_PARAM_CACHESIZE</code></li>
     *        <li><code>Const.HAM_PARAM_DATA_ACCESS_MODE</code></li>
     *      </ul>
     * <p>
     * More information about flags, parameters and possible exceptions:
     * <a href="http://hamsterdb.com/public/scripts/html_www/group__ham__database.html#ga7b1ae31472d71ae215249295457d23f9">C documentation</a>
     */
    public synchronized void open(String filename, int flags,
            Parameter[] params) throws DatabaseException {
        // make sure that the parameters don't have a NULL-element
        if (params!=null) {
            for (int i=0; i<params.length; i++)
                if (params[i]==null)
                    throw new NullPointerException();
        }
        if (m_handle==0)
            m_handle=ham_new();
        int status=ham_open_ex(m_handle, filename, flags, params);
        if (status!=0)
            throw new DatabaseException(status);
    }

    /**
     * Returns the last error code
     * <p>
     * This method wraps the native ham_get_error function.
     * <p>
     * More information: <a href="http://hamsterdb.com/public/scripts/html_www/group__ham__Database__cfg__parameters.html#gad7d007973f398906a822a5f58f22d801">C documentation</a>
     *
     * @return the error code of the last operation
     */
    public synchronized int getError() {
        return ham_get_error(m_handle);
    }

    /**
     * Sets the comparison function
     * <p>
     * This method wraps the native ham_set_compare_func function.
     * <p>
     * The <code>CompareCallback.compare</code> method compares two index
     * keys. It returns -1 if the first key is smaller, +1 if the second
     * key is smaller or 0 if both keys are equal.
     * <p>
     * If <code>cmp</code> is null, hamsterdb will use the default compare
     * function (which is based on memcmp(3)).
     * <p>
     * Note that if you use a custom comparison routine in combination with
     * extended keys, it might be useful to disable the prefix comparison,
     * which is based on memcmp(3). See <code>setPrefixComparator</code> for
     * details.
     * <p>
     * More information: <a href="http://hamsterdb.com/public/scripts/html_www/group__ham__Database__cfg__parameters.html#ga0fa5d7a6c42e161d07075cbfa157834d">C documentation</a>
     * <p>
     * @param cmp an object implementing the CompareCallback interface, or null
     * <p>
     * @see Database#setPrefixComparator
     */
    public synchronized void setComparator(CompareCallback cmp) {
        m_cmp=cmp;
        ham_set_compare_func(m_handle, cmp);
    }

    /**
     * Sets the prefix comparison function
     * <p>
     * This method wraps the native ham_set_prefix_compare_func function.
     * <p>
     * The prefix comparison function is called when an index uses
     * keys with variable length, and at least one of the two keys is loaded
     * only partially.
     * <p>
     * If <code>cmp</code> is null, hamsterdb will use the default prefix
     * compare function (which is based on memcmp(3)).
     * <p>
     * More information: <a href="http://hamsterdb.com/public/scripts/html_www/group__ham__Database__cfg__parameters.html#ga6d2b05b0c8581a75ae2d45b52b2f04d2">C documentation</a>
     * <p>
     * @param cmp an object implementing the PrefixCompareCallback interface,
     *          or null
     * <p>
     * @see Database#setComparator
     */
    public synchronized void setPrefixComparator(PrefixCompareCallback cmp) {
        m_prefix_cmp=cmp;
        ham_set_prefix_compare_func(m_handle, cmp);
    }

    /**
     * Searches an item in the Database, returns the record
     * <p>
     * This method wraps the native ham_find function.
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
        if (key==null)
            throw new NullPointerException();
        byte[] r;
        r=ham_find(m_handle, txn!=null ? txn.getHandle() : 0, key, 0);
        if (r==null)
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
     * This method wraps the native ham_insert function.
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
     * be created with <code>Const.HAM_ENABLE_DUPLICATES</code> in order
     * to use duplicate keys.) <br>
     * The duplicate key is inserted after all other duplicate keys (see
     * <code>Const.HAM_DUPLICATE_INSERT_LAST</code>).
     * <p>
     * @param txn the (optional) Transaction
     * @param key the key of the new item
     * @param record the record of the new item
     * @param flags optional flags for inserting. Possible flags are:
     *      <ul>
     *        <li><code>Const.HAM_OVERWRITE</code>. If the key already
     *          exists, the record is overwritten. Otherwise, the key is
     *          inserted.
     *        <li><code>Const.HAM_DUPLICATE</code>. If the key already
     *          exists, a duplicate key is inserted. The key is inserted
     *          before the already existing duplicates.
     *      </ul>
     * <p>
     * More information: <a href="http://hamsterdb.com/public/scripts/html_www/group__ham__Database__cfg__parameters.html#ga5bb99ca3c41f069db310123253c1c1fb">C documentation</a>
     */
    public synchronized void insert(Transaction txn, byte[] key,
            byte[] record, int flags)
            throws DatabaseException {
        if (key==null || record==null)
            throw new NullPointerException();
        int status=ham_insert(m_handle, txn!=null ? txn.getHandle() : 0,
                                key, record, flags);
        if (status!=0)
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
     * This method wraps the native ham_erase function.
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
        if (key==null)
            throw new NullPointerException();
        int status=ham_erase(m_handle, txn!=null ? txn.getHandle() : 0, key, 0);
        if (status!=0)
            throw new DatabaseException(status);
    }

    /**
     * Flushes the Database
     * <p>
     * This method wraps the native ham_flush function.
     * <p>
     * This function flushes the Database cache and writes the whole file
     * to disk. If this Database was opened in an Environment, all other
     * Databases of this Environment are flushed as well.
     * <p>
     * Since In-Memory Databases do not have a file on disk, the
     * function will have no effect and will return successfully.
     * <p>
     * More information: <a href="http://hamsterdb.com/public/scripts/html_www/group__ham__Database__cfg__parameters.html#gad4db2642d606577e23808ed8e35b7d30">C documentation</a>
     */
    public synchronized void flush()
            throws DatabaseException {
        int status=ham_flush(m_handle, 0);
        if (status!=0)
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
        int status=ham_get_parameters(m_handle, params);
        if (status!=0)
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
        return ham_get_key_count(m_handle, txn!=null ? txn.getHandle() : 0,
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
     * This method wraps the native ham_close function.
     * <p>
     * This function flushes the Database and then closes the file handle.
     * <p>
     * More information: <a href="http://hamsterdb.com/public/scripts/html_www/group__ham__Database__cfg__parameters.html#gac0e1e492c2b36e2ae0e87d0c0ff6e04e">C documentation</a>
     */
    public synchronized void close(int flags) {
        if (m_handle==0)
            return;
        closeCursors();
        ham_close(m_handle, flags);
        ham_delete(m_handle);
        m_handle=0;
    }

    private HashSet m_cursor_set;

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
            m_cursor_set = new HashSet();
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
    private PrefixCompareCallback m_prefix_cmp;
    private DuplicateCompareCallback m_dupe_cmp;
    private static ErrorHandler m_eh;

    static {
        System.loadLibrary("hamsterdb-java");
    }
}
