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

using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;

namespace Upscaledb
{
  /// <summary>
  /// An extended Parameter, used i.e. in Database.Open, Database.Create
  /// </summary>
  [StructLayout(LayoutKind.Sequential)]
  public struct Parameter
  {
    /// <summary>The name of this Parameter</summary>
    public int name;
    /// <summary>The value of this Parameter</summary>
    public Int64 value;
  }

  /// <summary>
  /// Structure for the upscaledb Version Information
  /// </summary>
  /// <see cref="Database.GetVersion" />
  public struct Version
  {
    /// <summary>The major version number</summary>
    public int major;
    /// <summary>The minor version number</summary>
    public int minor;
    /// <summary>The revision version number</summary>
    public int revision;
  }

  /// <summary>
  /// Operation type used for bulk operations.
  /// </summary>
  public enum OperationType
  {
    /// <summary>
    /// Insert operation
    /// </summary>
    Insert = UpsConst.UPS_OP_INSERT,
    /// <summary>
    /// Erase operation
    /// </summary>
    Erase = UpsConst.UPS_OP_ERASE,
    /// <summary>
    /// Find operation
    /// </summary>
    Find = UpsConst.UPS_OP_FIND
  }

  /// <summary>
  ///  A single operation within a set of bulk operations.
  /// </summary>
  [StructLayout(LayoutKind.Sequential)]
  public struct Operation
  {
      /// <summary>
      ///  The operation type; UPS_OP_INSERT, UPS_OP_ERASE or UPS_OP_FIND
      /// </summary>
      public OperationType OperationType;
      /// <summary>
      ///  The key
      /// </summary>
      public byte[] Key;
      /// <summary>
      ///  The record; not required if OperationType is Erase or Find
      /// </summary>
      public byte[] Record;
      /// <summary>
      /// Flags for ups_db_insert, ups_db_erase, ups_db_find
      /// </summary>
      public Int32 Flags;
      /// <summary>
      /// Operation result status (output)
      /// </summary>
      public Int32 Result;
  }

  /// <summary>
  /// Delegate for handling error messages
  /// </summary>
  /// <remarks>
  /// This delegate method is called whenever a message is emitted.
  /// </remarks>
  /// <param name="level">The debug level (0 = Debug, 1 = Normal,
  /// 3 = Fatal)</param>
  /// <param name="message">The message text</param>
  [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
  public delegate void ErrorHandler(int level,
      [MarshalAs(UnmanagedType.LPStr)]string message);

  /// <summary>
  /// Delegate for comparing two keys
  /// </summary>
  /// <remarks>
  /// This delegate compares two keys - the "left-hand side"
  /// (lhs) and the "right-hand side" (rhs).
  /// <br />
  /// Also see <see cref="Database.SetCompareFunc" />.
  /// 
  /// To convert the parameters to byte[] arrays:
  ///   byte[] alhs = new byte[lhsLength];
  ///   byte[] arhs = new byte[rhsLength];
  ///   Marshal.Copy(lhs, alhs, 0, lhsLength);
  ///   Marshal.Copy(rhs, arhs, 0, rhsLength);
  ///
  /// </remarks>
  /// <returns>-1 if the first key (lhs) is smaller, +1 if the first
  /// key is larger, 0 if both keys are equal</returns>
  [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
  public delegate int CompareFunc(IntPtr handle,
      IntPtr lhs, int lhsLength,
      IntPtr rhs, int rhsLength);

  /// <summary>
  /// A Database class
  /// </summary>
  public class Database : IDisposable
  {
    /// <summary>
    /// Default Constructor
    /// </summary>
    public Database() {
      cursors = new List<Cursor>();
      handle = IntPtr.Zero;
    }

    internal Database(IntPtr handle) {
      this.handle = handle;
      cursors = new List<Cursor>();
    }

    /// <summary>
    /// Destructor - automatically closes the Database
    /// </summary>
    ~Database() {
      Dispose(true);
    }

    /// <summary>
    /// Returns the upscaledb.dll version
    /// </summary>
    /// <returns>The upscaledb.dll version</returns>
    static public Version GetVersion() {
      Version v = new Version();
      NativeMethods.GetVersion(out v.major, out v.minor, out v.revision);
      return v;
    }

    /// <summary>
    /// Sets the global error handler
    /// </summary>
    /// <remarks>
    /// This method wraps the native ups_set_error_handler function.<br />
    /// <br />
    /// This handler will receive all debug messages that are emitted
    /// by upscaledb. You can install the default handler by setting
    /// eh to null.
    /// </remarks>
    /// <param name="eh">The delegate which is called whenever an
    /// error message is emitted; set to null to set the default
    /// error handler</param>
    static public void SetErrorHandler(ErrorHandler eh) {
      NativeMethods.SetErrorHandler(eh);
    }

    /// <summary>
    /// Registers a global compare function.
    /// </summary>
    /// <remarks>
    /// This method wraps the native ups_register_compare function.
    /// </remarks>
    /// <param name="name">Descriptive name of the compare function</param>
    /// <param name="foo">Delegate object</param>
    public static void RegisterCompare(String name, CompareFunc foo) {
      CompareFunc pinned = new CompareFunc(foo);
      callbacks.Add(pinned);
      NativeMethods.RegisterCompare(name, pinned);
    }

    /// <summary>
    /// Sets the comparison function
    /// </summary>
    /// <remarks>
    /// This method wraps the native ups_db_set_compare_func function.<br />
    /// <br />
    /// The <see cref="CompareFunc" /> delegate compares two index keys.
    /// It returns -1 if the first key is smaller, +1 if the second key
    /// is smaller or 0 if both keys are equal.<br />
    /// <br />
    /// If <paramref name="foo"/> is null, upscaledb will use the default
    /// compare function (which is based on memcmp(3)).<br />
    /// </remarks>
    /// <param name="foo">The compare delegate, or null</param>
    public void SetCompareFunc(CompareFunc foo) {
      int st;
      lock (this) {
        CompareFunc pinned = new CompareFunc(foo);
        callbacks.Add(pinned);
        st = NativeMethods.SetCompareFunc(handle, pinned);
      }
      if (st != 0)
        throw new DatabaseException(st);
    }

    /// <summary>
    /// Returns the last error code
    /// </summary>
    /// <remarks>
    /// This method wraps the native ups_db_get_error function.
    /// </remarks>
    /// <returns>The error code of the last operation</returns>
    public int GetLastError() {
      lock (this) {
        return NativeMethods.GetLastError(handle);
      }
    }

    /// <summary>
    /// Searches an item in the Database, returns the record
    /// </summary>
    public byte[] Find(byte[] key) {
      return Find(null, ref key, 0);
    }

    /// <summary>
    /// Searches an item in the Database, returns the record
    /// </summary>
    public byte[] Find(Transaction txn, byte[] key) {
      return Find(txn, ref key, 0);
    }

    /// <summary>
    /// Searches an item in the Database, returns the record
    /// </summary>
    /// <remarks>
    /// This method wraps the native ups_db_find function.<br />
    /// <br />
    /// This function searches the Database for a key. If the key
    /// is found, the method will return the record of this item.
    /// <br />
    /// Database.Find can not search for duplicate keys. If the
    /// key has multiple duplicates, only the first duplicate is returned.
    /// </remarks>
    /// <param name="txn">The optional Transaction</param>
    /// <param name="key">The key of the item</param>
    /// <param name="flags">The flags of the operation</param>
    /// <returns>The record of the item</returns>
    /// <exception cref="DatabaseException">
    ///   <list type="bullet">
    ///   <item><see cref="UpsConst.UPS_KEY_NOT_FOUND"/>
    ///     if the item was not found</item>
    ///   </list>
    /// </exception>
    public byte[] Find(Transaction txn, ref byte[] key, int flags) {
      byte[] record = null;
      lock (this) {
        int st = NativeMethods.Find(handle,
                        txn != null ? txn.Handle : IntPtr.Zero,
                        ref key, ref record, flags);
        if (st != 0)
          throw new DatabaseException(st);
      }
      return record;
    }

    /// <summary>
    /// Inserts a Database item
    /// </summary>
    /// <remarks>
    /// This is an overloaded function for
    ///   Database.Insert(txn, key, record, 0).
    /// </remarks>
    public void Insert(Transaction txn, byte[] key, byte[] record) {
      Insert(txn, key, record, 0);
    }

    /// <summary>
    /// Inserts a Database item
    /// </summary>
    /// <remarks>
    /// This is an overloaded function for
    ///   Database.Insert(null, key, record, 0).
    /// </remarks>
    public void Insert(byte[] key, byte[] record) {
      Insert(null, key, record, 0);
    }

    /// <summary>
    /// Inserts a Database item
    /// </summary>
    /// <remarks>
    /// This is an overloaded function for
    ///   Database.Insert(null, key, record, flags).
    /// </remarks>
    public void Insert(byte[] key, byte[] record, int flags) {
      Insert(null, key, record, flags);
    }

    /// <summary>
    /// Inserts a Database Item
    /// </summary>
    /// <remarks>
    /// This method wraps the native ups_db_insert function.
    /// <br />
    /// This function inserts a key/record pair as a new Database item.
    /// <br />
    /// If the key already exists in the Database, error code
    /// <see cref="UpsConst.UPS_DUPLICATE_KEY" /> is thrown.
    /// <br />
    /// If you wish to overwrite an existing entry specify the flag
    /// <see cref="UpsConst.UPS_OVERWRITE"/>
    /// <br />
    /// If you wish to insert a duplicate key specify the flag
    /// <see cref="UpsConst.UPS_DUPLICATE" />. (Note that
    /// the Database has to be created with the flag
    /// <see cref="UpsConst.UPS_ENABLE_DUPLICATE_KEYS" /> in order
    /// to use duplicate keys.)
    /// The duplicate key is inserted after all other duplicate keys (see
    /// <see cref="UpsConst.UPS_DUPLICATE_INSERT_LAST" />).
    /// </remarks>
    /// <param name="txn">An optional Transaction object</param>
    /// <param name="key">The key of the new item</param>
    /// <param name="record">The record of the new item</param>
    /// <param name="flags">Optional flags for this operation. Possible
    /// flags are:
    /// <list type="bullet">
    ///   <item><see cref="UpsConst.UPS_OVERWRITE"/>
    ///     If the key already exists, the record is overwritten.
    ///     Otherwise, the key is inserted.</item>
    ///   <item><see cref="UpsConst.UPS_DUPLICATE"/>
    ///     If the key already exists, a duplicate key is inserted.
    ///     The key is inserted before the already existing duplicates.
    ///     </item>
    /// </list></param>
    /// <exception cref="DatabaseException">
    ///   <list type="bullet">
    ///   <item><see cref="UpsConst.UPS_INV_PARAMETER"/>
    ///     if the flags UpsConst.UPS_DUPLICATE <b>AND</b>
    ///     UpsConst.UPS_OVERWRITE were specified, or if
    ///     UpsConst.UPS_DUPLICATE was specified but the Database
    ///     was not created with UpsConst.UPS_ENABLE_DUPLICATE_KEYS</item>
    ///   <item><see cref="UpsConst.UPS_WRITE_PROTECTED"/>
    ///     if you tried to insert a key in a read-only Database</item>
    ///   <item><see cref="UpsConst.UPS_INV_KEYSIZE"/>
    ///     if key size is different than than the key size parameter
    ///     specified for Database.Create.</item>
    ///   </list>
    /// </exception>
    public void Insert(Transaction txn, byte[] key, byte[] record,
              int flags) {
      int st;
      lock (this) {
        st = NativeMethods.Insert(handle,
                  txn != null ? txn.Handle : IntPtr.Zero,
                  key, record, flags);
      }
      if (st != 0)
        throw new DatabaseException(st);
    }

    /// <summary>
    /// Inserts a Database Item into a Record Number Database
    /// </summary>
    /// <remarks>
    /// This is an overloaded function for
    ///   Database.InsertRecNo(null, record, 0).
    /// </remarks>
    public byte[] InsertRecNo(byte[] record)
    {
        return InsertRecNo(null, record, 0);
    }

    /// <summary>
    /// Inserts a Database Item into a Record Number Database
    /// </summary>
    /// <remarks>
    /// This is an overloaded function for
    ///   Database.InsertRecNo(null, record, flags).
    /// </remarks>
    public byte[] InsertRecNo(byte[] record, int flags)
    {
        return InsertRecNo(null, record, flags);
    }

    /// <summary>
    /// Inserts a Database Item into a Record Number Database
    /// </summary>
    /// <returns name="key">The key of the new item</returns>
    /// <remarks>
    /// This method wraps the native ups_db_insert function.
    /// <br />
    /// This function inserts a record as a new Database item.
    /// <br />
    /// </remarks>
    /// <param name="txn">An optional Transaction object</param>
    /// <param name="record">The record of the new item</param>
    /// <param name="flags">Optional flags for this operation.</param>
    /// <exception cref="DatabaseException">
    ///   <list type="bullet">
    ///   <item><see cref="UpsConst.UPS_WRITE_PROTECTED"/>
    ///     if you tried to insert a key in a read-only Database</item>
    ///   </list>
    /// </exception>
    public byte [] InsertRecNo(Transaction txn, byte[] record, int flags)
    {
        int st;
        byte[] key = null;
        lock (this)
        {
            st = NativeMethods.InsertRecNo(handle,
                      txn != null ? txn.Handle : IntPtr.Zero,
                      ref key, record, flags);
        }
        if (st != 0)
            throw new DatabaseException(st);
        return key;
    }

    /// <summary>
    /// Erases a Database Item
    /// </summary>
    public void Erase(byte[] key) {
      Erase(null, key);
    }

    /// <summary>
    /// Erases a Database Item
    /// </summary>
    /// <remarks>
    /// This method wraps the native ups_db_erase function.
    /// <br />
    /// This function erases a Database item. If the item with the
    /// specified key does not exist in the Database, error code
    /// <see cref="UpsConst.UPS_KEY_NOT_FOUND" /> is thrown.
    /// <br />
    /// Note that this method can not erase a single duplicate key.
    /// If the key has multiple duplicates, all duplicates of this key
    /// will be erased. Use <see cref="Cursor.Erase" /> to erase a
    /// specific duplicate key.
    /// </remarks>
    /// <param name="txn">The optional Transaction</param>
    /// <param name="key">The key of the item to delete</param>
    /// <exception cref="DatabaseException">
    ///   <list type="bullet">
    ///   <item><see cref="UpsConst.UPS_KEY_NOT_FOUND"/>
    ///     if the key was not found</item>
    ///   <item><see cref="UpsConst.UPS_WRITE_PROTECTED"/>
    ///     if you tried to insert a key in a read-only Database</item>
    ///   </list>
    /// </exception>
    public void Erase(Transaction txn, byte[] key) {
      int st;
      lock (this) {
        st = NativeMethods.Erase(handle,
              txn != null ? txn.Handle : IntPtr.Zero, key, 0);
      }
      if (st != 0)
        throw new DatabaseException(st);
    }

    /// <summary>
    /// Perform bulk operations on a database.
    /// This function receives an array of Operation structures
    /// and performs the necessary calls to Insert, Erase
    /// and Find.  The Result field within each operation is populated 
    /// with the result status of each operation and should be checked by the caller.
    /// </summary>
    /// <param name="txn">The optional Transaction passed to ups_db_insert, ups_db_erase and ups_db_find.</param>
    /// <param name="operations">The operations to perform.</param>
    /// <param name="flags">Optional flags for this operation.</param>
    public void BulkOperations(Transaction txn, Operation[] operations, int flags)
    {
      int st;
      lock (this) {
        st = NativeMethods.BulkOperations(handle, (txn != null ? txn.Handle : IntPtr.Zero), operations, flags);
      }
      if (st != 0)
        throw new DatabaseException(st);
    }

    /// <summary>
    /// Perform bulk operations on a database.
    /// This function receives an array of Operation structures
    /// and performs the necessary calls to Insert, Erase
    /// and Find.  The Result field within each operation is populated 
    /// with the result status of each operation and should be checked by the caller.
    /// </summary>
    /// <param name="operations">The operations to perform.</param>
    /// <param name="flags">Optional flags for this operation.</param>
    /// <remarks>
    /// This is an overloaded function for Database.BulkOperations(null, operations, flags).
    /// </remarks>
    public void BulkOperations(Operation[] operations, int flags)
    {
        BulkOperations(null, operations, flags);
    }

    /// <summary>
    /// Perform bulk operations on a database.
    /// This function receives an array of Operation structures
    /// and performs the necessary calls to Insert, Erase
    /// and Find.  The Result field within each operation is populated 
    /// with the result status of each operation and should be checked by the caller.
    /// </summary>
    /// <param name="operations">The operations to perform.</param>
    /// <remarks>
    /// This is an overloaded function for Database.BulkOperations(null, operations, 0).
    /// </remarks>
    public void BulkOperations(Operation[] operations)
    {
        BulkOperations(null, operations, 0);
    }

    /// <summary>
    /// Returns the number of keys in this Database
    /// </summary>
    public Int64 GetCount(int flags) {
      return GetCount(null, flags);
    }

    /// <summary>
    /// Returns the number of keys in this Database
    /// </summary>
    public Int64 GetCount() {
      return GetCount(0);
    }

    /// <summary>
    /// Returns the number of keys in this Database
    /// </summary>
    /// <remarks>
    /// This method wraps the native ups_db_count function.
    /// <br />
    /// You can specify UPS_SKIP_DUPLICATES if you do now want
    /// to include any duplicates in the count.
    /// </remarks>
    public Int64 GetCount(Transaction txn, int flags) {
      int st;
      Int64 count = 0;
      lock (this) {
        st = NativeMethods.GetCount(handle,
                  txn != null ? txn.Handle : IntPtr.Zero,
                  flags, out count);
      }
      if (st != 0)
        throw new DatabaseException(st);
      return count;
    }

    /// <summary>
    /// Closes the Database
    /// </summary>
    /// <remarks>
    /// This method wraps the native ups_db_close function.
    /// <br />
    /// Before closing the Database, the cache is flushed to Disk.
    /// </remarks>
    public void Close() {
      lock (this) {
        if (handle == IntPtr.Zero)
          return;
        foreach (Cursor c in cursors)
          c.Close();
        cursors.Clear();
        int st = NativeMethods.Close(handle, 0);
        if (st != 0)
          throw new DatabaseException(st);
        handle = IntPtr.Zero;
      }
    }

    /// <summary>
    /// Closes the Database
    /// </summary>
    public void Dispose() {
      Dispose(true);
      GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Closes the Database
    /// </summary>
    protected virtual void Dispose(bool all) {
      if (all)
        Close();
    }

    /// <summary>
    /// Returns the low-level Database handle
    /// </summary>
    public IntPtr Handle {
      get {
        return handle;
      }
    }

    /// <summary>
    /// Adds a Cursor to the Cursor list
    /// </summary>
    public void AddCursor(Cursor c) {
      cursors.Add(c);
    }

    /// <summary>
    /// Removes a Cursor from the Cursor list
    /// </summary>
    public void RemoveCursor(Cursor c) {
      cursors.Remove(c);
    }

    private static Parameter[] AppendNullParameter(Parameter[] parameters) {
      Parameter[] newArray = new Parameter[parameters.GetLength(0) + 1];
      for (int i = 0; i < parameters.GetLength(0); i++)
        newArray[i] = parameters[i];
      return newArray;
    }

    private IntPtr handle;
    private readonly List<Cursor> cursors;
    private static readonly List<CompareFunc> callbacks = new List<CompareFunc>();
  }
}
