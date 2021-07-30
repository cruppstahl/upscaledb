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

namespace Upscaledb
{
  /// <summary>
  /// A Database Environment class
  /// </summary>
  public class Environment : IDisposable
  {
    /// <summary>
    /// Default Constructor
    /// </summary>
    public Environment() {
      handle = IntPtr.Zero;
      databases = new List<Database>();
    }

    /// <summary>
    /// Default Constructor which sets the handle
    /// </summary>
    /// <remarks>This constructor is used by Database.GetEnvironment()</remarks>
    public Environment(IntPtr ptr) {
      handle = ptr;
      databases = new List<Database>();
    }

    /// <summary>
    /// Destructor - automatically closes the Environment
    /// </summary>
    ~Environment() {
      Dispose(true);
    }

    /// <summary>
    /// Creates a new Environment
    /// </summary>
    /// <remarks>
    /// This is an overloaded function for
    ///   Create(fileName, 0, 0, null).
    /// </remarks>
    public void Create(string fileName) {
      Create(fileName, 0, 0, null);
    }

    /// <summary>
    /// Creates a new Environment
    /// </summary>
    /// <remarks>
    /// This is an overloaded function for
    ///   Create(fileName, flags, 0, null).
    /// </remarks>
    public void Create(String fileName, int flags) {
      Create(fileName, flags, 0644, null);
    }

    /// <summary>
    /// Creates a new Environment
    /// </summary>
    /// <remarks>
    /// This is an overloaded function for
    ///   Create(fileName, flags, mode, null).
    /// </remarks>
    public void Create(String fileName, int flags, int mode) {
      Create(fileName, flags, mode, null);
    }

    /// <summary>
    /// Creates a new Database
    /// </summary>
    /// <remarks>
    /// This method wraps the native ups_env_create function.
    /// <br />
    /// A Database Environment is a collection of Databases, which are all
    /// stored in one physical file (or in-memory). Per default, up to 16
    /// Databases can be stored in one file (<see
    /// cref="UpsConst.UPS_PARAM_MAX_DATABASES" />
    /// on how to store even more Databases).
    /// <br />
    /// Each Database is identified by a positive 16bit value (except
    /// 0 and values at or above 0xf000).
    /// Databases in an Environment can be created with
    /// <see cref="Environment.CreateDatabase(short)" /> or opened with
    /// <see cref="Environment.OpenDatabase(short)" />.
    /// </remarks>
    ///
    /// <param name="fileName">The file name of the Environment file. If
    /// the file already exists, it is overwritten. Can be null if you
    /// create an In-Memory Environment.</param>
    /// <param name="flags">Optional flags for this operation, combined
    /// with bitwise OR. Possible flags are:
    ///   <list type="bullet">
    ///   <item><see cref="UpsConst.UPS_ENABLE_FSYNC" />
    ///     Immediately write modified pages to the disk. This
    ///     slows down all Database operations, but may save the
    ///     Database integrity in case of a system crash.</item><br />
    ///   <item><see cref="UpsConst.UPS_IN_MEMORY" />
    ///     Creates an In-Memory Environment. No file will be created,
    ///     and the Databases are lost after the Environment
    ///     is closed. The <paramref name="fileName" /> parameter can
    ///     be null. Do <b>NOT</b> use in combination with
    ///     a cache size other than 0.</item><br />
    ///   <item><see cref="UpsConst.UPS_DISABLE_MMAP" />
    ///     Do not use memory mapped files for I/O. By default,
    ///     upscaledb checks if it can use mmap, since mmap is faster
    ///     than read/write. For performance reasons, this flag should
    ///     not be used.</item><br />
    ///   <item><see cref="UpsConst.UPS_ENABLE_TRANSACTIONS" />
    ///     Enables Transactions for this Database.</item><br />
    ///   </list>
    /// </param>
    /// <param name="mode">File access rights for the new file. This is
    /// the <i>mode</i> parameter for creat(2). Ignored on
    /// Microsoft Windows.</param>
    /// <param name="parameters">An array of <see cref="Parameter" />
    /// structures. The following parameters are available:<br />
    ///   <list type="bullet">
    ///   <item><see cref="UpsConst.UPS_PARAM_CACHESIZE" />
    ///     The size of the Database cache, in bytes. The default size
    ///     is defined in <i>src/config.h</i> as UPS_DEFAULT_CACHESIZE
    ///     - usually 2 MB.</item><br />
    ///   <item><see cref="UpsConst.UPS_PARAM_PAGESIZE" />
    ///     The size of a file page, in bytes. It is recommended not
    ///     to change the default size. The default size depends on
    ///     hardware and operating system. Page sizes must be 1024 or a
    ///     multiple of 2048.</item><br />
    ///   <item><see cref="UpsConst.UPS_PARAM_MAX_DATABASES" />
    ///     The number of maximum Databases in this Environment;
    ///     default: 16.</item>
    ///   </list>
    /// </param>
    /// <exception cref="DatabaseException">
    ///   <list type="bullet">
    ///   <item><see cref="UpsConst.UPS_INV_PARAMETER"/>
    ///     if an invalid combination of flags was specified</item>
    ///   <item><see cref="UpsConst.UPS_INV_PARAMETER"/>
    ///     if the value for UPS_PARAM_MAX_DATABASES is too
    ///     high (either decrease it or increase the page size)</item>
    ///   <item><see cref="UpsConst.UPS_IO_ERROR"/>
    ///     if the file could not be opened or reading/writing failed</item>
    ///   <item><see cref="UpsConst.UPS_OUT_OF_MEMORY"/>
    ///     if memory could not be allocated</item>
    ///   <item><see cref="UpsConst.UPS_INV_PAGESIZE"/>
    ///     if the page size is not a multiple of 1024</item>
    ///   <item><see cref="UpsConst.UPS_INV_KEYSIZE"/>
    ///     if the key size is too large (at least 4 keys must
    ///     fit in a page)</item>
    ///   <item><see cref="UpsConst.UPS_WOULD_BLOCK"/>
    ///     if another process has locked the file</item>
    ///   </list>
    /// </exception>
    public void Create(String fileName, int flags, int mode,
        Parameter[] parameters) {
      int st;
      handle = new IntPtr(0);
      if (parameters != null)
        parameters = AppendNullParameter(parameters);
      lock (this) {
        st = NativeMethods.EnvCreate(out handle, fileName, flags, mode,
            parameters);
      }
      if (st != 0)
        throw new DatabaseException(st);
    }

    /// <summary>
    /// Opens an existing Environment
    /// </summary>
    /// <remarks>
    /// This is an overloaded function for
    ///   Open(fileName, 0, null).
    /// </remarks>
    public void Open(string fileName)
    {
        Open(fileName, 0, null);
    }

    /// <summary>
    /// Opens an existing Environment
    /// </summary>
    /// <remarks>
    /// This is an overloaded function for
    ///   Open(fileName, flags, null).
    /// </remarks>
    public void Open(String fileName, int flags)
    {
        Open(fileName, flags, null);
    }

    /// <summary>
    /// Opens an existing Environment
    /// </summary>
    /// <remarks>
    /// This method wraps the native ups_env_open function.
    /// </remarks>
    ///
    /// <param name="fileName">The file name of the Environment file.</param>
    /// <param name="flags">Optional flags for this operation, combined
    /// with bitwise OR. Possible flags are:
    ///   <list type="bullet">
    ///   <item><see cref="UpsConst.UPS_READ_ONLY" />
    ///     Opens the file for reading only. Operations which need
    ///     write access (i.e. Database.Insert)
    ///     will return <see cref="UpsConst.UPS_WRITE_PROTECTED" />.
    ///     </item><br />
    ///   <item><see cref="UpsConst.UPS_ENABLE_FSYNC" />
    ///     Immediately write modified pages to the disk. This
    ///     slows down all Database operations, but may save the
    ///     Database integrity in case of a system crash.</item><br />
    ///   <item><see cref="UpsConst.UPS_DISABLE_MMAP" />
    ///     Do not use memory mapped files for I/O. By default,
    ///     upscaledb checks if it can use mmap, since mmap is faster
    ///     than read/write. For performance reasons, this flag should
    ///     not be used.</item><br />
    ///   <item><see cref="UpsConst.UPS_AUTO_RECOVERY" />
    ///     Automatically recover the Database, if necessary. This
    ///     flag imples <see cref="UpsConst.UPS_ENABLE_TRANSACTIONS" />.
    ///     </item><br />
    ///   <item><see cref="UpsConst.UPS_ENABLE_TRANSACTIONS" />
    ///     Enables Transactions for this Database.</item><br />
    ///   </list>
    /// </param>
    /// <param name="parameters">An array of <see cref="Parameter" />
    /// structures. The following parameters are available:<br />
    ///   <list type="bullet">
    ///   <item><see cref="UpsConst.UPS_PARAM_CACHESIZE" />
    ///     The size of the Database cache, in bytes. The default size
    ///     is defined in <i>src/config.h</i> as UPS_DEFAULT_CACHESIZE
    ///     - usually 2 MB.</item><br />
    ///   </list>
    /// </param>
    /// <exception cref="DatabaseException">
    ///   <list type="bullet">
    ///   <item><see cref="UpsConst.UPS_INV_PARAMETER"/>
    ///     if an invalid combination of flags was specified</item>
    ///   <item><see cref="UpsConst.UPS_FILE_NOT_FOUND"/>
    ///     if the file does not exist</item>
    ///   <item><see cref="UpsConst.UPS_IO_ERROR"/>
    ///     if the file could not be opened or reading/writing failed</item>
    ///   <item><see cref="UpsConst.UPS_INV_FILE_VERSION"/>
    ///     if the Database version is not compatible with the library
    ///     version</item>
    ///   <item><see cref="UpsConst.UPS_OUT_OF_MEMORY"/>
    ///     if memory could not be allocated</item>
    ///   <item><see cref="UpsConst.UPS_WOULD_BLOCK"/>
    ///     if another process has locked the file</item>
    ///   </list>
    /// </exception>
    public void Open(String fileName, int flags,
        Parameter[] parameters) {
      int st;
      handle = new IntPtr(0);
      if (parameters != null)
        parameters = AppendNullParameter(parameters);
      lock (this) {
        st = NativeMethods.EnvOpen(out handle, fileName, flags,
              parameters);
      }
      if (st != 0)
        throw new DatabaseException(st);
    }

    /// <summary>
    /// Creates a new Database in this Environment
    /// </summary>
    /// <remarks>This is an overloaded function for
    ///   CreateDatabase(name, 0, null).</remarks>
    /// <returns>The new Database object</returns>
    public Database CreateDatabase(short name) {
      return CreateDatabase(name, 0, null);
    }

    /// <summary>
    /// Creates a new Database in this Environment
    /// </summary>
    /// <remarks>This is an overloaded function for
    ///   CreateDatabase(name, flags, null).</remarks>
    /// <returns>The new Database object</returns>
    public Database CreateDatabase(short name, int flags) {
      return CreateDatabase(name, flags, null);
    }

    /// <summary>
    /// Creates a new Database in this Environment
    /// </summary>
    /// <remarks>This method wraps the native ups_env_create_db function.
    /// </remarks>
    /// <returns>The new Database object</returns>
    /// <param name="name">The name of the Database. If a Database
    /// with this name already exists,
    /// <see cref="UpsConst.UPS_DATABASE_ALREADY_EXISTS"/> is thrown.
    /// Database names from 0xf000 to 0xffff and 0 are reserved.</param>
    /// <param name="flags">Optional flags for creating the Database,
    /// combined with bitwise OR. Possible values are:
    ///   <list>
    ///   <item><see cref="UpsConst.UPS_RECORD_NUMBER" />
    ///     Creates an "auto-increment" Database. Keys in Record
    ///     Number Databases are automatically assigned an incrementing
    ///     64bit value.</item>
    ///   </list>
    /// </param>
    /// <param name="parameters">An array of <see cref="Parameter" />
    /// structures. The following parameters are available:<br />
    ///   <list type="bullet">
    ///   <item><see cref="UpsConst.UPS_PARAM_KEYSIZE" />
    ///     The size of the keys in the B+Tree index. The default size
    ///     is 21 bytes.</item><br />
    ///   </list>
    /// </param>
    /// <exception cref="DatabaseException">
    ///   <list type="bullet">
    ///   <item><see cref="UpsConst.UPS_INV_PARAMETER"/>
    ///     if an invalid combination of flags was specified</item>
    ///   <item><see cref="UpsConst.UPS_DATABASE_ALREADY_EXISTS"/>
    ///     if a Database with this name already exists in this
    ///     Environment</item>
    ///   <item><see cref="UpsConst.UPS_OUT_OF_MEMORY"/>
    ///     if memory could not be allocated</item>
    ///   <item><see cref="UpsConst.UPS_INV_KEYSIZE"/>
    ///     if the key size is too large (at least 4 keys must
    ///     fit in a page)</item>
    ///   <item><see cref="UpsConst.UPS_LIMITS_REACHED"/>
    ///     if the maximum number of Databases per Environment
    ///     was already created</item>
    ///   </list>
    /// </exception>
    /// <returns>The new Database object</returns>
    public Database CreateDatabase(short name, int flags,
            Parameter[] parameters) {
      int st;
      IntPtr dbh = new IntPtr(0);
      if (parameters != null)
        parameters = AppendNullParameter(parameters);
      lock (this) {
        st = NativeMethods.EnvCreateDatabase(handle, out dbh,
              name, flags, parameters);
      }
      if (st != 0)
        throw new DatabaseException(st);
      
      Database db = new Database(dbh);
      databases.Add(db);
      return db;
    }

    /// <summary>
    /// Opens a Database in this Environment
    /// </summary>
    /// <remarks>This is an overloaded function for
    ///   OpenDatabase(name, 0).</remarks>
    /// <returns>The new Database object</returns>
    public Database OpenDatabase(short name) {
      return OpenDatabase(name, 0, null);
    }

    /// <summary>
    /// Opens a Database in this Environment
    /// </summary>
    /// <remarks>This is an overloaded function for
    ///   OpenDatabase(name, flags, null).</remarks>
    /// <returns>The new Database object</returns>
    public Database OpenDatabase(short name, int flags) {
      return OpenDatabase(name, flags, null);
    }

    /// <summary>
    /// Opens a Database in this Environment
    /// </summary>
    /// <remarks>
    /// This method wraps the native ups_env_open_db function.
    /// </remarks>
    /// <param name="name">The name of the Database. If a Database
    /// with this name does not exist, the function will throw
    /// <see cref="UpsConst.UPS_DATABASE_NOT_FOUND"/>.</param>
    /// <param name="flags">Optional flags for this operation, combined
    /// with bitwise OR. Possible flags are:
    ///   <list type="bullet">
    ///   <item><see cref="UpsConst.UPS_READ_ONLY" />
    ///     Opens the database for reading.</item>
    ///   </list>
    /// </param>
    /// <param name="parameters">An array of <see cref="Parameter" />
    /// structures. The following parameters are available:<br />
    ///   <list type="bullet">
    ///   </list>
    /// </param>
    /// <exception cref="DatabaseException">
    ///   <list type="bullet">
    ///   <item><see cref="UpsConst.UPS_INV_PARAMETER"/>
    ///     if an invalid combination of flags was specified</item>
    ///   <item><see cref="UpsConst.UPS_DATABASE_NOT_FOUND"/>
    ///     if a Database with this name does not exist</item>
    ///   <item><see cref="UpsConst.UPS_DATABASE_ALREADY_OPEN"/>
    ///     if this Database was already opened</item>
    ///   <item><see cref="UpsConst.UPS_OUT_OF_MEMORY"/>
    ///     if memory could not be allocated</item>
    ///   <item><see cref="UpsConst.UPS_WOULD_BLOCK"/>
    ///     if another process has locked the file</item>
    ///   </list>
    /// </exception>
    /// <returns>The new Database object</returns>
    public Database OpenDatabase(short name, int flags,
        Parameter[] parameters) {
      int st;
      IntPtr dbh = new IntPtr(0);
      if (parameters != null)
        parameters = AppendNullParameter(parameters);
      lock (this) {
        st = NativeMethods.EnvOpenDatabase(handle, out dbh,
            name, flags, parameters);
      }
      if (st != 0)
        throw new DatabaseException(st);
      
      Database db = new Database(dbh);
      databases.Add(db);
      return db;
    }

    /// <summary>
    /// Renames a Database in this Environment
    /// </summary>
    /// <remarks>
    /// This method wraps the native ups_env_rename_db function.
    /// </remarks>
    /// <param name="oldName">The old name of the Database. If a Database
    /// with this name does not exist, the function will throw
    /// <see cref="UpsConst.UPS_DATABASE_NOT_FOUND"/>.</param>
    /// <param name="newName">The new name of the Database. If a Database
    /// with this name already exists, the function will throw
    /// <see cref="UpsConst.UPS_DATABASE_ALREADY_EXISTS"/>.</param>
    /// <exception cref="DatabaseException">
    ///   <list type="bullet">
    ///   <item><see cref="UpsConst.UPS_INV_PARAMETER"/>
    ///     if the new Database name is reserved</item>
    ///   <item><see cref="UpsConst.UPS_DATABASE_NOT_FOUND"/>
    ///     if a Database with this name does not exist</item>
    ///   <item><see cref="UpsConst.UPS_DATABASE_ALREADY_EXISTS"/>
    ///     if a Database with the new name already exists</item>
    ///   <item><see cref="UpsConst.UPS_OUT_OF_MEMORY"/>
    ///     if memory could not be allocated</item>
    ///   </list>
    /// </exception>
    public void RenameDatabase(short oldName, short newName) {
      int st;
      lock (this) {
        st = NativeMethods.EnvRenameDatabase(handle, oldName, newName);
      }
      if (st != 0)
        throw new DatabaseException(st);
    }

    /// <summary>
    /// Deletes a Database from this Environment
    /// </summary>
    /// <remarks>
    /// This method wraps the native ups_env_erase_db function.
    /// </remarks>
    /// <param name="name">The name of the Database which is deleted.
    /// If a Database with this name does not exist, the function will throw
    /// <see cref="UpsConst.UPS_DATABASE_NOT_FOUND"/>.</param>
    /// <exception cref="DatabaseException">
    ///   <list type="bullet">
    ///   <item><see cref="UpsConst.UPS_DATABASE_NOT_FOUND"/>
    ///     if a Database with this name does not exist</item>
    ///   <item><see cref="UpsConst.UPS_DATABASE_ALREADY_OPEN"/>
    ///     if the Database with the new name is still open</item>
    ///   </list>
    /// </exception>
    public void EraseDatabase(short name) {
      int st;
      lock (this) {
        st = NativeMethods.EnvEraseDatabase(handle, name, 0);
      }
      if (st != 0)
        throw new DatabaseException(st);
    }

    /// <summary>
    /// Flushes the Environment
    /// </summary>
    /// <remarks>
    /// This method wraps the native ups_env_flush function.
    /// <br />
    /// This function flushes the Database cache and writes the whole
    /// file to disk.
    /// <br />
    /// Since In-Memory Environments do not have a file on disk, the
    /// function will have no effect and will return successfully.
    /// </remarks>
    public void Flush() {
      int st;
      lock (this) {
        st = NativeMethods.EnvFlush(handle, 0);
      }
      if (st != 0)
        throw new DatabaseException(st);
    }

    /// <summary>
    /// Returns the names of all Databases in this Environment
    /// </summary>
    /// <remarks>
    /// This method wraps the native ups_env_get_database_names function.
    /// <br />
    /// This function returns the names of all Databases and the number of
    /// Databases in an Environment.
    /// </remarks>
    /// <returns>An Array with all Database names</returns>
    public short[] GetDatabaseNames() {
      short[] ret;
      int st;
      lock (this) {
        st = NativeMethods.EnvGetDatabaseNames(handle, out ret);
      }
      if (st != 0)
        throw new DatabaseException(st);
      return ret;
    }

    /// <summary>
    /// Closes the Environment
    /// </summary>
    public void Dispose() {
      Dispose(true);
      GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Closes the Environment
    /// </summary>
    protected virtual void Dispose(bool all) {
      if (all)
        Close();
    }

    /// <summary>
    /// Begins a new Transaction
    /// </summary>
    public Transaction Begin() {
      return Begin(0);
    }

    /// <summary>
    /// Begins a new Transaction
    /// </summary>
    /// <remarks>
    /// This method wraps the native ups_txn_begin function.
    /// </remarks>
    public Transaction Begin(int flags) {
      int st;
      IntPtr txnh;
      lock (this) {
        st = NativeMethods.TxnBegin(out txnh, handle, null,
            IntPtr.Zero, flags);
      }
      if (st != 0)
        throw new DatabaseException(st);
      return new Transaction(this, txnh);
    }

    public Result SelectRange(String query, Cursor begin, Cursor end) {
      int st;
      IntPtr result = new IntPtr(0);
      lock (this) {
        st = NativeMethods.EnvSelectRange(handle, query,
                                begin != null ? begin.GetHandle() : IntPtr.Zero,
                                end != null ? end.GetHandle() : IntPtr.Zero,
                                out result);
      }
      if (st != 0)
        throw new DatabaseException(st);
      
      return new Result(result);
    }

    /// <summary>
    /// Closes the Environment
    /// </summary>
    /// <remarks>
    /// This method wraps the native ups_env_close function.
    /// <br />
    /// </remarks>
    public void Close() {
      if (handle == IntPtr.Zero)
        return;
      lock (this) {
        foreach (Database db in databases)
          db.Close();
        databases.Clear();
        int st = NativeMethods.EnvClose(handle, 0);
        if (st != 0)
          throw new DatabaseException(st);
        handle = IntPtr.Zero;
      }
    }

    /// <summary>
    /// Returns the Environment handle
    /// </summary>
    public IntPtr GetHandle() {
      return handle;
    }

    private static Parameter[] AppendNullParameter(Parameter[] parameters) {
      Parameter[] newArray = new Parameter[parameters.GetLength(0) + 1];
      for (int i = 0; i < parameters.GetLength(0); i++)
        newArray[i] = parameters[i];
      return newArray;
    }

    private IntPtr handle;
    private readonly List<Database> databases;
  }
}
