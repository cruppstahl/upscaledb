/*
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

using System;
using System.Security;
using System.Collections.Generic;
using System.Runtime.InteropServices;

namespace Hamster
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
    /// Structure for the hamsterdb Version Information
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
    /// Structure for the hamsterdb License Information
    /// </summary>
    /// <see cref="Database.GetLicense" />
    public struct License
    {
        /// <summary>The name of the licensee, or &quot;&quot; for GPL version</summary>
        public String licensee;
        /// <summary>The name of the licensed product</summary>
        public String product;
    }

    /// <summary>
    /// Delegate for comparing two keys
    /// </summary>
    /// <remarks>
    /// This delegate compares two keys - the "left-hand side"
    /// (lhs) and the "right-hand side" (rhs).
    /// <br />
    /// Also see <see cref="Database.SetCompareFunc" />.
    /// </remarks>
    /// <param name="lhs">The first key</param>
    /// <param name="rhs">The second key</param>
    /// <returns>-1 if the first key (lhs) is smaller, +1 if the first
    /// key is larger, 0 if both keys are equal</returns>
    public delegate int CompareFunc(byte[] lhs, byte[] rhs);

    /// <summary>
    /// Delegate for comparing the prefixes of two keys
    /// </summary>
    /// <remarks>
    /// The compare method compares the prefixes of two keys - the
    /// "left-hand side" (lhs) and the "right-hand side (rhs).
    /// <br />
    /// Also see <see cref="Database.SetPrefixCompareFunc" />.
    /// </remarks>
    /// <param name="lhs">The prefix of the first key</param>
    /// <param name="lhsRealLength">The real length of the first key</param>
    /// <param name="rhs">The prefix of the second key</param>
    /// <param name="rhsRealLength">The real length of the second key</param>
    /// <returns>-1 if the first key (lhs) is smaller, +1 if the first
    /// key is larger, 0 if both keys are equal or
    /// <see cref="HamConst.HAM_PREFIX_REQUEST_FULLKEY" /> if the prefixes
    /// are not sufficient for the comparison</returns>
    public delegate int PrefixCompareFunc(byte[] lhs,
            int lhsRealLength, byte[] rhs, int rhsRealLength);

    /// <summary>
    /// Delegate for comparing two records of duplicate keys
    /// </summary>
    /// <remarks>
    /// This delegate compares two records - the "left-hand side"
    /// (lhs) and the "right-hand side" (rhs).
    /// <br />
    /// Also see <see cref="Database.SetDuplicateCompareFunc" />.
    /// </remarks>
    /// <param name="lhs">The first record</param>
    /// <param name="rhs">The second record</param>
    /// <returns>-1 if the first record (lhs) is smaller, +1 if the first
    /// record is larger, 0 if both records are equal</returns>
    public delegate int DuplicateCompareFunc(byte[] lhs, byte[] rhs);

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
    /// A Database class
    /// </summary>
    public class Database : IDisposable
    {
        /// <summary>
        /// Default Constructor
        /// </summary>
        public Database() {
            this.cursors = new List<Cursor>();
            pinnedCompareFunc = new NativeMethods.CompareFunc(MyCompareFunc);
            pinnedPrefixCompareFunc = new NativeMethods.PrefixCompareFunc(MyPrefixCompareFunc);
            pinnedDupeCompareFunc = new NativeMethods.DuplicateCompareFunc(MyDuplicateCompareFunc);
        }

        internal Database(IntPtr handle) {
            this.handle = handle;
            this.initialized = true;
            this.cursors = new List<Cursor>();
        }

        /// <summary>
        /// Destructor - automatically closes the Database
        /// </summary>
        ~Database() {
            Dispose(true);
        }

        /// <summary>
        /// Returns the hamsterdb.dll version
        /// </summary>
        /// <returns>The hamsterdb.dll version</returns>
        static public Version GetVersion() {
            Version v = new Version();
            NativeMethods.GetVersion(out v.major, out v.minor, out v.revision);
            return v;
        }

        /// <summary>
        /// Returns the license information
        /// </summary>
        /// <returns>The license information</returns>
        static public License GetLicense() {
            License l = new License();
            NativeMethods.GetLicense(out l.licensee, out l.product);
            return l;
        }

        /// <summary>
        /// Sets the global error handler
        /// </summary>
        /// <remarks>
        /// This method wraps the native ham_set_error_handler function.<br />
        /// <br />
        /// This handler will receive all debug messages that are emitted
        /// by hamsterdb. You can install the default handler by setting
        /// eh to null.
        /// </remarks>
        /// <param name="eh">The delegate which is called whenever an
        /// error message is emitted; set to null to set the default
        /// error handler</param>
        static public void SetErrorHandler(ErrorHandler eh) {
            NativeMethods.SetErrorHandler(eh);
        }

        /// <summary>
        /// Creates a new Database
        /// </summary>
        /// <remarks>
        /// This is an overloaded function for
        ///   Create(fileName, 0, 0, null).
        /// </remarks>
        public void Create(String fileName) {
            Create(fileName, 0, 0644, null);
        }

        /// <summary>
        /// Creates a new Database
        /// </summary>
        /// <remarks>
        /// This is an overloaded function for
        ///   Create(fileName, flags, 0, null).
        /// </remarks>
        public void Create(String fileName, int flags) {
            Create(fileName, flags, 0644, null);
        }

        /// <summary>
        /// Creates a new Database
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
        /// This method wraps the native ham_create_ex function.
        /// </remarks>
        ///
        /// <param name="fileName">The file name of the Database file. If
        /// the file already exists, it is overwritten. Can be null if you
        /// create an In-Memory Database.</param>
        /// <param name="flags">Optional flags for this operation, combined
        /// with bitwise OR. Possible flags are:
        ///   <list type="bullet">
        ///     <item><see cref="HamConst.HAM_WRITE_THROUGH" />
        ///         Immediately write modified pages to the disk. This
        ///         slows down all Database operations, but may save the
        ///         Database integrity in case of a system crash.</item><br />
        ///     <item><see cref="HamConst.HAM_USE_BTREE" />
        ///         Use a B+Tree for the index structure. Currently enabled
        ///         by default, but future releases of hamsterdb will offer
        ///         additional index structures, i.e. hash tables.</item><br />
        ///     <item><see cref="HamConst.HAM_DISABLE_VAR_KEYLEN" />
        ///         Do not allow the use of variable length keys. Inserting
        ///         a key, which is larger than the B+Tree index key size,
        ///         returns <see cref="HamConst.HAM_INV_KEYSIZE" />.</item><br />
        ///     <item><see cref="HamConst.HAM_IN_MEMORY_DB" />
        ///         Creates an In-Memory Database. No file will be created,
        ///         and the Database contents are lost after the Database
        ///         is closed. The <paramref name="fileName" /> parameter can
        ///         be null. Do <b>NOT</b> use in combination with
        ///         <see cref="HamConst.HAM_CACHE_STRICT" /> and do <b>NOT</b>
        ///         specify a cache size other than 0.</item><br />
        ///     <item><see cref="HamConst.HAM_RECORD_NUMBER" />
        ///         Enable duplicate keys for this Database. By default,
        ///         duplicate keys are disabled.</item><br />
        ///     <item><see cref="HamConst.HAM_ENABLE_DUPLICATES" />
        ///         Creates an "auto-increment" Database. Keys in Record
        ///         Number Databases are automatically assigned an incrementing
        ///         64bit value.</item><br />
        ///     <item><see cref="HamConst.HAM_SORT_DUPLICATES" />
        ///         Sort duplicate keys for this Database. Only allowed in
        ///         combination with HAM_ENABLE_DUPLICATES. A compare function
        ///         can be set with <see cref="Database.SetDuplicateCompareFunc"/>.
        ///         This flag is not persistent.</item><br />
        ///     <item><see cref="HamConst.HAM_DISABLE_MMAP" />
        ///         Do not use memory mapped files for I/O. By default,
        ///         hamsterdb checks if it can use mmap, since mmap is faster
        ///         than read/write. For performance reasons, this flag should
        ///         not be used.</item><br />
        ///     <item><see cref="HamConst.HAM_CACHE_STRICT" />
        ///         Do not allow the cache to grow larger than the size specified
        ///         with <see cref="HamConst.HAM_PARAM_CACHESIZE" />. If a
        ///         Database operation needs to resize the cache, it will
        ///         fail and return <see cref="HamConst.HAM_CACHE_FULL" />.
        ///         If the flag is not set, the cache is allowed to allocate
        ///         more pages than the maximum cache size, but only if it's
        ///         necessary and only for a short time.</item><br />
        ///     <item><see cref="HamConst.HAM_DISABLE_FREELIST_FLUSH" />
        ///         This flag is deprecated.</item>
        ///     <item><see cref="HamConst.HAM_LOCK_EXCLUSIVE" />
        ///         Place an exclusive lock on the file. Only one process
        ///         may hold an exclusive lock for a given file at a given
        ///         time. Deprecated - this is now the default.</item><br />
        ///     <item><see cref="HamConst.HAM_ENABLE_RECOVERY" />
        ///         Enables logging/recovery for this Database. Not allowed in
        ///         combination with <see cref="HamConst.HAM_IN_MEMORY_DB" />,
        ///         <see cref="HamConst.HAM_DISABLE_FREELIST_FLUSH" /> and
        ///         <see cref="HamConst.HAM_WRITE_THROUGH" />.</item><br />
        ///     <item><see cref="HamConst.HAM_ENABLE_TRANSACTIONS" />
        ///         Enables Transactions for this Database. This flag implies
        ///         <see cref="HamConst.HAM_ENABLE_RECOVERY" />.</item><br />
        ///   </list>
        /// </param>
        /// <param name="mode">File access rights for the new file. This is
        /// the <i>mode</i> parameter for creat(2). Ignored on
        /// Microsoft Windows.</param>
        /// <param name="parameters">An array of <see cref="Parameter" />
        /// structures. The following parameters are available:<br />
        ///   <list type="bullet">
        ///     <item><see cref="HamConst.HAM_PARAM_CACHESIZE" />
        ///         The size of the Database cache, in bytes. The default size
        ///         is defined in <i>src/config.h</i> as HAM_DEFAULT_CACHESIZE
        ///         - usually 2 MB.</item><br />
        ///     <item><see cref="HamConst.HAM_PARAM_PAGESIZE" />
        ///         The size of a file page, in bytes. It is recommended not
        ///         to change the default size. The default size depends on
        ///         hardware and operating system. Page sizes must be a
        ///         1024 or a multiple of 2048.</item><br />
        ///     <item><see cref="HamConst.HAM_PARAM_KEYSIZE" />
        ///         The size of the keys in the B+Tree index. The default size
        ///         is 21 bytes.</item><br />
        ///     <item><see cref="HamConst.HAM_PARAM_DATA_ACCESS_MODE" />
        ///         Gives a hint regarding data access patterns. The default
        ///         setting optimizes hamsterdb for random read/write access
        ///         (<see cref="HamConst.HAM_DAM_RANDOM_WRITE"/>).
        ///         Use <see cref="HamConst.HAM_DAM_SEQUENTIAL_INSERT"/> for
        ///         sequential inserts (this is automatically set for
        ///         record number Databases). This flag is not persistent.</item>
        ///     </list>
        /// </param>
        /// <exception cref="DatabaseException">
        ///   <list type="bullet">
        ///     <item><see cref="HamConst.HAM_INV_PARAMETER"/>
        ///         if an invalid combination of flags was specified</item>
        ///     <item><see cref="HamConst.HAM_IO_ERROR"/>
        ///         if the file could not be opened or reading/writing failed</item>
        ///     <item><see cref="HamConst.HAM_OUT_OF_MEMORY"/>
        ///         if memory could not be allocated</item>
        ///     <item><see cref="HamConst.HAM_INV_PAGESIZE"/>
        ///         if the page size is not a multiple of 1024</item>
        ///     <item><see cref="HamConst.HAM_INV_KEYSIZE"/>
        ///         if the key size is too large (at least 4 keys must
        ///         fit in a page)</item>
        ///     <item><see cref="HamConst.HAM_WOULD_BLOCK"/>
        ///         if another process has locked the file</item>
        ///   </list>
        /// </exception>
        public void Create(String fileName, int flags, int mode,
                Parameter[] parameters) {
            int st;
            if (parameters != null)
                parameters = AppendNullParameter(parameters);
            lock (this) {
                if (initialized == false) {
                    st = NativeMethods.NewDatabaseHandle(out handle);
                    if (st != 0)
                        throw new DatabaseException(st);
                    initialized = true;
                }
                st = NativeMethods.Create(handle, fileName, flags, mode,
                    parameters);
            }
            if (st != 0)
                throw new DatabaseException(st);
        }

        /// <summary>
        /// Opens an existing Database
        /// </summary>
        /// <remarks>
        /// This is an overloaded function for
        ///   Open(fileName, 0, null).
        /// </remarks>
        public void Open(String fileName) {
            Open(fileName, 0, null);
        }

        /// <summary>
        /// Opens an existing Database
        /// </summary>
        /// <remarks>
        /// This is an overloaded function for
        ///   Open(fileName, mode, null).
        /// </remarks>
        public void Open(String fileName, int flags) {
            Open(fileName, flags, null);
        }

        /// <summary>
        /// Opens an existing Database
        /// </summary>
        /// <remarks>
        /// This method wraps the native ham_open_ex function.
        /// </remarks>
        /// <param name="fileName">The file name of the Database file.</param>
        /// <param name="flags">Optional flags for this operation, combined
        /// with bitwise OR. Possible flags are:
        ///   <list type="bullet">
        ///     <item><see cref="HamConst.HAM_READ_ONLY" />
        ///         Opens the file for reading only. Operations which need
        ///         write access (i.e. Database.Insert)
        ///         will return <see cref="HamConst.HAM_DB_READ_ONLY" />.
        ///         </item><br />
        ///     <item><see cref="HamConst.HAM_WRITE_THROUGH" />
        ///         Immediately write modified pages to the disk. This
        ///         slows down all Database operations, but may save the
        ///         Database integrity in case of a system crash.</item><br />
        ///     <item><see cref="HamConst.HAM_DISABLE_VAR_KEYLEN" />
        ///         Do not allow the use of variable length keys. Inserting
        ///         a key, which is larger than the B+Tree index key size,
        ///         returns <see cref="HamConst.HAM_INV_KEYSIZE" />.</item><br />
        ///     <item><see cref="HamConst.HAM_DISABLE_MMAP" />
        ///         Do not use memory mapped files for I/O. By default,
        ///         hamsterdb checks if it can use mmap, since mmap is faster
        ///         than read/write. For performance reasons, this flag should
        ///         not be used.</item><br />
        ///     <item><see cref="HamConst.HAM_CACHE_STRICT" />
        ///         Do not allow the cache to grow larger than the size specified
        ///         with <see cref="HamConst.HAM_PARAM_CACHESIZE" />. If a
        ///         Database operation needs to resize the cache, it will
        ///         fail and return <see cref="HamConst.HAM_CACHE_FULL" />.
        ///         If the flag is not set, the cache is allowed to allocate
        ///         more pages than the maximum cache size, but only if it's
        ///         necessary and only for a short time.</item><br />
        ///     <item><see cref="HamConst.HAM_DISABLE_FREELIST_FLUSH" />
        ///         This flag is deprecated.</item><br />
        ///     <item><see cref="HamConst.HAM_LOCK_EXCLUSIVE" />
        ///         Place an exclusive lock on the file. Only one process
        ///         may hold an exclusive lock for a given file at a given
        ///         time. Deprecated - this is now the default.</item><br />
        ///     <item><see cref="HamConst.HAM_ENABLE_RECOVERY" />
        ///         Enables logging/recovery for this Database. Will return
        ///         <see cref="HamConst.HAM_NEED_RECOVERY" />, if the
        ///         Database is in an inconsistent state. Not allowed in
        ///         combination with <see cref="HamConst.HAM_IN_MEMORY_DB" />,
        ///         <see cref="HamConst.HAM_DISABLE_FREELIST_FLUSH" /> and
        ///         <see cref="HamConst.HAM_WRITE_THROUGH" />.</item><br />
        ///     <item><see cref="HamConst.HAM_ENABLE_TRANSACTIONS" />
        ///         Enables Transactions for this Database. This flag implies
        ///         <see cref="HamConst.HAM_ENABLE_RECOVERY" />.</item><br />
        ///     <item><see cref="HamConst.HAM_AUTO_RECOVERY" />
        ///         Automatically recover the Database, if necessary. This
        ///         flag imples <see cref="HamConst.HAM_ENABLE_RECOVERY" />.
        ///         </item><br />
        ///     <item><see cref="HamConst.HAM_SORT_DUPLICATES" />
        ///         Sort duplicate keys for this Database. Only allowed in
        ///         combination with HAM_ENABLE_DUPLICATES. A compare function
        ///         can be set with <see cref="Database.SetDuplicateCompareFunc"/>.
        ///         This flag is not persistent.</item><br />
        ///   </list>
        /// </param>
        /// <param name="parameters">An array of <see cref="Parameter" />
        /// structures. The following parameters are available:<br />
        ///   <list type="bullet">
        ///     <item><see cref="HamConst.HAM_PARAM_CACHESIZE" />
        ///         The size of the Database cache, in bytes. The default size
        ///         is defined in <i>src/config.h</i> as HAM_DEFAULT_CACHESIZE
        ///         - usually 2 MB.</item><br />
        ///     <item><see cref="HamConst.HAM_PARAM_DATA_ACCESS_MODE" />
        ///         Gives a hint regarding data access patterns. The default
        ///         setting optimizes hamsterdb for random read/write access
        ///         (<see cref="HamConst.HAM_DAM_RANDOM_WRITE"/>).
        ///         Use <see cref="HamConst.HAM_DAM_SEQUENTIAL_INSERT"/> for
        ///         sequential inserts (this is automatically set for
        ///         record number Databases). This flag is not persistent.</item>
        ///   </list>
        /// </param>
        /// <exception cref="DatabaseException">
        ///   <list type="bullet">
        ///     <item><see cref="HamConst.HAM_INV_PARAMETER"/>
        ///         if an invalid combination of flags was specified</item>
        ///     <item><see cref="HamConst.HAM_FILE_NOT_FOUND"/>
        ///         if the file does not exist</item>
        ///     <item><see cref="HamConst.HAM_IO_ERROR"/>
        ///         if the file could not be opened or reading/writing failed</item>
        ///     <item><see cref="HamConst.HAM_INV_FILE_VERSION"/>
        ///         if the Database version is not compatible with the library
        ///         version</item>
        ///     <item><see cref="HamConst.HAM_OUT_OF_MEMORY"/>
        ///         if memory could not be allocated</item>
        ///     <item><see cref="HamConst.HAM_WOULD_BLOCK"/>
        ///         if another process has locked the file</item>
        ///   </list>
        /// </exception>
        public void Open(String fileName, int flags,
                Parameter[] parameters) {
            int st;
            if (parameters != null)
                parameters = AppendNullParameter(parameters);
            lock (this) {
                if (initialized == false) {
                    st = NativeMethods.NewDatabaseHandle(out handle);
                    if (st != 0)
                        throw new DatabaseException(st);
                    initialized = true;
                }
                st = NativeMethods.Open(handle, fileName, flags, parameters);
            }
            if (st != 0)
                throw new DatabaseException(st);
        }

        private int MyCompareFunc(IntPtr dbhandle,
                IntPtr lhs, int lhs_length,
                IntPtr rhs, int rhs_length) {
            byte[] alhs = new byte[lhs_length];
            byte[] arhs = new byte[rhs_length];
            Marshal.Copy(lhs, alhs, 0, lhs_length);
            Marshal.Copy(rhs, arhs, 0, rhs_length);
            return CompareFoo(alhs, arhs);
        }

        /// <summary>
        /// Sets the comparison function
        /// </summary>
        /// <remarks>
        /// This method wraps the native ham_set_compare_func function.<br />
        /// <br />
        /// The <see cref="CompareFunc" /> delegate compares two index keys.
        /// It returns -1 if the first key is smaller, +1 if the second key
        /// is smaller or 0 if both keys are equal.<br />
        /// <br />
        /// If <paramref name="foo"/> is null, hamsterdb will use the default
        /// compare function (which is based on memcmp(3)).<br />
        /// <br />
        /// Note that if you use a custom compare function routine in combination
        /// with extended keys, it might be useful to disable the prefix
        /// comparison, which is based on memcmp(3). See
        /// <see cref="SetPrefixCompareFunc" /> for details.
        /// </remarks>
        /// <param name="foo">The compare delegate, or null</param>
        public void SetCompareFunc(CompareFunc foo) {
            int st;
            lock (this) {
                st = NativeMethods.SetCompareFunc(handle, pinnedCompareFunc);
            }
            if (st != 0)
                throw new DatabaseException(st);
            CompareFoo=foo;
        }

        private CompareFunc CompareFoo;

        private int MyPrefixCompareFunc(IntPtr dbhandle,
                byte[] lhs, int lhs_length, int lhs_real_length,
                byte[] rhs, int rhs_length, int rhs_real_length) {
            return PrefixCompareFoo(lhs, lhs_real_length, rhs, rhs_real_length);
        }

        /// <summary>
        /// Sets the prefix comparison function
        /// </summary>
        /// <remarks>
        /// This method wraps the native ham_set_prefix_compare_func function.<br />
        /// <br />
        /// The <see cref="PrefixCompareFunc" /> delegate is called when an index
        /// uses keys with variable length, and at least one of the keys
        /// is loaded only partially.<br />
        /// <br />
        /// If <paramref name="foo"/> is null, hamsterdb will use the default
        /// prefix compare function (which is based on memcmp(3)).<br />
        /// </remarks>
        /// <param name="foo">The prefix compare delegate, or null</param>
        public void SetPrefixCompareFunc(PrefixCompareFunc foo) {
            int st;
            lock (this) {
                st = NativeMethods.SetPrefixCompareFunc(handle,
                    pinnedPrefixCompareFunc);
            }
            if (st != 0)
                throw new DatabaseException(st);
            PrefixCompareFoo = foo;
        }

        private PrefixCompareFunc PrefixCompareFoo;

        private int MyDuplicateCompareFunc(IntPtr dbhandle,
                byte[] lhs, int lhs_length,
                byte[] rhs, int rhs_length)
        {
            return DuplicateCompareFoo(lhs, rhs);
        }

        /// <summary>
        /// Sets the duplicate comparison function
        /// </summary>
        /// <remarks>
        /// This method wraps the native ham_set_duplicate_compare_func function.<br />
        /// <br />
        /// The <see cref="CompareFunc" /> delegate compares two records.
        /// It returns -1 if the first record is smaller, +1 if the second record
        /// is smaller or 0 if both records are equal.<br />
        /// <br />
        /// If <paramref name="foo"/> is null, hamsterdb will use the default
        /// compare function (which is based on memcmp(3)).<br />
        /// <br />
        /// Note that duplicate comparison has to be enabled with
        /// <see cref="HamConst.HAM_SORT_DUPLICATES"/>.
        /// </remarks>
        /// <param name="foo">The duplicate compare delegate, or null</param>
        public void SetDuplicateCompareFunc(DuplicateCompareFunc foo)
        {
            int st;
            lock (this)
            {
                st = NativeMethods.SetDuplicateCompareFunc(handle,
                    pinnedDupeCompareFunc);
            }
            if (st != 0)
                throw new DatabaseException(st);
            DuplicateCompareFoo = foo;
        }

        private DuplicateCompareFunc DuplicateCompareFoo;

        /// <summary>
        /// Returns the last error code
        /// </summary>
        /// <remarks>
        /// This method wraps the native ham_get_error function.
        /// </remarks>
        /// <returns>The error code of the last operation</returns>
        public int GetLastError() {
            lock (this) {
                return NativeMethods.GetLastError(handle);
            }
        }

        /// <summary>
        /// Enables zlib compression for all inserted records.
        /// </summary>
        public void EnableCompression() {
            EnableCompression(0);
        }

        /// <summary>
        /// Enables zlib compression for all inserted records.
        /// </summary>
        /// <remarks>
        /// This method wraps the native ham_enable_compression function.<br />
        /// <br/>
        /// The compression will be active till <see cref="Database.Close()" /> is
        /// called. This method should be called immediately after
        /// Database.Create or Database.Open.<br />
        /// <br />
        /// Note that zlib usually has an overhead and often is not effective if
        /// the records are small (i.e. less than 128byte), but this highly
        /// depends on the data that is inserted.
        /// </remarks>
        /// <param name="level">The zlib compression level; set to 0 for the zlib
        /// default, 1 for best speed and 9 for minimum size</param>
        /// <exception cref="DatabaseException">
        ///   <list type="bullet">
        ///     <item><see cref="HamConst.HAM_INV_PARAMETER"/>
        ///         if compression level is not between 0 and 9</item>
        ///     <item><see cref="HamConst.HAM_NOT_IMPLEMENTED"/>
        ///         if hamsterdb was built without support for compression</item>
        ///   </list>
        /// </exception>
        public void EnableCompression(int level) {
            int st;
            lock (this) {
                st = NativeMethods.EnableCompression(handle, level);
            }
            if (st != 0)
                throw new DatabaseException(st);
        }

        /// <summary>
        /// Searches an item in the Database, returns the record
        /// </summary>
        public byte[] Find(byte[] key)
        {
            return Find(null, key);
        }

        /// <summary>
        /// Searches an item in the Database, returns the record
        /// </summary>
        /// <remarks>
        /// This method wraps the native ham_find function.<br />
        /// <br />
        /// This function searches the Database for a key. If the key
        /// is found, the method will return the record of this item.
        /// <br />
        /// Database.Find can not search for duplicate keys. If the
        /// key has multiple duplicates, only the first duplicate is returned.
        /// </remarks>
        /// <param name="txn">The optional Transaction</param>
        /// <param name="key">The key of the item</param>
        /// <returns>The record of the item</returns>
        /// <exception cref="DatabaseException">
        ///   <list type="bullet">
        ///     <item><see cref="HamConst.HAM_KEY_NOT_FOUND"/>
        ///         if the item was not found</item>
        ///   </list>
        /// </exception>
        public byte[] Find(Transaction txn, byte[] key) {
            byte[] r;
            lock (this) {
                r = NativeMethods.Find(handle, txn != null ? txn.Handle : IntPtr.Zero, key, 0);
            }
            if (r == null)
                throw new DatabaseException(GetLastError());
            return r;
        }

        /// <summary>
        /// Inserts a Database item
        /// </summary>
        /// <remarks>
        /// This is an overloaded function for
        ///   Database.Insert(txn, key, record, 0).
        /// </remarks>
        public void Insert(Transaction txn, byte[] key, byte[] record)
        {
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
        public void Insert(byte[] key, byte[] record, int flags)
        {
            Insert(null, key, record, flags);
        }

        /// <summary>
        /// Inserts a Database Item
        /// </summary>
        /// <remarks>
        /// This method wraps the native ham_insert function.
        /// <br />
        /// This function inserts a key/record pair as a new Database item.
        /// <br />
        /// If the key already exists in the Database, error code
        /// <see cref="HamConst.HAM_DUPLICATE_KEY" /> is thrown.
        /// <br />
        /// If you wish to overwrite an existing entry specify the flag
        /// <see cref="HamConst.HAM_OVERWRITE"/>
        /// <br />
        /// If you wish to insert a duplicate key specify the flag
        /// <see cref="HamConst.HAM_DUPLICATE" />. (Note that
        /// the Database has to be created with the flag
        /// <see cref="HamConst.HAM_ENABLE_DUPLICATES" /> in order
        /// to use duplicate keys.)
        /// The duplicate key is inserted after all other duplicate keys (see
        /// <see cref="HamConst.HAM_DUPLICATE_INSERT_LAST" />).
        /// </remarks>
        /// <param name="txn">An optional Transaction object</param>
        /// <param name="key">The key of the new item</param>
        /// <param name="record">The record of the new item</param>
        /// <param name="flags">Optional flags for this operation. Possible
        /// flags are:
        /// <list type="bullet">
        ///   <item><see cref="HamConst.HAM_OVERWRITE"/>
        ///         If the key already exists, the record is overwritten.
        ///         Otherwise, the key is inserted.</item>
        ///   <item><see cref="HamConst.HAM_DUPLICATE"/>
        ///         If the key already exists, a duplicate key is inserted.
        ///         The key is inserted before the already existing duplicates.
        ///         </item>
        /// </list></param>
        /// <exception cref="DatabaseException">
        ///   <list type="bullet">
        ///     <item><see cref="HamConst.HAM_INV_PARAMETER"/>
        ///         if the flags HamConst.HAM_DUPLICATE <b>AND</b>
        ///         HamConst.HAM_OVERWRITE were specified, or if
        ///         HamConst.HAM_DUPLICATE was specified but the Database
        ///         was not created with HamConst.HAM_ENABLE_DUPLICATES</item>
        ///     <item><see cref="HamConst.HAM_DB_READ_ONLY"/>
        ///         if you tried to insert a key in a read-only Database</item>
        ///     <item><see cref="HamConst.HAM_INV_KEYSIZE"/>
        ///         if key size is larger than the key size parameter
        ///         specified for Database.Create, and variable
        ///         length keys are disabled (see
        ///         <see cref="HamConst.HAM_DISABLE_VAR_KEYLEN" />).</item>
        ///   </list>
        /// </exception>
        public void Insert(Transaction txn, byte[] key, byte[] record, int flags) {
            int st;
            lock (this) {
                st = NativeMethods.Insert(handle, txn != null ? txn.Handle : IntPtr.Zero,
                                            key, record, flags);
            }
            if (st!=0)
                throw new DatabaseException(st);
        }

        /// <summary>
        /// Erases a Database Item
        /// </summary>
        public void Erase(byte[] key)
        {
            Erase(null, key);
        }

        /// <summary>
        /// Erases a Database Item
        /// </summary>
        /// <remarks>
        /// This method wraps the native ham_erase function.
        /// <br />
        /// This function erases a Database item. If the item with the
        /// specified key does not exist in the Database, error code
        /// <see cref="HamConst.HAM_KEY_NOT_FOUND" /> is thrown.
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
        ///     <item><see cref="HamConst.HAM_KEY_NOT_FOUND"/>
        ///         if the key was not found</item>
        ///     <item><see cref="HamConst.HAM_DB_READ_ONLY"/>
        ///         if you tried to insert a key in a read-only Database</item>
        ///   </list>
        /// </exception>
        public void Erase(Transaction txn, byte[] key) {
            int st;
            lock (this) {
                st = NativeMethods.Erase(handle, txn != null ? txn.Handle : IntPtr.Zero, key, 0);
            }
            if (st != 0)
                throw new DatabaseException(st);
        }

        /// <summary>
        /// Flushes the Database
        /// </summary>
        /// <remarks>
        /// This method wraps the native ham_flush function.
        /// <br />
        /// This function flushes the Database cache and writes the whole
        /// file to disk. If this Database was opened in an Environment,
        /// all other Databases of this Environment are flushed as well.
        /// <br />
        /// Since In-Memory Databases do not have a file on disk, the
        /// function will have no effect and will return successfully.
        /// </remarks>
        public void Flush() {
            int st;
            lock (this) {
                st = NativeMethods.Flush(handle, 0);
            }
            if (st != 0)
                throw new DatabaseException(st);
        }

        /// <summary>
        /// Returns the Environment handle of this Database
        /// </summary>
        /// <remarks>
        /// This method wraps the native ham_get_env function.
        /// </remarks>
        public Environment GetEnvironment()
        {
            lock (this)
            {
                IntPtr h = NativeMethods.GetEnv(handle);
                return new Environment(h);
            }
        }

        /// <summary>
        /// Returns the number of keys in this Database
        /// </summary>
        public Int64 GetKeyCount(int flags)
        {
            return GetKeyCount(null, flags);
        }

        /// <summary>
        /// Returns the number of keys in this Database
        /// </summary>
        public Int64 GetKeyCount(Transaction txn)
        {
            return GetKeyCount(txn, 0);
        }

        /// <summary>
        /// Returns the number of keys in this Database
        /// </summary>
        public Int64 GetKeyCount()
        {
            return GetKeyCount(0);
        }

        /// <summary>
        /// Returns the number of keys in this Database
        /// </summary>
        /// <remarks>
        /// This method wraps the native ham_get_key_count function.
        /// <br />
        /// You can specify HAM_SKIP_DUPLICATES if you do now want
        /// to include any duplicates in the count; if all you're after is
        /// a quick estimate, you can specify the flag HAM_FAST_ESTIMATE
        /// (which implies HAM_SKIP_DUPLICATES), which will improve the
        /// execution speed of this operation significantly.
        /// </remarks>
        public Int64 GetKeyCount(Transaction txn, int flags)
        {
            int st;
            Int64 count=0;
            lock (this)
            {
                st = NativeMethods.GetKeyCount(handle, txn != null ? txn.Handle : IntPtr.Zero,
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
        /// This is an overloaded function for
        ///   Database.Close(0).
        /// </remarks>
        public void Close() {
            Close(0);
        }

        /// <summary>
        /// Closes the Database
        /// </summary>
        /// <remarks>
        /// This method wraps the native ham_close function.
        /// <br />
        /// Before closing the Database, the cache is flushed to Disk
        /// <see cref="Database.Flush" />.
        /// </remarks>
        public void Close(int flags) {
            if (initialized == false)
                return;
            if (0 != (flags & HamConst.HAM_AUTO_CLEANUP)) {
                while (cursors.Count > 0) {
                    Cursor c = cursors[0];
                    if (c != null)
                        c.Close();
                    RemoveCursor(c);
                }
                cursors.Clear();
            }
            lock (this) {
                int st = NativeMethods.Close(handle, flags);
                if (st!=0)
                    throw new DatabaseException(st);
                NativeMethods.DeleteDatabaseHandle(handle);
                handle = IntPtr.Zero;
                initialized = false;
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
                Close(HamConst.HAM_AUTO_CLEANUP);
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
        public void AddCursor(Cursor c)
        {
            cursors.Add(c);
        }

        /// <summary>
        /// Removes a Cursor from the Cursor list
        /// </summary>
        public void RemoveCursor(Cursor c)
        {
            cursors.Remove(c);
        }

        private static Parameter[] AppendNullParameter(Parameter[] parameters) {
            Parameter[] newArray = new Parameter[parameters.GetLength(0)+1];
            for (int i = 0; i < parameters.GetLength(0); i++)
                newArray[i] = parameters[i];
            return newArray;
        }

        private IntPtr handle;
        private bool initialized;
        private List<Cursor> cursors;
        private NativeMethods.CompareFunc pinnedCompareFunc;
        private NativeMethods.PrefixCompareFunc pinnedPrefixCompareFunc;
        private NativeMethods.DuplicateCompareFunc pinnedDupeCompareFunc;
    }
}
