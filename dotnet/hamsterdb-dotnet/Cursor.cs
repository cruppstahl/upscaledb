/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Hamster
{
  using System;

  /// <summary>
  /// A Database Cursor class
  /// </summary>
  public class Cursor : IDisposable
  {
    /// <summary>
    /// Constructor which creates a new Cursor
    /// </summary>
    /// <param name="db">The Database of this Cursor</param>
    /// <param name="handle">The handle of this Cursor</param>
    internal Cursor(Database db, IntPtr handle) {
      this.db = db;
      this.handle = handle;
    }

    /// <summary>
    /// Constructor which creates a new Cursor
    /// </summary>
    /// <param name="db">The Database of this Cursor</param>
    public Cursor(Database db) {
      Create(db);
    }

    /// <summary>
    /// Constructor which creates a new Cursor in a Transaction
    /// </summary>
    public Cursor(Database db, Transaction txn) {
      Create(db, txn);
    }

    /// <summary>
    /// Destructor; closes the Cursor
    /// </summary>
    ~Cursor() {
      Dispose(false);
    }

    /// <summary>
    /// Creates a new Cursor
    /// </summary>
    ///
    /// <see cref="Cursor.Create(Database, Transaction)" />
    public void Create(Database db) {
      Create(db, null);
    }

    /// <summary>
    /// Creates a new Cursor in a Transaction
    /// </summary>
    /// <remarks>
    /// This method wraps the native ham_cursor_create function.
    /// <br />
    /// Creates a new Database Cursor. Cursors can be used to traverse
    /// the Database from start to end or vice versa. Cursors can also
    /// be used to insert, delete or search Database items.
    ///
    /// A newly created Cursor does not point to any item in the Database.
    ///
    /// The application should close all Database Cursors before closing
    /// the Database.
    /// </remarks>
    /// <param name="db">The Database object</param>
    /// <param name="txn">The optional Transaction</param>
    /// <exception cref="DatabaseException">
    ///   <list type="bullet">
    ///   <item><see cref="HamConst.HAM_OUT_OF_MEMORY"/>
    ///     if the new structure could not be allocated</item>
    ///   </list>
    /// </exception>
    public void Create(Database db, Transaction txn) {
      this.db = db;
      lock (this.db) {
        int st = NativeMethods.CursorCreate(out handle, db.Handle,
                txn != null ? txn.Handle : IntPtr.Zero, 0);
        if (st != 0)
          throw new DatabaseException(st);
        db.AddCursor(this);
      }
    }

    /// <summary>
    /// Clones a Database Cursor
    /// </summary>
    /// <remarks>
    /// This method wraps the native ham_cursor_clone function.
    /// <br />
    /// Clones an existing Cursor. The new Cursor will point to exactly the
    /// same item as the old Cursor. If the old Cursor did not point
    /// to any item, so will the new Cursor.
    ///
    /// If the old Cursor is bound to a Transaction, then the new
    /// Cursor will also be bound to this Transaction.
    /// </remarks>
    /// <returns>The new Cursor object</returns>
    /// <exception cref="DatabaseException">
    ///   <list type="bullet">
    ///   <item><see cref="HamConst.HAM_OUT_OF_MEMORY"/>
    ///     if the new structure could not be allocated</item>
    ///   </list>
    /// </exception>
    public Cursor Clone() {
      IntPtr newHandle = new IntPtr(0);
      lock (this.db) {
        int st = NativeMethods.CursorClone(handle, out newHandle);
        if (st != 0)
          throw new DatabaseException(st);
        Cursor c = new Cursor(db, newHandle);
        db.AddCursor(c);
        return c;
      }
    }

    /// <summary>
    /// Moves the Cursor to the direction specified in the flags
    /// </summary>
    /// <remarks>
    /// This method wraps the native ham_cursor_move function.
    ///
    /// Moves the Cursor. Use the flags to specify the direction.
    /// After the move, use Cursor.GetKey and Cursor.GetRecord to
    /// retrieve key and record of the item.
    ///
    /// If the direction is not specified, the Cursor will not move.
    /// </remarks>
    /// <param name="flags">The direction for the move. If no direction
    /// is specified, the Cursor will remain on the current position.
    /// Possible flags are:
    ///   <list type="bullet">
    ///   <item><see cref="HamConst.HAM_CURSOR_FIRST" /> positions
    ///     the Cursor to the first item in the Database</item>
    ///   <item><see cref="HamConst.HAM_CURSOR_LAST" /> positions
    ///     the Cursor to the last item in the Database</item>
    ///   <item><see cref="HamConst.HAM_CURSOR_NEXT" /> positions
    ///     the Cursor to the next item in the Database; if the Cursor
    ///     does not point to any item, the function behaves as if
    ///     direction was <see cref="HamConst.HAM_CURSOR_FIRST"/>.</item>
    ///   <item><see cref="HamConst.HAM_CURSOR_PREVIOUS" /> positions
    ///     the Cursor to the previous item in the Database; if the Cursor
    ///     does not point to any item, the function behaves as if
    ///     direction was <see cref="HamConst.HAM_CURSOR_LAST"/>.</item>
    ///   <item><see cref="HamConst.HAM_SKIP_DUPLICATES" /> skips
    ///     duplicate keys of the current key. Not allowed in combination
    ///     with <see cref="HamConst.HAM_ONLY_DUPLICATES" />.</item>
    ///   <item><see cref="HamConst.HAM_ONLY_DUPLICATES" /> only
    ///     moves through duplicate keys of the current key. Not allowed
    ///     in combination with
    ///     <see cref="HamConst.HAM_SKIP_DUPLICATES" />.</item>
    ///   </list>
    /// </param>
    /// <exception cref="DatabaseException">
    ///   <list type="bullet">
    ///   <item><see cref="HamConst.HAM_KEY_NOT_FOUND"/>
    ///     if the Cursor points to the first (or last) item, and a
    ///     move to the previous (or next) item was requested</item>
    ///   </list>
    /// </exception>
    public void Move(int flags) {
      int st;
      lock (db) {
        st = NativeMethods.CursorMove(handle, flags);
      }
      if (st != 0)
        throw new DatabaseException(st);
    }

    /// <summary>
    /// Moves the Cursor to the first Database element
    /// </summary>
    /// <see cref="Cursor.Move" />
    public void MoveFirst() {
      Move(HamConst.HAM_CURSOR_FIRST);
    }

    /// <summary>
    /// Moves the Cursor to the last Database element
    /// </summary>
    /// <see cref="Cursor.Move" />
    public void MoveLast() {
      Move(HamConst.HAM_CURSOR_LAST);
    }

    /// <summary>
    /// Moves the Cursor to the next Database element
    /// </summary>
    /// <see cref="Cursor.Move" />
    public void MoveNext() {
      Move(HamConst.HAM_CURSOR_NEXT);
    }

    /// <summary>
    /// Moves the Cursor to the next Database element
    /// </summary>
    /// <see cref="Cursor.Move" />
    /// <param name="flags">Additional flags for the movement</param>
    public void MoveNext(int flags) {
      Move(HamConst.HAM_CURSOR_NEXT | flags);
    }

    /// <summary>
    /// Moves the Cursor to the previous Database element
    /// </summary>
    public void MovePrevious() {
      Move(HamConst.HAM_CURSOR_PREVIOUS);
    }

    /// <summary>
    /// Moves the Cursor to the previous Database element
    /// </summary>
    /// <see cref="Cursor.Move" />
    /// <param name="flags">Additional flags for the movement</param>
    public void MovePrevious(int flags) {
      Move(HamConst.HAM_CURSOR_PREVIOUS | flags);
    }

    /// <summary>
    /// Retrieves the Key of the current item
    /// </summary>
    /// <remarks>
    /// This method wraps the native ham_cursor_move function.
    /// <br />
    /// Returns the key of the current Database item. Throws
    /// <see cref="HamConst.HAM_CURSOR_IS_NIL" /> if the Cursor does
    /// not point to any item.
    /// </remarks>
    /// <returns>The key of the current item</returns>
    /// <exception cref="DatabaseException">
    ///   <list type="bullet">
    ///   <item><see cref="HamConst.HAM_CURSOR_IS_NIL"/>
    ///     if the Cursor does not point to any item</item>
    ///   </list>
    /// </exception>
    public byte[] GetKey() {
      byte[] ret;
      lock (db) {
        ret = NativeMethods.CursorGetKey(handle, 0);
      }
      if (ret == null)
        throw new DatabaseException(db.GetLastError());
      return ret;
    }

    /// <summary>
    /// Retrieves the Record of the current item
    /// </summary>
    /// <remarks>
    /// This method wraps the native ham_cursor_move function.
    /// <br />
    /// Returns the record of the current Database item. Throws
    /// <see cref="HamConst.HAM_CURSOR_IS_NIL" /> if the Cursor does
    /// not point to any item.
    /// </remarks>
    /// <returns>The record of the current item</returns>
    /// <exception cref="DatabaseException">
    ///   <list type="bullet">
    ///   <item><see cref="HamConst.HAM_CURSOR_IS_NIL"/>
    ///     if the Cursor does not point to any item</item>
    ///   </list>
    /// </exception>
    public byte[] GetRecord() {
      byte[] ret;
      lock (db) {
        ret = NativeMethods.CursorGetRecord(handle, 0);
      }
      if (ret == null)
        throw new DatabaseException(db.GetLastError());
      return ret;
    }

    /// <summary>
    /// Overwrites the record of the current item
    /// </summary>
    /// <remarks>
    /// This method wraps the native ham_cursor_overwrite function.
    /// <br />
    /// This function overwrites the record of the current item.
    /// </remarks>
    /// <exception cref="DatabaseException">
    ///   <list type="bullet">
    ///   <item><see cref="HamConst.HAM_CURSOR_IS_NIL"/>
    ///     if the Cursor does not point to any item</item>
    ///   </list>
    /// </exception>
    public void Overwrite(byte[] record) {
      int st;
      lock (db) {
        st = NativeMethods.CursorOverwrite(handle, record, 0);
      }
      if (st != 0)
        throw new DatabaseException(st);
    }

    /// <summary>
    /// Searches a key and points the Cursor to this key;
    /// returns the record of the key
    /// </summary>
    public byte[] Find(byte[] key)
    {
        return Find(key, 0);
    }

    /// <summary>
    /// Searches a key and points the Cursor to this key
    /// </summary>
    /// <remarks>
    /// This method wraps the native ham_cursor_find function.
    /// <br />
    /// Searches for an item in the Database and points the Cursor to this
    /// item. If the item could not be found, the Cursor is not modified.
    /// <br />
    /// If the key has multiple duplicates, the Cursor is positioned
    /// on the first duplicate.
    /// </remarks>
    /// <param name="key">The key to search for</param>
    /// <param name="flags">The flags, can be zero</param>
    /// <exception cref="DatabaseException">
    ///   <list type="bullet">
    ///   <item><see cref="HamConst.HAM_KEY_NOT_FOUND"/>
    ///     if the requested key was not found</item>
    ///   </list>
    /// </exception>
    public byte[] Find(ref byte[] key, int flags) {
      int st;
      byte[] record = null;
      lock (db) {
        st = NativeMethods.CursorFind(handle, ref key, ref record, flags);
      }
      if (st != 0)
        throw new DatabaseException(st);
      return record;
    }

    public byte[] Find(byte[] key, int flags)
    {
        return Find(ref key, flags);
    }

    /// <summary>
    /// Searches a key and points the Cursor to this key
    /// </summary>
    /// <remarks>
    /// This method wraps the native ham_cursor_find function.
    /// <br />
    /// Searches for an item in the Database and points the Cursor to this
    /// item. If the item could not be found, the Cursor is not modified.
    /// <br />
    /// If the key has multiple duplicates, the Cursor is positioned
    /// on the first duplicate.
    /// </remarks>
    /// <param name="key">The key to search for</param>
    /// <param name="flags">The flags, can be zero</param>
    /// <param name="ok">Set to true if the key was found, false otherwise</param>
    public byte[] TryFind(ref byte[] key, int flags, out bool ok)
    {
        int st;
        byte[] record = null;
        lock (db)
        {
            st = NativeMethods.CursorFind(handle, ref key, ref record, flags);
        }
        ok = (st == 0);
        return record;
    }

    /// <summary>
    /// Searches a key and points the Cursor to this key
    /// </summary>
    /// <remarks>
    /// This method wraps the native ham_cursor_find function.
    /// <br />
    /// Searches for an item in the Database and points the Cursor to this
    /// item. If the item could not be found, the Cursor is not modified.
    /// <br />
    /// If the key has multiple duplicates, the Cursor is positioned
    /// on the first duplicate.
    /// </remarks>
    /// <param name="key">The key to search for</param>
    /// <param name="flags">The flags, can be zero</param>
    /// <param name="ok">Set to true if the key was found, false otherwise</param>
    public byte[] TryFind(byte[] key, int flags, out bool ok)
    {
        return TryFind(ref key, flags, out ok);
    }

    /// <summary>
    /// Searches a key and points the Cursor to this key
    /// </summary>
    /// <remarks>
    /// This method wraps the native ham_cursor_find function.
    /// <br />
    /// Searches for an item in the Database and points the Cursor to this
    /// item. If the item could not be found, the Cursor is not modified.
    /// <br />
    /// If the key has multiple duplicates, the Cursor is positioned
    /// on the first duplicate.
    /// </remarks>
    /// <param name="key">The key to search for</param>
    /// <param name="ok">Set to true if the key was found, false otherwise</param>
    public byte[] TryFind(byte[] key, out bool ok)
    {
        return TryFind(ref key, 0, out ok);
    }

    /// <summary>
    /// Inserts a Database item and points the Cursor to the new item
    /// </summary>
    /// <remarks>
    /// This is an overloaded function for Insert(key, record, 0).
    /// </remarks>
    /// <param name="key">The key of the new item</param>
    /// <param name="record">The record of the new item</param>
    public void Insert(byte[] key, byte[] record) {
      Insert(key, record, 0);
    }

    /// <summary>
    /// Inserts a Database item and points the Cursor to the new item
    /// </summary>
    /// <remarks>
    /// This method wraps the native ham_cursor_insert function.
    /// <br />
    /// This function inserts a key/record pair as a new Database item.
    /// If the key already exists in the Database, error
    /// <see cref="HamConst.HAM_DUPLICATE_KEY" /> is thrown.
    /// <br />
    /// If you wish to overwrite an existing entry specify the flag
    /// <see cref="HamConst.HAM_OVERWRITE"/>
    /// <br />
    /// If you wish to insert a duplicate key specify the flag
    /// <see cref="HamConst.HAM_DUPLICATE" />. (Note that
    /// the Database has to be created with the flag
    /// <see cref="HamConst.HAM_ENABLE_DUPLICATE_KEYS" /> in order
    /// to use duplicate keys.)
    /// By default, the duplicate key is inserted after all other duplicate
    /// keys (<see cref="HamConst.HAM_DUPLICATE_INSERT_LAST"/>). This
    /// behaviour can be overwritten by specifying
    /// <see cref="HamConst.HAM_DUPLICATE_INSERT_FIRST"/>,
    /// <see cref="HamConst.HAM_DUPLICATE_INSERT_BEFORE"/> or
    /// <see cref="HamConst.HAM_DUPLICATE_INSERT_AFTER"/>.
    /// <br />
    /// Specify the flag <see cref="HamConst.HAM_HINT_APPEND"/> if you
    /// insert sequential data and the current key is higher than any
    /// other key in this Database. In this case hamsterdb will optimize
    /// the insert algorithm. hamsterdb will verify that this key is
    /// the highest; if not, it will perform a normal insert. This is
    /// the default for Record Number Databases.
    /// <br />
    /// Specify the flag <see cref="HamConst.HAM_HINT_PREPEND"/> if you
    /// insert sequential data and the current key is lower than any
    /// other key in this Database. In this case hamsterdb will optimize
    /// the insert algorithm. hamsterdb will verify that this key is
    /// the lowest; if not, it will perform a normal insert.
    /// <br />
    /// After inserting, the Cursor will point to the new item. If inserting
    /// the item failed, the Cursor is not modified.
    /// </remarks>
    /// <param name="key">The key of the new item</param>
    /// <param name="record">The record of the new item</param>
    /// <param name="flags">Optional flags for this operation, combined
    /// with bitwise OR. Possible flags are:
    /// <list type="bullet">
    ///   <item><see cref="HamConst.HAM_OVERWRITE"/>
    ///     If the key already exists, the record is overwritten.
    ///     Otherwise, the key is inserted.</item>
    ///   <item><see cref="HamConst.HAM_DUPLICATE"/>
    ///     If the key already exists, a duplicate key is inserted.
    ///     The key is inserted after the already existing duplicates.
    ///     Same as <see cref="HamConst.HAM_DUPLICATE_INSERT_LAST" />.
    ///     </item>
    ///   <item><see cref="HamConst.HAM_DUPLICATE_INSERT_BEFORE" />
    ///     If the key already exists, a duplicate key is inserted before
    ///     the duplicate pointed to by this Cursor.</item>
    ///   <item><see cref="HamConst.HAM_DUPLICATE_INSERT_AFTER" />
    ///     If the key already exists, a duplicate key is inserted after
    ///     the duplicate pointed to by this Cursor.</item>
    ///   <item><see cref="HamConst.HAM_DUPLICATE_INSERT_FIRST" />
    ///     If the key already exists, a duplicate key is inserted as
    ///     the first duplicate of the current key.</item>
    ///   <item><see cref="HamConst.HAM_DUPLICATE_INSERT_LAST" />
    ///     If the key already exists, a duplicate key is inserted as
    ///     the last duplicate of the current key.</item>
    /// </list></param>
    /// <exception cref="DatabaseException">
    ///   <list type="bullet">
    ///   <item><see cref="HamConst.HAM_INV_PARAMETER"/>
    ///     if the flags HamConst.HAM_DUPLICATE <b>AND</b>
    ///     HamConst.HAM_OVERWRITE were specified, or if
    ///     HamConst.HAM_DUPLICATE was specified but the Database
    ///     was not created with HamConst.HAM_ENABLE_DUPLICATE_KEYS</item>
    ///   <item><see cref="HamConst.HAM_WRITE_PROTECTED"/>
    ///     if you tried to insert a key in a read-only Database</item>
    ///   <item><see cref="HamConst.HAM_INV_KEYSIZE"/>
    ///     if key size is different than than the key size parameter
    ///     specified for Database.Create.</item>
    ///   </list>
    /// </exception>
    public void Insert(byte[] key, byte[] record, int flags) {
      int st;
      lock (db) {
        st = NativeMethods.CursorInsert(handle, key, record, flags);
      }
      if (st != 0)
        throw new DatabaseException(st);
    }

    /// <summary>
    /// Erases the current key
    /// </summary>
    /// <remarks>
    /// This method wraps the native ham_cursor_erase function.
    /// <br />
    /// Erases a key from the Database. If the erase was successfull, the
    /// Cursor is invalidated, and does no longer point to any item.
    /// In case of an error, the Cursor is not modified.
    /// <br />
    /// If the Database was opened with the flag
    /// <see cref="HamConst.HAM_ENABLE_DUPLICATE_KEYS" />, this function erases
    /// only the duplicate item to which the Cursor refers.
    /// </remarks>
    /// <exception cref="DatabaseException">
    ///   <list type="bullet">
    ///   <item><see cref="HamConst.HAM_CURSOR_IS_NIL"/>
    ///     if the Cursor does not point to any item</item>
    ///   <item><see cref="HamConst.HAM_WRITE_PROTECTED"/>
    ///     if you tried to erase a key from a read-only Database</item>
    ///   </list>
    /// </exception>
    public void Erase() {
      int st;
      lock (db) {
        st = NativeMethods.CursorErase(handle, 0);
      }
      if (st != 0)
        throw new DatabaseException(st);
    }

    /// <summary>
    /// Returns the number of duplicate keys
    /// </summary>
    /// <remarks>
    /// This method wraps the native ham_cursor_get_duplicate_count function.
    /// <br />
    /// Returns the number of duplicate keys of the item to which the
    /// Cursor currently refers.
    /// <br />
    /// Returns 1 if the key has no duplicates.
    /// </remarks>
    /// <returns>The number of duplicate keys of the current item</returns>
    /// <exception cref="DatabaseException">
    ///   <list type="bullet">
    ///   <item><see cref="HamConst.HAM_CURSOR_IS_NIL"/>
    ///     if the Cursor does not point to any item</item>
    ///   </list>
    /// </exception>
    public int GetDuplicateCount() {
      int count, st;
      lock (db) {
        st = NativeMethods.CursorGetDuplicateCount(handle, out count, 0);
      }
      if (st != 0)
        throw new DatabaseException(st);
      return count;
    }

    /// <summary>
    /// Closes the Cursor
    /// </summary>
    /// <remarks>
    /// This method wraps the native ham_cursor_close function.
    /// <br />
    /// Closes this Cursor and frees allocated memory. All Cursors should
    /// be closed before closing the Database.
    /// </remarks>
    public void Close() {
      if (handle == IntPtr.Zero)
        return;
      int st;
      lock (db) {
        st = NativeMethods.CursorClose(handle);
      }
      if (st != 0)
        throw new DatabaseException(st);
      handle = IntPtr.Zero;
      db = null;
    }

    /// <summary>
    /// Closes the Cursor
    /// </summary>
    /// <see cref="Close" />
    public void Dispose() {
      Dispose(true);
      GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Closes the Cursor
    /// </summary>
    /// <see cref="Close" />
    protected virtual void Dispose(bool all) {
      if (all)
        Close();
    }

    private Database db;
    private IntPtr handle;
  }
}
