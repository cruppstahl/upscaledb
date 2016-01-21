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

namespace Upscaledb
{
  /// <summary>
  /// A Transaction class
  /// </summary>
  public class Transaction : IDisposable
  {
    internal Transaction(Environment env, IntPtr handle) {
      this.env = env;
      this.handle = handle;
    }

    /// <summary>
    /// Commits the Transaction
    /// </summary>
    /// <remarks>
    /// This method wraps the native ups_txn_commit function.
    /// <br />
    /// Note that the function will fail with UPS_CURSOR_STILL_OPEN if
    /// a Cursor was attached to this Transaction, and the Cursor was
    /// not closed.
    /// </remarks>
    public void Commit() {
      int st;
      lock (env) {
        st = NativeMethods.TxnCommit(handle, 0);
      }
      if (st != 0)
        throw new DatabaseException(st);
      handle = IntPtr.Zero;
      env = null;
    }

    /// <summary>
    /// Aborts the Transaction if it has not already been committed or aborted.
    /// </summary>
    /// <remarks>
    /// This method wraps the native ups_txn_abort function.
    /// <br />
    /// Note that the function will fail with UPS_CURSOR_STILL_OPEN if
    /// a Cursor was attached to this Transaction, and the Cursor was
    /// not closed.
    /// </remarks>
    public void Abort() {
      if (env != null) {
        int st;
        lock (env) {
          st = NativeMethods.TxnAbort(handle, 0);
        }
        if (st != 0)
          throw new DatabaseException(st);
        handle = IntPtr.Zero;
        env = null;
      }
    }

    /// <summary>
    /// Aborts the Transaction if it has not already been committed or aborted.
    /// </summary>
    /// <see cref="Abort" />
    public void Dispose() {
      Abort();
    }

    /// <summary>
    /// Returns the low-level Transaction handle
    /// </summary>
    public IntPtr Handle {
      get {
        return handle;
      }
    }

    private Environment env;
    private IntPtr handle;
  }
}
