/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
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

namespace Hamster
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
    /// Destructor; aborts the Transaction
    /// </summary>
    ~Transaction() {
      Dispose(false);
    }

    /// <summary>
    /// Commits the Transaction
    /// </summary>
    /// <remarks>
    /// This method wraps the native ham_txn_commit function.
    /// <br />
    /// Note that the function will fail with HAM_CURSOR_STILL_OPEN if
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
    /// Aborts the Transaction
    /// </summary>
    /// <remarks>
    /// This method wraps the native ham_txn_abort function.
    /// <br />
    /// Note that the function will fail with HAM_CURSOR_STILL_OPEN if
    /// a Cursor was attached to this Transaction, and the Cursor was
    /// not closed.
    /// </remarks>
    public void Abort() {
      int st;
      lock (env) {
        st = NativeMethods.TxnAbort(handle, 0);
      }
      if (st != 0)
        throw new DatabaseException(st);
      handle = IntPtr.Zero;
      env = null;
    }

    /// <summary>
    /// Aborts the Transaction
    /// </summary>
    /// <see cref="Abort" />
    public void Dispose() {
      Dispose(true);
      GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Aborts the Transaction
    /// </summary>
    /// <see cref="Abort" />
    protected virtual void Dispose(bool all) {
      if (all)
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
