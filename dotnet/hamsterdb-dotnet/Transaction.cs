/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
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
