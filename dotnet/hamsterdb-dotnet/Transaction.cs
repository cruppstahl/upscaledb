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
