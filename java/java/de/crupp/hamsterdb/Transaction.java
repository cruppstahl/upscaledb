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

package de.crupp.hamsterdb;

public class Transaction {

    private native int ham_txn_commit(long handle, int flags);

    private native int ham_txn_abort(long handle, int flags);

    /**
     * Constructor - assigns an Environment object and a Transaction handle
     */
    public Transaction(Environment env, long handle) {
        m_env=env;
        m_handle=handle;
    }

    /**
     * Destructor - automatically aborts the Transaction
     */
    public void finalize()
            throws DatabaseException {
        abort();
    }

    /**
     * Aborts the Transaction
     * <p>
     * This method wraps the native ham_txn_abort function.
     * <p>
     * More information: <a href="http://hamsterdb.com/public/scripts/html_www/group__ham__txn.html#ga9c08ad4fffe7f2b988593cf4c09c5116">C documentation</a>
     */
    public void abort()
            throws DatabaseException {
        int status;
        if (m_handle==0)
            return;
        synchronized (m_env) {
            status=ham_txn_abort(m_handle, 0);
        }
        if (status!=0)
            throw new DatabaseException(status);
        m_handle=0;
    }

    /**
     * Commits the Transaction
     * <p>
     * This method wraps the native ham_txn_commit function.
     * <p>
     * More information: <a href="http://hamsterdb.com/public/scripts/html_www/group__ham__txn.html#ga106406656415985aae40a85abdfa777d">C documentation</a>
     */
    public void commit()
            throws DatabaseException {
        int status;
        if (m_handle==0)
            return;
        synchronized (m_env) {
            status=ham_txn_commit(m_handle, 0);
        }
        if (status!=0)
            throw new DatabaseException(status);
        m_handle=0;
    }

    /**
     * Sets the Transaction handle
     */
    public void setHandle(long h) {
        m_handle=h;
    }

    /**
     * Returns the Transaction handle
     */
    public long getHandle() {
        return m_handle;
    }

    private long m_handle;
    private Environment m_env;
}
