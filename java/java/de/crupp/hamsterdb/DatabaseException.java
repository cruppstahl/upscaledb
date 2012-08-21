/**
 * Copyright (C) 2005-2010 Christoph Rupp (chris@crupp.de).
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

public class DatabaseException extends java.lang.Exception {

    private native String ham_strerror(int errno);

    /**
     * Constructor
     */
    public DatabaseException() {
        super();
    }

    public DatabaseException(Throwable t) {
        super(t);
    }

    public DatabaseException(int errno) {
        super();
        m_errno=errno;
    }

    /**
     * Returns the hamsterdb error code
     *
     * @return The hamsterdb error code
     */
    public int getErrno() {
        return m_errno;
    }

    /**
     * Returns an English error description.
     * <p>
     * This method wraps the native ham_strerror function.
     *
     * @return an English error description
     */
    public String getMessage() {
        return ham_strerror(m_errno);
    }

    /**
     * Returns an English error description.
     * <p>
     * This method wraps the native ham_strerror function.
     *
     * @return an English error description
     */
    public String toString() {
        return getMessage();
    }

    /**
     * The hamsterdb status code
     */
    private int m_errno;

    static {
        System.loadLibrary("hamsterdb-java");
    }
}
