/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
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

package de.crupp.upscaledb;

public class DatabaseException extends java.lang.Exception {

  private native String ups_strerror(int errno);

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
    m_errno = errno;
  }

  /**
   * Returns the upscaledb error code
   *
   * @return The upscaledb error code
   */
  public int getErrno() {
    return m_errno;
  }

  /**
   * Returns an English error description.
   * <p>
   * This method wraps the native ups_strerror function.
   *
   * @return an English error description
   */
  public String getMessage() {
    return ups_strerror(m_errno);
  }

  /**
   * Returns an English error description.
   * <p>
   * This method wraps the native ups_strerror function.
   *
   * @return an English error description
   */
  public String toString() {
    return getMessage();
  }

  /**
   * The upscaledb status code
   */
  private int m_errno;

  static {
    System.loadLibrary("upscaledb-java");
  }
}
