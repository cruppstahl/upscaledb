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

package de.crupp.upscaledb;

public interface CompareCallback {

  /**
   * The compare method compares two keys - the "left-hand side"
   * (lhs) and the "right-hand side" (rhs).
   * <p>
   * More information: <a href="http://files.upscaledb.com/documentation/html/group__ups__database.html#gaa89b9aae5445c6deb2a31b41f6f99a9a">C documentation</a>
   *
   * @param lhs The first key
   * @param rhs The second key
   * @return -1 if the first key is smaller, +1 if the first key
   *   is larger, 0 if both keys are equal
   */
  public int compare(byte[] lhs, byte[] rhs);
}

