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

public interface ErrorHandler {

  /**
   * The handleMessage method is called whenever a message
   * is emitted.
   * <p>
   * More information: <a href="http://files.upscaledb.com/documentation/html/group__ups__static.html#ga7e9a7dfcb312d1407b69e3c1a1f0d71e">C documentation</a>
   *
   * @param level the debug level (0 = Debug, 1 = Normal, 3 = Fatal)
   * @param message the message
   */
  public void handleMessage(int level, String message);
}

