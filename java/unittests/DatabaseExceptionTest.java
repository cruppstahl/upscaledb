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

import de.crupp.hamsterdb.*;
import junit.framework.TestCase;

public class DatabaseExceptionTest extends TestCase {

  public void testGetErrno() {
    DatabaseException e = new DatabaseException(13);
    assertEquals(13, e.getErrno());
  }

  public void testGetMessage() {
    DatabaseException e = new DatabaseException(-8);
    assertEquals(-8, e.getErrno());
    assertEquals("Invalid parameter", e.getMessage());
  }

  public void testToString() {
    DatabaseException e = new DatabaseException(0);
    assertEquals(0, e.getErrno());
    assertEquals("Success", e.toString());
  }

}
