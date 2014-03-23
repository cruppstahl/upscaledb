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
