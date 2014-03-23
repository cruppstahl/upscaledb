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

#define TEST_PREFIX "-ps4"
#define TEST_PAGESIZE 1024 * 4
#define TEST_INMEMORY false
#define TEST_FLAGS 0

namespace read_ps4 {

/* read at offset 0, partial size 50, record size 50 (no gaps) */
TEST_CASE("PartialRead" TEST_PREFIX "/simpleFindTest", "")
{
  PartialReadFixture f(TEST_PAGESIZE, TEST_INMEMORY, TEST_FLAGS);
  f.simpleFindTest();
}

/* read at offset 0, partial size 50, record size 100 (gap at end) */
TEST_CASE("PartialRead" TEST_PREFIX "/findGapsAtEndTestSmall", "")
{
  PartialReadFixture f(TEST_PAGESIZE, TEST_INMEMORY, TEST_FLAGS);
  f.findGapsAtEndTestSmall();
}

/* read at offset 0, partial size 500, record size 1000 (gap at end) */
TEST_CASE("PartialRead" TEST_PREFIX "/findGapsAtEndTestBig", "")
{
  PartialReadFixture f(TEST_PAGESIZE, TEST_INMEMORY, TEST_FLAGS);
  f.findGapsAtEndTestBig();
}

/* read at offset 0, partial size 5000, record size 10000 (gap at end) */
TEST_CASE("PartialRead" TEST_PREFIX "/findGapsAtEndTestBigger", "")
{
  PartialReadFixture f(TEST_PAGESIZE, TEST_INMEMORY, TEST_FLAGS);
  f.findGapsAtEndTestBigger();
}

/* read at offset 0, partial size 50000, record size 100000 (gap at end) */
TEST_CASE("PartialRead" TEST_PREFIX "/findGapsAtEndTestBiggest", "")
{
  PartialReadFixture f(TEST_PAGESIZE, TEST_INMEMORY, TEST_FLAGS);
  f.findGapsAtEndTestBiggest();
}

/* read at offset 0, partial size 500000, record size 1000000 (gap at end) */
TEST_CASE("PartialRead" TEST_PREFIX "/findGapsAtEndTestSuperbig", "")
{
  PartialReadFixture f(TEST_PAGESIZE, TEST_INMEMORY, TEST_FLAGS);
  f.findGapsAtEndTestSuperbig();
}

/* read at offset 50, partial size 50, record size 100 (gap at end) */
TEST_CASE("PartialRead" TEST_PREFIX "/findGapsAtBeginningTestSmall", "")
{
  PartialReadFixture f(TEST_PAGESIZE, TEST_INMEMORY, TEST_FLAGS);
  f.findGapsAtBeginningTestSmall();
}

/* read at offset 500, partial size 500, record size 1000 (gap at end) */
TEST_CASE("PartialRead" TEST_PREFIX "/findGapsAtBeginningTestBig", "")
{
  PartialReadFixture f(TEST_PAGESIZE, TEST_INMEMORY, TEST_FLAGS);
  f.findGapsAtBeginningTestBig();
}

/* read at offset 5000, partial size 5000, record size 10000 (gap at end) */
TEST_CASE("PartialRead" TEST_PREFIX "/findGapsAtBeginningTestBigger", "")
{
  PartialReadFixture f(TEST_PAGESIZE, TEST_INMEMORY, TEST_FLAGS);
  f.findGapsAtBeginningTestBigger();
}

/* read at offset 50000, partial size 50000, record size 100000 (gap at end) */
TEST_CASE("PartialRead" TEST_PREFIX "/findGapsAtBeginningTestBiggest", "")
{
  PartialReadFixture f(TEST_PAGESIZE, TEST_INMEMORY, TEST_FLAGS);
  f.findGapsAtBeginningTestBiggest();
}

/* read at offset 500000, partial size 500000, record size 1000000 (gap at end) */
TEST_CASE("PartialRead" TEST_PREFIX "/findGapsAtBeginningTestSuperbig", "")
{
  PartialReadFixture f(TEST_PAGESIZE, TEST_INMEMORY, TEST_FLAGS);
  f.findGapsAtBeginningTestSuperbig();
}

/* read at offset 50, partial size 50, record size 200 (gap
* at beginning and end) */
TEST_CASE("PartialRead" TEST_PREFIX "/findGapsTestSmall", "")
{
  PartialReadFixture f(TEST_PAGESIZE, TEST_INMEMORY, TEST_FLAGS);
  f.findGapsTestSmall();
}

/* read at offset 500, partial size 500, record size 2000 (gap
* at beginning and end) */
TEST_CASE("PartialRead" TEST_PREFIX "/findGapsTestBig", "")
{
  PartialReadFixture f(TEST_PAGESIZE, TEST_INMEMORY, TEST_FLAGS);
  f.findGapsTestBig();
}

/* read at offset 5000, partial size 5000, record size 20000 (gap
* at beginning and end) */
TEST_CASE("PartialRead" TEST_PREFIX "/findGapsTestBigger", "")
{
  PartialReadFixture f(TEST_PAGESIZE, TEST_INMEMORY, TEST_FLAGS);
  f.findGapsTestBigger();
}

/* read at offset 50000, partial size 50000, record size 200000 (gap
* at beginning and end) */
TEST_CASE("PartialRead" TEST_PREFIX "/findGapsTestBiggest", "")
{
  PartialReadFixture f(TEST_PAGESIZE, TEST_INMEMORY, TEST_FLAGS);
  f.findGapsTestBiggest();
}

/* read at offset 500000, partial size 500000, record size 2000000 (gap
* at beginning and end) */
TEST_CASE("PartialRead" TEST_PREFIX "/findGapsTestSuperbig", "")
{
  PartialReadFixture f(TEST_PAGESIZE, TEST_INMEMORY, TEST_FLAGS);
  f.findGapsTestSuperbig();
}

} // namespace

#undef TEST_PREFIX
#undef TEST_PAGESIZE
#undef TEST_INMEMORY
#undef TEST_FLAGS
