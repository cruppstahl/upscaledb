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

#define TEST_PREFIX "-inmem-ps4"
#define TEST_PAGESIZE 1024 * 4
#define TEST_INMEMORY true
#define TEST_FLAGS 0

namespace read_inmem_ps4 {

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
