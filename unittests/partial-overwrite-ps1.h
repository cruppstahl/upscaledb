/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
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

#define TEST_PREFIX "-ps1"
#define TEST_PAGESIZE 1024
#define TEST_INMEMORY false

namespace over_ps1 {

/* write at offset 0, partial size 50, record size 50 (no gaps) */
TEST_CASE("Partial-overwrite" TEST_PREFIX "/simpleInsertTest", "")
{
  OverwritePartialWriteFixture f(TEST_PAGESIZE, TEST_INMEMORY);
  f.simpleInsertTest();
}

/* write at offset 0, partial size 50, record size 100 (gap at end) */
TEST_CASE("Partial-overwrite" TEST_PREFIX "/insertGapsAtEndTestSmall", "")
{
  OverwritePartialWriteFixture f(TEST_PAGESIZE, TEST_INMEMORY);
  f.insertGapsAtEndTestSmall();
}

/* write at offset 0, partial size 500, record size 1000 (gap at end) */
TEST_CASE("Partial-overwrite" TEST_PREFIX "/insertGapsAtEndTestBig", "")
{
  OverwritePartialWriteFixture f(TEST_PAGESIZE, TEST_INMEMORY);
  f.insertGapsAtEndTestBig();
}

/* write at offset 0, partial size 5000, record size 10000 (gap at end) */
TEST_CASE("Partial-overwrite" TEST_PREFIX "/insertGapsAtEndTestBigger", "")
{
  OverwritePartialWriteFixture f(TEST_PAGESIZE, TEST_INMEMORY);
  f.insertGapsAtEndTestBigger();
}

/* write at offset 0, partial size 5001, record size 10001 (gap at end) */
TEST_CASE("Partial-overwrite" TEST_PREFIX "/insertGapsAtEndTestBiggerPlus1", "")
{
  OverwritePartialWriteFixture f(TEST_PAGESIZE, TEST_INMEMORY);
  f.insertGapsAtEndTestBiggerPlus1();
}

/* write at offset 0, partial size 50000, record size 100000 (gap at end) */
TEST_CASE("Partial-overwrite" TEST_PREFIX "/insertGapsAtEndTestBiggest", "")
{
  OverwritePartialWriteFixture f(TEST_PAGESIZE, TEST_INMEMORY);
  f.insertGapsAtEndTestBiggest();
}

/* write at offset 0, partial size 50001, record size 100001 (gap at end) */
TEST_CASE("Partial-overwrite" TEST_PREFIX "/insertGapsAtEndTestBiggestPlus1", "")
{
  OverwritePartialWriteFixture f(TEST_PAGESIZE, TEST_INMEMORY);
  f.insertGapsAtEndTestBiggestPlus1();
}

/* write at offset 0, partial size 500000, record size 1000000 (gap at end) */
TEST_CASE("Partial-overwrite" TEST_PREFIX "/insertGapsAtEndTestSuperbig", "")
{
  OverwritePartialWriteFixture f(TEST_PAGESIZE, TEST_INMEMORY);
  f.insertGapsAtEndTestSuperbig();
}

/* write at offset 0, partial size 500001, record size 1000001 (gap at end) */
TEST_CASE("Partial-overwrite" TEST_PREFIX "/insertGapsAtEndTestSuperbigPlus1", "")
{
  OverwritePartialWriteFixture f(TEST_PAGESIZE, TEST_INMEMORY);
  f.insertGapsAtEndTestSuperbigPlus1();
}

/* write at offset 50, partial size 50, record size 100 (gap at beginning) */
TEST_CASE("Partial-overwrite" TEST_PREFIX "/insertGapsAtBeginningSmall", "")
{
  OverwritePartialWriteFixture f(TEST_PAGESIZE, TEST_INMEMORY);
  f.insertGapsAtBeginningSmall();
}

/* write at offset 500, partial size 500, record size 1000 (gap at beginning) */
TEST_CASE("Partial-overwrite" TEST_PREFIX "/insertGapsAtBeginningBig", "")
{
  OverwritePartialWriteFixture f(TEST_PAGESIZE, TEST_INMEMORY);
  f.insertGapsAtBeginningBig();
}

/* write at offset 5000, partial size 5000, record size 10000 (gap at beginning) */
TEST_CASE("Partial-overwrite" TEST_PREFIX "/insertGapsAtBeginningBigger", "")
{
  OverwritePartialWriteFixture f(TEST_PAGESIZE, TEST_INMEMORY);
  f.insertGapsAtBeginningBigger();
}

/* write at offset 5001, partial size 5001, record size 10001 (gap at beginning) */
TEST_CASE("Partial-overwrite" TEST_PREFIX "/insertGapsAtBeginningBiggerPlus1", "")
{
  OverwritePartialWriteFixture f(TEST_PAGESIZE, TEST_INMEMORY);
  f.insertGapsAtBeginningBiggerPlus1();
}

/* write at offset 50000, partial size 50000, record size 100000 (gap at beginning) */
TEST_CASE("Partial-overwrite" TEST_PREFIX "/insertGapsAtBeginningBiggest", "")
{
  OverwritePartialWriteFixture f(TEST_PAGESIZE, TEST_INMEMORY);
  f.insertGapsAtBeginningBiggest();
}

/* write at offset 50001, partial size 50001, record size 100001 (gap at beginning) */
TEST_CASE("Partial-overwrite" TEST_PREFIX "/insertGapsAtBeginningBiggestPlus1", "")
{
  OverwritePartialWriteFixture f(TEST_PAGESIZE, TEST_INMEMORY);
  f.insertGapsAtBeginningBiggestPlus1();
}

/* write at offset 500000, partial size 500000, record size 1000000 (gap at beginning) */
TEST_CASE("Partial-overwrite" TEST_PREFIX "/insertGapsAtBeginningSuperbig", "")
{
  OverwritePartialWriteFixture f(TEST_PAGESIZE, TEST_INMEMORY);
  f.insertGapsAtBeginningSuperbig();
}

/* write at offset 500001, partial size 500001, record size 1000001 (gap at beginning) */
TEST_CASE("Partial-overwrite" TEST_PREFIX "/insertGapsAtBeginningSuperbigPlus1", "")
{
  OverwritePartialWriteFixture f(TEST_PAGESIZE, TEST_INMEMORY);
  f.insertGapsAtBeginningSuperbigPlus1();
}

/* write at offset 50, partial size 50, record size 200 (gap at
* beginning AND end) */
TEST_CASE("Partial-overwrite" TEST_PREFIX "/insertGapsTestSmall", "")
{
  OverwritePartialWriteFixture f(TEST_PAGESIZE, TEST_INMEMORY);
  f.insertGapsTestSmall();
}

/* write at offset 500, partial size 500, record size 2000 (gap at
* beginning AND end) */
TEST_CASE("Partial-overwrite" TEST_PREFIX "/insertGapsTestBig", "")
{
  OverwritePartialWriteFixture f(TEST_PAGESIZE, TEST_INMEMORY);
  f.insertGapsTestBig();
}

/* write at offset 5000, partial size 5000, record size 20000 (gap at
* beginning AND end) */
TEST_CASE("Partial-overwrite" TEST_PREFIX "/insertGapsTestBigger", "")
{
  OverwritePartialWriteFixture f(TEST_PAGESIZE, TEST_INMEMORY);
  f.insertGapsTestBigger();
}

/* write at offset 5001, partial size 5001, record size 20001 (gap at
* beginning AND end) */
TEST_CASE("Partial-overwrite" TEST_PREFIX "/insertGapsTestBiggerPlus1", "")
{
  OverwritePartialWriteFixture f(TEST_PAGESIZE, TEST_INMEMORY);
  f.insertGapsTestBiggerPlus1();
}

/* write at offset 50000, partial size 50000, record size 200000 (gap at
* beginning AND end) */
TEST_CASE("Partial-overwrite" TEST_PREFIX "/insertGapsTestBiggest", "")
{
  OverwritePartialWriteFixture f(TEST_PAGESIZE, TEST_INMEMORY);
  f.insertGapsTestBiggest();
}

/* write at offset 50001, partial size 50001, record size 200001 (gap at
* beginning AND end) */
TEST_CASE("Partial-overwrite" TEST_PREFIX "/insertGapsTestBiggestPlus1", "")
{
  OverwritePartialWriteFixture f(TEST_PAGESIZE, TEST_INMEMORY);
  f.insertGapsTestBiggestPlus1();
}

/* write at offset 500000, partial size 500000, record size 2000000
* (gap at beginning AND end) */
TEST_CASE("Partial-overwrite" TEST_PREFIX "/insertGapsTestSuperbig", "")
{
  OverwritePartialWriteFixture f(TEST_PAGESIZE, TEST_INMEMORY);
  f.insertGapsTestSuperbig();
}

/* write at offset 500001, partial size 500001, record size 2000001
* (gap at beginning AND end) */
TEST_CASE("Partial-overwrite" TEST_PREFIX "/insertGapsTestSuperbigPlus1", "")
{
  OverwritePartialWriteFixture f(TEST_PAGESIZE, TEST_INMEMORY);
  f.insertGapsTestSuperbigPlus1();
}

/* write at offset PS, partial size PS, record size 2*PS
* (gap at beginning AND end) */
TEST_CASE("Partial-overwrite" TEST_PREFIX "/insertGapsTestPagesize", "")
{
  OverwritePartialWriteFixture f(TEST_PAGESIZE, TEST_INMEMORY);
  f.insertGapsTestPagesize();
}

/* write at offset PS*2, partial size PS*2, record size 4*PS
* (gap at beginning AND end) */
TEST_CASE("Partial-overwrite" TEST_PREFIX "/insertGapsTestPagesize2", "")
{
  OverwritePartialWriteFixture f(TEST_PAGESIZE, TEST_INMEMORY);
  f.insertGapsTestPagesize2();
}

/* write at offset PS*4, partial size PS*4, record size 8*PS
* (gap at beginning AND end) */
TEST_CASE("Partial-overwrite" TEST_PREFIX "/insertGapsTestPagesize4", "")
{
  OverwritePartialWriteFixture f(TEST_PAGESIZE, TEST_INMEMORY);
  f.insertGapsTestPagesize4();
}

} // namespace

#undef TEST_PREFIX
#undef TEST_PAGESIZE
#undef TEST_INMEMORY
