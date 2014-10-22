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

#ifdef HAM_ENABLE_COMPRESSION

#include <vector>
#include <algorithm>

#include "3rdparty/catch/catch.hpp"
#include "3rdparty/simdcomp/include/simdcomp.h"

#include "utils.h"
#include "os.hpp"

namespace hamsterdb {

struct Zint32Fixture {
  ham_db_t *m_db;
  ham_env_t *m_env;

  typedef std::vector<uint32_t> IntVector;

  Zint32Fixture(uint64_t compressor)
    : m_db(0), m_env(0) {
    ham_parameter_t p[] = {
      { HAM_PARAM_KEY_TYPE, HAM_TYPE_UINT32 },
      { HAM_PARAM_KEY_COMPRESSION, compressor },
      { HAM_PARAM_RECORD_SIZE, 4 },
      { 0, 0 }
    };

    REQUIRE(0 == ham_env_create(&m_env, Utils::opath(".test"), 0, 0644, 0));
    REQUIRE(0 == ham_env_create_db(m_env, &m_db, 1, 0, &p[0]));
  }

  ~Zint32Fixture() {
    if (m_env)
	  REQUIRE(0 == ham_env_close(m_env, HAM_AUTO_CLEANUP));
  }

  void basicSimdcompTest() {
    uint32_t din[128];
    for (uint32_t i = 0; i < 128; i++)
      din[i] = i;

    uint32_t dout[128];
    uint32_t bits = simdmaxbitsd1(0, &din[0]);
    REQUIRE(bits == 1);
    simdpackwithoutmaskd1(0, &din[0], (__m128i *)&dout[0], bits);

    ::memset(&din[0], 0, sizeof(din));
    simdunpackd1(0, (__m128i *)&dout[0], &din[0], bits);

    for (uint32_t i = 0; i < 128; i++)
      REQUIRE(din[i] == i);
  }

  void insertFindEraseFind(const IntVector &ivec) {
    ham_key_t key = {0};
    ham_record_t record = {0};

    for (IntVector::const_iterator it = ivec.begin();
                    it != ivec.end(); it++) {
      uint32_t k = *it;
      key.data = (void *)&k;
      key.size = sizeof(k);
      record.data = (void *)&k;
      record.size = sizeof(k);

      REQUIRE(0 == ham_db_insert(m_db, 0, &key, &record, 0));
    }

    for (IntVector::const_iterator it = ivec.begin();
                    it != ivec.end(); it++) {
      uint32_t k = *it;
      key.data = (void *)&k;
      key.size = sizeof(k);

      REQUIRE(0 == ham_db_find(m_db, 0, &key, &record, 0));
      REQUIRE(record.size == sizeof(uint32_t));
      REQUIRE(*(uint32_t *)record.data == k);
    }

    for (IntVector::const_iterator it = ivec.begin();
                    it != ivec.end(); it++) {
      uint32_t k = *it;
      key.data = (void *)&k;
      key.size = sizeof(k);

      REQUIRE(0 == ham_db_erase(m_db, 0, &key, 0));
    }

    for (IntVector::const_iterator it = ivec.begin();
                    it != ivec.end(); it++) {
      uint32_t k = *it;
      key.data = (void *)&k;
      key.size = sizeof(k);

      REQUIRE(HAM_KEY_NOT_FOUND == ham_db_find(m_db, 0, &key, &record, 0));
    }
  }
};

TEST_CASE("Varbyte/randomDataTest", "")
{
  Zint32Fixture::IntVector ivec;
  for (int i = 0; i < 30000; i++)
    ivec.push_back(i);
  std::srand(0); // make this reproducable
  std::random_shuffle(ivec.begin(), ivec.end());

  Zint32Fixture f(HAM_COMPRESSOR_UINT32_VARBYTE);
  f.insertFindEraseFind(ivec);
}

TEST_CASE("Varbyte/ascendingDataTest", "")
{
  Zint32Fixture::IntVector ivec;
  for (int i = 0; i < 30000; i++)
    ivec.push_back(i);

  Zint32Fixture f(HAM_COMPRESSOR_UINT32_VARBYTE);
  f.insertFindEraseFind(ivec);
}

TEST_CASE("Varbyte/descendingDataTest", "")
{
  Zint32Fixture::IntVector ivec;
  for (int i = 30000; i >= 0; i--)
    ivec.push_back(i);

  Zint32Fixture f(HAM_COMPRESSOR_UINT32_VARBYTE);
  f.insertFindEraseFind(ivec);
}

TEST_CASE("SimdComp/basicSimdcompTest", "")
{
  Zint32Fixture f(HAM_COMPRESSOR_UINT32_SIMDCOMP);
  f.basicSimdcompTest();
}

TEST_CASE("SimdComp/randomDataTest", "")
{
  Zint32Fixture::IntVector ivec;
  for (int i = 0; i < 30000; i++)
    ivec.push_back(i);
  std::srand(0); // make this reproducible
  std::random_shuffle(ivec.begin(), ivec.end());

  Zint32Fixture f(HAM_COMPRESSOR_UINT32_SIMDCOMP);
  f.insertFindEraseFind(ivec);
}

TEST_CASE("SimdComp/ascendingDataTest", "")
{
  Zint32Fixture::IntVector ivec;
  for (int i = 0; i < 30000; i++)
    ivec.push_back(i);

  Zint32Fixture f(HAM_COMPRESSOR_UINT32_SIMDCOMP);
  f.insertFindEraseFind(ivec);
}

TEST_CASE("SimdComp/descendingDataTest", "")
{
  Zint32Fixture::IntVector ivec;
  for (int i = 30000; i >= 0; i--)
    ivec.push_back(i);

  Zint32Fixture f(HAM_COMPRESSOR_UINT32_SIMDCOMP);
  f.insertFindEraseFind(ivec);
}

TEST_CASE("GroupVarint/randomDataTest", "")
{
  Zint32Fixture::IntVector ivec;
  for (int i = 0; i < 30000; i++)
    ivec.push_back(i);
  std::srand(0); // make this reproducible
  std::random_shuffle(ivec.begin(), ivec.end());

  Zint32Fixture f(HAM_COMPRESSOR_UINT32_GROUPVARINT);
  f.insertFindEraseFind(ivec);
}

TEST_CASE("GroupVarint/ascendingDataTest", "")
{
  Zint32Fixture::IntVector ivec;
  for (int i = 0; i < 30000; i++)
    ivec.push_back(i);

  Zint32Fixture f(HAM_COMPRESSOR_UINT32_GROUPVARINT);
  f.insertFindEraseFind(ivec);
}

TEST_CASE("GroupVarint/descendingDataTest", "")
{
  Zint32Fixture::IntVector ivec;
  for (int i = 30000; i >= 0; i--)
    ivec.push_back(i);

  Zint32Fixture f(HAM_COMPRESSOR_UINT32_GROUPVARINT);
  f.insertFindEraseFind(ivec);
}

TEST_CASE("StreamVbyte/randomDataTest", "")
{
  Zint32Fixture::IntVector ivec;
  for (int i = 0; i < 30000; i++)
    ivec.push_back(i);
  std::srand(0); // make this reproducible
  std::random_shuffle(ivec.begin(), ivec.end());

  Zint32Fixture f(HAM_COMPRESSOR_UINT32_STREAMVBYTE);
  f.insertFindEraseFind(ivec);
}

TEST_CASE("StreamVbyte/ascendingDataTest", "")
{
  Zint32Fixture::IntVector ivec;
  for (int i = 0; i < 30000; i++)
    ivec.push_back(i);

  Zint32Fixture f(HAM_COMPRESSOR_UINT32_STREAMVBYTE);
  f.insertFindEraseFind(ivec);
}

TEST_CASE("StreamVbyte/descendingDataTest", "")
{
  Zint32Fixture::IntVector ivec;
  for (int i = 30000; i >= 0; i--)
    ivec.push_back(i);

  Zint32Fixture f(HAM_COMPRESSOR_UINT32_STREAMVBYTE);
  f.insertFindEraseFind(ivec);
}

TEST_CASE("MaskedVbyte/randomDataTest", "")
{
  if (os_has_avx()) {
    Zint32Fixture::IntVector ivec;
    for (int i = 0; i < 30000; i++)
      ivec.push_back(i);
    std::srand(0); // make this reproducible
    std::random_shuffle(ivec.begin(), ivec.end());

    Zint32Fixture f(HAM_COMPRESSOR_UINT32_MASKEDVBYTE);
    f.insertFindEraseFind(ivec);
  }
}

TEST_CASE("MaskedVbyte/ascendingDataTest", "")
{
  if (os_has_avx()) {
    Zint32Fixture::IntVector ivec;
    for (int i = 0; i < 30000; i++)
      ivec.push_back(i);

    Zint32Fixture f(HAM_COMPRESSOR_UINT32_MASKEDVBYTE);
    f.insertFindEraseFind(ivec);
  }
}

TEST_CASE("MaskedVbyte/descendingDataTest", "")
{
  if (os_has_avx()) {
    Zint32Fixture::IntVector ivec;
    for (int i = 30000; i >= 0; i--)
      ivec.push_back(i);

    Zint32Fixture f(HAM_COMPRESSOR_UINT32_MASKEDVBYTE);
    f.insertFindEraseFind(ivec);
  }
}

TEST_CASE("BlockIndex/randomDataTest", "")
{
  Zint32Fixture::IntVector ivec;
  for (int i = 0; i < 30000; i++)
    ivec.push_back(i);
  std::srand(0); // make this reproducible
  std::random_shuffle(ivec.begin(), ivec.end());

  Zint32Fixture f(HAM_COMPRESSOR_UINT32_BLOCKINDEX);
  f.insertFindEraseFind(ivec);
}

TEST_CASE("BlockIndex/ascendingDataTest", "")
{
  Zint32Fixture::IntVector ivec;
  for (int i = 0; i < 30000; i++)
    ivec.push_back(i);

  Zint32Fixture f(HAM_COMPRESSOR_UINT32_BLOCKINDEX);
  f.insertFindEraseFind(ivec);
}

TEST_CASE("BlockIndex/descendingDataTest", "")
{
  Zint32Fixture::IntVector ivec;
  for (int i = 30000; i >= 0; i--)
    ivec.push_back(i);

  Zint32Fixture f(HAM_COMPRESSOR_UINT32_BLOCKINDEX);
  f.insertFindEraseFind(ivec);
}

<<<<<<< HEAD
TEST_CASE("Zint32/invalidPagesizeTest", "")
{
  ham_parameter_t p1[] = {
    { HAM_PARAM_PAGE_SIZE, 1024 },
    { 0, 0 }
  };
  ham_parameter_t p2[] = {
    { HAM_PARAM_KEY_TYPE, HAM_TYPE_UINT32 },
    { HAM_PARAM_KEY_COMPRESSION, HAM_COMPRESSOR_UINT32_VARBYTE},
    { 0, 0 }
  };

  ham_env_t *env;
  ham_db_t *db;

  REQUIRE(0 == ham_env_create(&env, Utils::opath(".test"), 0, 0644, &p1[0]));
  REQUIRE(HAM_INV_PARAMETER == ham_env_create_db(env, &db, 1, 0, &p2[0]));
  ham_env_close(env, 0);
}

=======
>>>>>>> a3bdef5... Adding stream vbyte-compression for uint32 keys
=======
#endif

>>>>>>> c528002... Adding MaskedVbyte compression algorithm (wip)
} // namespace hamsterdb

#endif // HAM_ENABLE_COMPRESSION
