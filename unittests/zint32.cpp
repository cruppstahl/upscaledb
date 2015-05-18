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

#include "1os/os.h"

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
#ifdef WIN32
    uint32_t *din = (uint32_t *)::_aligned_malloc(sizeof(uint32_t) * 128, 16);
#else
	uint32_t *din = (uint32_t *)::malloc(sizeof(uint32_t) * 128);
#endif
    for (uint32_t i = 0; i < 128; i++)
      din[i] = i;

    uint32_t dout[128];
    uint32_t bits = simdmaxbitsd1(0, &din[0]);
    REQUIRE(bits == 1);
    simdpackwithoutmaskd1(0, &din[0], (__m128i *)&dout[0], bits);

    ::memset(&din[0], 0, sizeof(uint32_t) * 128);
    simdunpackd1(0, (__m128i *)&dout[0], &din[0], bits);

    for (uint32_t i = 0; i < 128; i++)
      REQUIRE(din[i] == i);

#ifdef WIN32
	::_aligned_free(din);
#else
	::free(din);
#endif
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

TEST_CASE("Zint32/Varbyte/randomDataTest", "")
{
  Zint32Fixture::IntVector ivec;
  for (int i = 0; i < 30000; i++)
    ivec.push_back(i);
  std::srand(0); // make this reproducable
  std::random_shuffle(ivec.begin(), ivec.end());

  Zint32Fixture f(HAM_COMPRESSOR_UINT32_VARBYTE);
  f.insertFindEraseFind(ivec);
}

TEST_CASE("Zint32/Varbyte/ascendingDataTest", "")
{
  Zint32Fixture::IntVector ivec;
  for (int i = 0; i < 30000; i++)
    ivec.push_back(i);

  Zint32Fixture f(HAM_COMPRESSOR_UINT32_VARBYTE);
  f.insertFindEraseFind(ivec);
}

TEST_CASE("Zint32/Varbyte/descendingDataTest", "")
{
  Zint32Fixture::IntVector ivec;
  for (int i = 30000; i >= 0; i--)
    ivec.push_back(i);

  Zint32Fixture f(HAM_COMPRESSOR_UINT32_VARBYTE);
  f.insertFindEraseFind(ivec);
}

TEST_CASE("Zint32/SimdComp/basicSimdcompTest", "")
{
  Zint32Fixture f(HAM_COMPRESSOR_UINT32_SIMDCOMP);
  f.basicSimdcompTest();
}

TEST_CASE("Zint32/SimdComp/randomDataTest", "")
{
  Zint32Fixture::IntVector ivec;
  for (int i = 0; i < 30000; i++)
    ivec.push_back(i);
  std::srand(0); // make this reproducible
  std::random_shuffle(ivec.begin(), ivec.end());

  Zint32Fixture f(HAM_COMPRESSOR_UINT32_SIMDCOMP);
  f.insertFindEraseFind(ivec);
}

TEST_CASE("Zint32/SimdComp/ascendingDataTest", "")
{
  Zint32Fixture::IntVector ivec;
  for (int i = 0; i < 30000; i++)
    ivec.push_back(i);

  Zint32Fixture f(HAM_COMPRESSOR_UINT32_SIMDCOMP);
  f.insertFindEraseFind(ivec);
}

TEST_CASE("Zint32/SimdComp/descendingDataTest", "")
{
  Zint32Fixture::IntVector ivec;
  for (int i = 30000; i >= 0; i--)
    ivec.push_back(i);

  Zint32Fixture f(HAM_COMPRESSOR_UINT32_SIMDCOMP);
  f.insertFindEraseFind(ivec);
}

TEST_CASE("Zint32/GroupVarint/randomDataTest", "")
{
  Zint32Fixture::IntVector ivec;
  for (int i = 0; i < 30000; i++)
    ivec.push_back(i);
  std::srand(0); // make this reproducible
  std::random_shuffle(ivec.begin(), ivec.end());

  Zint32Fixture f(HAM_COMPRESSOR_UINT32_GROUPVARINT);
  f.insertFindEraseFind(ivec);
}

TEST_CASE("Zint32/GroupVarint/ascendingDataTest", "")
{
  Zint32Fixture::IntVector ivec;
  for (int i = 0; i < 30000; i++)
    ivec.push_back(i);

  Zint32Fixture f(HAM_COMPRESSOR_UINT32_GROUPVARINT);
  f.insertFindEraseFind(ivec);
}

TEST_CASE("Zint32/GroupVarint/descendingDataTest", "")
{
  Zint32Fixture::IntVector ivec;
  for (int i = 30000; i >= 0; i--)
    ivec.push_back(i);

  Zint32Fixture f(HAM_COMPRESSOR_UINT32_GROUPVARINT);
  f.insertFindEraseFind(ivec);
}

TEST_CASE("Zint32/StreamVbyte/randomDataTest", "")
{
  Zint32Fixture::IntVector ivec;
  for (int i = 0; i < 30000; i++)
    ivec.push_back(i);
  std::srand(0); // make this reproducible
  std::random_shuffle(ivec.begin(), ivec.end());

  Zint32Fixture f(HAM_COMPRESSOR_UINT32_STREAMVBYTE);
  f.insertFindEraseFind(ivec);
}

TEST_CASE("Zint32/StreamVbyte/ascendingDataTest", "")
{
  Zint32Fixture::IntVector ivec;
  for (int i = 0; i < 30000; i++)
    ivec.push_back(i);

  Zint32Fixture f(HAM_COMPRESSOR_UINT32_STREAMVBYTE);
  f.insertFindEraseFind(ivec);
}

TEST_CASE("Zint32/StreamVbyte/descendingDataTest", "")
{
  Zint32Fixture::IntVector ivec;
  for (int i = 30000; i >= 0; i--)
    ivec.push_back(i);

  Zint32Fixture f(HAM_COMPRESSOR_UINT32_STREAMVBYTE);
  f.insertFindEraseFind(ivec);
}

TEST_CASE("Zint32/MaskedVbyte/randomDataTest", "")
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

TEST_CASE("Zint32/MaskedVbyte/ascendingDataTest", "")
{
  if (os_has_avx()) {
    Zint32Fixture::IntVector ivec;
    for (int i = 0; i < 30000; i++)
      ivec.push_back(i);

    Zint32Fixture f(HAM_COMPRESSOR_UINT32_MASKEDVBYTE);
    f.insertFindEraseFind(ivec);
  }
}

TEST_CASE("Zint32/MaskedVbyte/descendingDataTest", "")
{
  if (os_has_avx()) {
    Zint32Fixture::IntVector ivec;
    for (int i = 30000; i >= 0; i--)
      ivec.push_back(i);

    Zint32Fixture f(HAM_COMPRESSOR_UINT32_MASKEDVBYTE);
    f.insertFindEraseFind(ivec);
  }
}

TEST_CASE("Zint32/BlockIndex/randomDataTest", "")
{
  Zint32Fixture::IntVector ivec;
  for (int i = 0; i < 30000; i++)
    ivec.push_back(i);
  std::srand(0); // make this reproducible
  std::random_shuffle(ivec.begin(), ivec.end());

  Zint32Fixture f(HAM_COMPRESSOR_UINT32_BLOCKINDEX);
  f.insertFindEraseFind(ivec);
}

TEST_CASE("Zint32/BlockIndex/ascendingDataTest", "")
{
  Zint32Fixture::IntVector ivec;
  for (int i = 0; i < 30000; i++)
    ivec.push_back(i);

  Zint32Fixture f(HAM_COMPRESSOR_UINT32_BLOCKINDEX);
  f.insertFindEraseFind(ivec);
}

TEST_CASE("Zint32/BlockIndex/descendingDataTest", "")
{
  Zint32Fixture::IntVector ivec;
  for (int i = 30000; i >= 0; i--)
    ivec.push_back(i);

  Zint32Fixture f(HAM_COMPRESSOR_UINT32_BLOCKINDEX);
  f.insertFindEraseFind(ivec);
}

TEST_CASE("Zint32/Zint32/invalidPagesizeTest", "")
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

} // namespace hamsterdb

#endif // HAM_ENABLE_COMPRESSION
