/*
 * Copyright (C) 2005-2017 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * See the file COPYING for License information.
 */

#include <vector>
#include <algorithm>

#include <ups/upscaledb_uqi.h>

#include "3rdparty/catch/catch.hpp"
#ifdef HAVE_SSE2
#include "3rdparty/simdcomp/include/simdcomp.h"
#endif

#include "1os/os.h"

#include "os.hpp"
#include "fixture.hpp"

namespace upscaledb {

struct Zint32Fixture : BaseFixture {
  typedef std::vector<uint32_t> IntVector;

  Zint32Fixture(uint64_t compressor, bool use_duplicates,
                  uint64_t record_size) {
    ups_parameter_t p[] = {
      { UPS_PARAM_RECORD_SIZE, record_size },
      { UPS_PARAM_KEY_TYPE, UPS_TYPE_UINT32 },
      { UPS_PARAM_KEY_COMPRESSION, compressor },
      { 0, 0 }
    };

    if (compressor == 0) {
      p[2].name = 0;
      p[2].value = 0;
    }

    require_create(0, nullptr, use_duplicates ? UPS_ENABLE_DUPLICATES : 0, p);
  }

  void basicSimdcompTest() {
#ifdef HAVE_SSE2
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
#endif // HAVE_SSE2
  }

  void insertFindEraseFind(const IntVector &ivec) {
    ups_key_t key = {0};
    ups_record_t record = {0};

    for (IntVector::const_iterator it = ivec.begin(); it != ivec.end(); it++) {
      uint32_t k = *it;
      key.data = (void *)&k;
      key.size = sizeof(k);
      record.data = (void *)&k;
      record.size = sizeof(k);

      REQUIRE(0 == ups_db_insert(db, 0, &key, &record, 0));
    }

    for (IntVector::const_iterator it = ivec.begin();
                    it != ivec.end(); it++) {
      uint32_t k = *it;
      key.data = (void *)&k;
      key.size = sizeof(k);

      REQUIRE(0 == ups_db_find(db, 0, &key, &record, 0));
      REQUIRE(record.size == sizeof(uint32_t));
      REQUIRE(*(uint32_t *)record.data == k);
    }

    for (IntVector::const_iterator it = ivec.begin();
                    it != ivec.end(); it++) {
      uint32_t k = *it;
      key.data = (void *)&k;
      key.size = sizeof(k);

      REQUIRE(0 == ups_db_erase(db, 0, &key, 0));
    }

    for (IntVector::const_iterator it = ivec.begin();
                    it != ivec.end(); it++) {
      uint32_t k = *it;
      key.data = (void *)&k;
      key.size = sizeof(k);

      REQUIRE(UPS_KEY_NOT_FOUND == ups_db_find(db, 0, &key, &record, 0));
    }
  }

  void uqiTest() {
    ups_key_t key = {0};
    ups_record_t record = {0};

    for (uint32_t i = 0; i < 30000; i++) {
      key.data = (void *)&i;
      key.size = sizeof(i);

      REQUIRE(0 == ups_db_insert(db, 0, &key, &record, 0));
    }

    uqi_result_t *result;
    uint32_t size;

    REQUIRE(0 == uqi_select(env, "SUM($key) from database 1", &result));
    REQUIRE(uqi_result_get_record_type(result) == UPS_TYPE_UINT64);
    REQUIRE(*(uint64_t *)uqi_result_get_record_data(result, &size) == 449985000ull);

    uqi_result_close(result);

    REQUIRE(0 == uqi_select(env, "AVERAGE($key) from database 1", &result));
    REQUIRE(uqi_result_get_record_type(result) == UPS_TYPE_REAL64);
    REQUIRE(*(double *)uqi_result_get_record_data(result, &size) == 14999.5);

    uqi_result_close(result);
  }

  void uqiTestDuplicate() {
    ups_key_t key = {0};
    ups_record_t record = {0};

    uint32_t max = 10000;
    for (uint32_t i = 0; i < max; i++) {
      key.data = (void *)&i;
      key.size = sizeof(i);

      REQUIRE(0 == ups_db_insert(db, 0, &key, &record, UPS_DUPLICATE));
      REQUIRE(0 == ups_db_insert(db, 0, &key, &record, UPS_DUPLICATE));
      REQUIRE(0 == ups_db_insert(db, 0, &key, &record, UPS_DUPLICATE));
    }

    uint64_t value;
    uint32_t size;
    uqi_result_t *result;

    //REQUIRE(0 == ups_db_count(db, 0, 0, &value));
    //REQUIRE(30000ull == value);

    REQUIRE(0 == uqi_select(env, "COUNT ($key) from database 1",
                        &result));
    REQUIRE(uqi_result_get_record_type(result) == UPS_TYPE_UINT64);
    value = *(uint64_t *)uqi_result_get_record_data(result, &size);
    REQUIRE(value == max * 3);
    REQUIRE(size == 8ull);
    uqi_result_close(result);

    REQUIRE(0 == uqi_select(env, "DISTINCT COUNT ($key) from database 1",
                        &result));
    REQUIRE(uqi_result_get_record_type(result) == UPS_TYPE_UINT64);
    value = *(uint64_t *)uqi_result_get_record_data(result, &size);
    REQUIRE(value == max);
    REQUIRE(size == 8ull);
    uqi_result_close(result);
  }
};

TEST_CASE("Zint32/Pod/randomDataTest", "")
{
  Zint32Fixture::IntVector ivec;
  for (int i = 0; i < 30000; i++)
    ivec.push_back(i);
  std::srand(0); // make this reproducable
  std::random_shuffle(ivec.begin(), ivec.end());

  Zint32Fixture f(0, false, 4);
  f.insertFindEraseFind(ivec);
}

TEST_CASE("Zint32/Pod/ascendingDataTest", "")
{
  Zint32Fixture::IntVector ivec;
  for (int i = 0; i < 30000; i++)
    ivec.push_back(i);

  Zint32Fixture f(0, false, 4);
  f.insertFindEraseFind(ivec);
}

TEST_CASE("Zint32/Pod/descendingDataTest", "")
{
  Zint32Fixture::IntVector ivec;
  for (int i = 30000; i >= 0; i--)
    ivec.push_back(i);

  Zint32Fixture f(0, false, 4);
  f.insertFindEraseFind(ivec);
}

TEST_CASE("Zint32/Pod/uqiTest", "")
{
  Zint32Fixture f(0, false, 0);
  f.uqiTest();
}

TEST_CASE("Zint32/Pod/uqiTest-duplicate", "")
{
  Zint32Fixture f(0, true, 0);
  f.uqiTestDuplicate();
}

TEST_CASE("Zint32/Varbyte/randomDataTest", "")
{
  Zint32Fixture::IntVector ivec;
  for (int i = 0; i < 30000; i++)
    ivec.push_back(i);
  std::srand(0); // make this reproducable
  std::random_shuffle(ivec.begin(), ivec.end());

  Zint32Fixture f(UPS_COMPRESSOR_UINT32_VARBYTE, false, 4);
  f.insertFindEraseFind(ivec);
}

TEST_CASE("Zint32/Varbyte/ascendingDataTest", "")
{
  Zint32Fixture::IntVector ivec;
  for (int i = 0; i < 30000; i++)
    ivec.push_back(i);

  Zint32Fixture f(UPS_COMPRESSOR_UINT32_VARBYTE, false, 4);
  f.insertFindEraseFind(ivec);
}

TEST_CASE("Zint32/Varbyte/descendingDataTest", "")
{
  Zint32Fixture::IntVector ivec;
  for (int i = 30000; i >= 0; i--)
    ivec.push_back(i);

  Zint32Fixture f(UPS_COMPRESSOR_UINT32_VARBYTE, false, 4);
  f.insertFindEraseFind(ivec);
}

TEST_CASE("Zint32/Varbyte/uqiTest", "")
{
  Zint32Fixture f(UPS_COMPRESSOR_UINT32_VARBYTE, false, 0);
  f.uqiTest();
}

TEST_CASE("Zint32/Varbyte/uqiTest-duplicate", "")
{
  Zint32Fixture f(UPS_COMPRESSOR_UINT32_VARBYTE, true, 0);
  f.uqiTestDuplicate();
}

TEST_CASE("Zint32/SimdComp/basicSimdcompTest", "")
{
#ifdef HAVE_SSE2
  Zint32Fixture f(UPS_COMPRESSOR_UINT32_SIMDCOMP, false, 4);
  f.basicSimdcompTest();
#endif
}

TEST_CASE("Zint32/SimdComp/randomDataTest", "")
{
#ifdef HAVE_SSE2
  Zint32Fixture::IntVector ivec;
  for (int i = 0; i < 30000; i++)
    ivec.push_back(i);
  std::srand(0); // make this reproducible
  std::random_shuffle(ivec.begin(), ivec.end());

  Zint32Fixture f(UPS_COMPRESSOR_UINT32_SIMDCOMP, false, 4);
  f.insertFindEraseFind(ivec);
#endif
}

TEST_CASE("Zint32/SimdComp/ascendingDataTest", "")
{
#ifdef HAVE_SSE2
  Zint32Fixture::IntVector ivec;
  for (int i = 0; i < 30000; i++)
    ivec.push_back(i);

  Zint32Fixture f(UPS_COMPRESSOR_UINT32_SIMDCOMP, false, 4);
  f.insertFindEraseFind(ivec);
#endif
}

TEST_CASE("Zint32/SimdComp/descendingDataTest", "")
{
#ifdef HAVE_SSE2
  Zint32Fixture::IntVector ivec;
  for (int i = 30000; i >= 0; i--)
    ivec.push_back(i);

  Zint32Fixture f(UPS_COMPRESSOR_UINT32_SIMDCOMP, false, 4);
  f.insertFindEraseFind(ivec);
#endif
}

TEST_CASE("Zint32/SimdComp/uqiTest", "")
{
#ifdef HAVE_SSE2
  Zint32Fixture f(UPS_COMPRESSOR_UINT32_SIMDCOMP, false, 0);
  f.uqiTest();
#endif
}

TEST_CASE("Zint32/SimdComp/uqiTest-duplicate", "")
{
#ifdef HAVE_SSE2
  Zint32Fixture f(UPS_COMPRESSOR_UINT32_SIMDCOMP, true, 0);
  f.uqiTestDuplicate();
#endif
}

TEST_CASE("Zint32/GroupVarint/randomDataTest", "")
{
#ifdef HAVE_SSE2
  Zint32Fixture::IntVector ivec;
  for (int i = 0; i < 30000; i++)
    ivec.push_back(i);
  std::srand(0); // make this reproducible
  std::random_shuffle(ivec.begin(), ivec.end());

  Zint32Fixture f(UPS_COMPRESSOR_UINT32_GROUPVARINT, false, 4);
  f.insertFindEraseFind(ivec);
#endif
}

TEST_CASE("Zint32/GroupVarint/ascendingDataTest", "")
{
#ifdef HAVE_SSE2
  Zint32Fixture::IntVector ivec;
  for (int i = 0; i < 30000; i++)
    ivec.push_back(i);

  Zint32Fixture f(UPS_COMPRESSOR_UINT32_GROUPVARINT, false, 4);
  f.insertFindEraseFind(ivec);
#endif
}

TEST_CASE("Zint32/GroupVarint/descendingDataTest", "")
{
#ifdef HAVE_SSE2
  Zint32Fixture::IntVector ivec;
  for (int i = 30000; i >= 0; i--)
    ivec.push_back(i);

  Zint32Fixture f(UPS_COMPRESSOR_UINT32_GROUPVARINT, false, 4);
  f.insertFindEraseFind(ivec);
#endif
}

TEST_CASE("Zint32/GroupVarint/uqiTest", "")
{
#ifdef HAVE_SSE2
  Zint32Fixture f(UPS_COMPRESSOR_UINT32_GROUPVARINT, false, 0);
  f.uqiTest();
#endif
}

TEST_CASE("Zint32/GroupVarint/uqiTest-duplicate", "")
{
#ifdef HAVE_SSE2
  Zint32Fixture f(UPS_COMPRESSOR_UINT32_GROUPVARINT, true, 0);
  f.uqiTestDuplicate();
#endif
}

TEST_CASE("Zint32/StreamVbyte/randomDataTest", "")
{
#ifdef HAVE_SSE2
  Zint32Fixture::IntVector ivec;
  for (int i = 0; i < 30000; i++)
    ivec.push_back(i);
  std::srand(0); // make this reproducible
  std::random_shuffle(ivec.begin(), ivec.end());

  Zint32Fixture f(UPS_COMPRESSOR_UINT32_STREAMVBYTE, false, 4);
  f.insertFindEraseFind(ivec);
#endif
}

TEST_CASE("Zint32/StreamVbyte/ascendingDataTest", "")
{
#ifdef HAVE_SSE2
  Zint32Fixture::IntVector ivec;
  for (int i = 0; i < 30000; i++)
    ivec.push_back(i);

  Zint32Fixture f(UPS_COMPRESSOR_UINT32_STREAMVBYTE, false, 4);
  f.insertFindEraseFind(ivec);
#endif
}

TEST_CASE("Zint32/StreamVbyte/descendingDataTest", "")
{
#ifdef HAVE_SSE2
  Zint32Fixture::IntVector ivec;
  for (int i = 30000; i >= 0; i--)
    ivec.push_back(i);

  Zint32Fixture f(UPS_COMPRESSOR_UINT32_STREAMVBYTE, false, 4);
  f.insertFindEraseFind(ivec);
#endif
}

TEST_CASE("Zint32/StreamVbyte/uqiTest", "")
{
#ifdef HAVE_SSE2
  Zint32Fixture f(UPS_COMPRESSOR_UINT32_STREAMVBYTE, false, 0);
  f.uqiTest();
#endif
}

TEST_CASE("Zint32/StreamVbyte/uqiTest-duplicate", "")
{
#ifdef HAVE_SSE2
  Zint32Fixture f(UPS_COMPRESSOR_UINT32_STREAMVBYTE, true, 0);
  f.uqiTestDuplicate();
#endif
}

TEST_CASE("Zint32/FOR/randomDataTest", "")
{
  Zint32Fixture::IntVector ivec;
  for (int i = 0; i < 30000; i++)
    ivec.push_back(i);
  std::srand(0); // make this reproducible
  std::random_shuffle(ivec.begin(), ivec.end());

  Zint32Fixture f(UPS_COMPRESSOR_UINT32_FOR, false, 4);
  f.insertFindEraseFind(ivec);
}

TEST_CASE("Zint32/FOR/ascendingDataTest", "")
{
  Zint32Fixture::IntVector ivec;
  for (int i = 0; i < 30000; i++)
    ivec.push_back(i);

  Zint32Fixture f(UPS_COMPRESSOR_UINT32_FOR, false, 4);
  f.insertFindEraseFind(ivec);
}

TEST_CASE("Zint32/FOR/descendingDataTest", "")
{
  Zint32Fixture::IntVector ivec;
  for (int i = 30000; i >= 0; i--)
    ivec.push_back(i);

  Zint32Fixture f(UPS_COMPRESSOR_UINT32_FOR, false, 4);
  f.insertFindEraseFind(ivec);
}

TEST_CASE("Zint32/FOR/uqiTest", "")
{
  Zint32Fixture f(UPS_COMPRESSOR_UINT32_FOR, false, 0);
  f.uqiTest();
}

TEST_CASE("Zint32/FOR/uqiTest-duplicate", "")
{
  Zint32Fixture f(UPS_COMPRESSOR_UINT32_FOR, true, 0);
  f.uqiTestDuplicate();
}

TEST_CASE("Zint32/SimdFOR/randomDataTest", "")
{
#ifdef HAVE_SSE2
  Zint32Fixture::IntVector ivec;
  for (int i = 0; i < 30000; i++)
    ivec.push_back(i);
  std::srand(0); // make this reproducible
  std::random_shuffle(ivec.begin(), ivec.end());

  Zint32Fixture f(UPS_COMPRESSOR_UINT32_SIMDFOR, false, 4);
  f.insertFindEraseFind(ivec);
#endif
}

TEST_CASE("Zint32/SimdFOR/ascendingDataTest", "")
{
#ifdef HAVE_SSE2
  Zint32Fixture::IntVector ivec;
  for (int i = 0; i < 30000; i++)
    ivec.push_back(i);

  Zint32Fixture f(UPS_COMPRESSOR_UINT32_SIMDFOR, false, 4);
  f.insertFindEraseFind(ivec);
#endif
}

TEST_CASE("Zint32/SimdFOR/descendingDataTest", "")
{
#ifdef HAVE_SSE2
  Zint32Fixture::IntVector ivec;
  for (int i = 30000; i >= 0; i--)
    ivec.push_back(i);

  Zint32Fixture f(UPS_COMPRESSOR_UINT32_SIMDFOR, false, 4);
  f.insertFindEraseFind(ivec);
#endif
}

TEST_CASE("Zint32/SimdFOR/uqiTest", "")
{
#ifdef HAVE_SSE2
  Zint32Fixture f(UPS_COMPRESSOR_UINT32_SIMDFOR, false, 0);
  f.uqiTest();
#endif
}

TEST_CASE("Zint32/SimdFOR/uqiTest-duplicate", "")
{
#ifdef HAVE_SSE2
  Zint32Fixture f(UPS_COMPRESSOR_UINT32_SIMDFOR, true, 0);
  f.uqiTestDuplicate();
#endif
}

TEST_CASE("Zint32/Zint32/invalidPagesizeTest", "")
{
  ups_parameter_t p1[] = {
    { UPS_PARAM_PAGE_SIZE, 1024 },
    { 0, 0 }
  };
  ups_parameter_t p2[] = {
    { UPS_PARAM_KEY_TYPE, UPS_TYPE_UINT32 },
    { UPS_PARAM_KEY_COMPRESSION, UPS_COMPRESSOR_UINT32_VARBYTE},
    { 0, 0 }
  };

  ups_env_t *env;
  ups_db_t *db;

  REQUIRE(0 == ups_env_create(&env, "test.db", 0, 0644, &p1[0]));
  REQUIRE(UPS_INV_PARAMETER == ups_env_create_db(env, &db, 1, 0, &p2[0]));
  ups_env_close(env, 0);
}

} // namespace upscaledb
