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

#ifdef HAM_ENABLE_COMPRESSION

#include <vector>
#include <algorithm>

#include "3rdparty/catch/catch.hpp"

#include "utils.h"
#include "os.hpp"

namespace hamsterdb {

struct Zint32Fixture {
  ham_db_t *m_db;
  ham_env_t *m_env;

  typedef std::vector<uint32_t> IntVector;

  Zint32Fixture()
    : m_db(0), m_env(0) {
    ham_parameter_t p[] = {
      { HAM_PARAM_KEY_TYPE, HAM_TYPE_UINT32 },
      { HAM_PARAM_KEY_COMPRESSION, HAM_COMPRESSOR_UINT32_VARBYTE },
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

      if (*it == 23384)
          printf("hit\n");
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

TEST_CASE("Zint32/randomDataTest", "")
{
  Zint32Fixture::IntVector ivec;
  for (int i = 0; i < 30000; i++)
    ivec.push_back(i);
  std::srand(0); // make this reproducable
  std::random_shuffle(ivec.begin(), ivec.end());

  Zint32Fixture f;
  f.insertFindEraseFind(ivec);
}

TEST_CASE("Zint32/ascendingDataTest", "")
{
  Zint32Fixture::IntVector ivec;
  for (int i = 0; i < 30000; i++)
    ivec.push_back(i);

  Zint32Fixture f;
  f.insertFindEraseFind(ivec);
}

TEST_CASE("Zint32/descendingDataTest", "")
{
  Zint32Fixture::IntVector ivec;
  for (int i = 30000; i >= 0; i--)
    ivec.push_back(i);

  Zint32Fixture f;
  f.insertFindEraseFind(ivec);
}

} // namespace hamsterdb

#endif // HAM_ENABLE_COMPRESSION
