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

/*
 * @exception_safe: no
 * @thread_safe: no
 */

#ifndef HAM_ENV_TEST_H
#define HAM_ENV_TEST_H

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!
#include "4env/env.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

class EnvironmentTest
{
  public:
    // Constructor
    EnvironmentTest(EnvironmentConfiguration &config)
      : m_config(config) {
    }

    // Returns the Environment's configuration
    EnvironmentConfiguration &config() {
      return (m_config);
    }

    void set_filename(const std::string &filename) {
      m_config.filename = filename;
    }

  private:
    // Reference to the Environment's configuration
    EnvironmentConfiguration &m_config;
};

} // namespace hamsterdb

#endif /* HAM_ENV_TEST_H */
