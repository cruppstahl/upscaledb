/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
 * All Rights Reserved.
 *
 * NOTICE: All information contained herein is, and remains the property
 * of Christoph Rupp and his suppliers, if any. The intellectual and
 * technical concepts contained herein are proprietary to Christoph Rupp
 * and his suppliers and may be covered by Patents, patents in process,
 * and are protected by trade secret or copyright law. Dissemination of
 * this information or reproduction of this material is strictly forbidden
 * unless prior written permission is obtained from Christoph Rupp.
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
