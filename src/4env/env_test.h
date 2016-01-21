/*
 * Copyright (C) 2005-2016 Christoph Rupp (chris@crupp.de).
 * All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * See the file COPYING for License information.
 */

/*
 * @exception_safe: no
 * @thread_safe: no
 */

#ifndef UPS_ENV_TEST_H
#define UPS_ENV_TEST_H

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!
#include "4env/env.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

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

} // namespace upscaledb

#endif /* UPS_ENV_TEST_H */
