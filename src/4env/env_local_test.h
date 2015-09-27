/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
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

#ifndef UPS_ENV_LOCAL_TEST_H
#define UPS_ENV_LOCAL_TEST_H

#include "ups/upscaledb.h"

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

class Journal;
class LocalEnvironment;

class LocalEnvironmentTest
{
  public:
    LocalEnvironmentTest(LocalEnvironment *env)
      : m_env(env) {
    }

    // Sets a new journal object
    void set_journal(Journal *journal);

  private:
    LocalEnvironment *m_env;
};

} // namespace upscaledb

#endif /* UPS_ENV_LOCAL_TEST_H */
