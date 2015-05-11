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

#ifndef HAM_ENV_LOCAL_TEST_H
#define HAM_ENV_LOCAL_TEST_H

#include "ham/hamsterdb.h"

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

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

} // namespace hamsterdb

#endif /* HAM_ENV_LOCAL_TEST_H */
