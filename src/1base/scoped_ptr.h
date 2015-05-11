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
 * A (stupid) smart pointer
 *
 * @exception_safe: nothrow
 * @thread_safe: yes
 */

#ifndef HAM_SCOPED_PTR_H
#define HAM_SCOPED_PTR_H

#include "0root/root.h"

#define BOOST_ALL_NO_LIB // disable MSVC auto-linking
#include <boost/scoped_ptr.hpp>

// Always verify that a file of level N does not include headers > N!

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

template <typename T>
struct ScopedPtr : public boost::scoped_ptr<T>
{
  ScopedPtr()
    : boost::scoped_ptr<T>() {
  }

  ScopedPtr(T *t)
    : boost::scoped_ptr<T>(t) {
  }
};

} // namespace hamsterdb

#endif /* HAM_SCOPED_PTR_H */
