/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 * All Rights Reserved.
 *
 * NOTICE: All information contained herein is, and remains the property
 * of Christoph Rupp and his suppliers, if any. The intellectual and
 * technical concepts contained herein are proprietary to Christoph Rupp
 * and his suppliers and may be covered by Patents, patents in process,
 * and are protected by trade secret or copyright law. Dissemination of
 * this information or reproduction of this material is strictly forbidden
 * unless prior written permission is obtained from Christoph Rupp.
 *
 * See files COPYING.* for License information.
 */

/*
 * A factory for compressor objects.
 *
 * @exception_safe: nothrow
 * @thread_safe: yes
 */

#ifndef HAM_COMPRESSOR_FACTORY_H
#define HAM_COMPRESSOR_FACTORY_H

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!
#include "2compressor/compressor.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

class CompressorFactory {
  public:
    // Returns true if the specified compressor is available, otherwise false
    static bool is_available(int type);

    // Creates a new Compressor instance for the specified |type| (being
    // HAM_COMPRESSOR_ZLIB, HAM_COMPRESSOR_SNAPPY etc)
    static Compressor *create(int type);
};

}; // namespace hamsterdb

#endif // HAM_COMPRESSOR_FACTORY_H
